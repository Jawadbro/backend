from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Any
from datetime import datetime, timedelta
import mysql.connector
import json
import uuid
import numpy as np
from sentence_transformers import SentenceTransformer

# --- CORS Middleware ---
app = FastAPI()
origins = [
    "http://localhost:3000", 
     "https://database-five-mu.vercel.app", # React dev server
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Database Connection ---
def get_db_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='mahin1tanim2@',  # <-- CHANGE THIS
        database='casa_rom_sales'      # <-- CHANGE THIS
    )

# --- Pydantic Models ---
class Product(BaseModel):
    sku: str
    name: str
    brand: Optional[str]
    category: Optional[str]
    unit_price: float

class QuoteLineIn(BaseModel):
    sku: str
    qty: int
    attributes: Optional[dict] = {}

class QuoteCreateIn(BaseModel):
    customerRef: str
    lines: List[QuoteLineIn]

class QuoteLineOut(BaseModel):
    line_number: int
    sku: str
    name: str
    qty: int
    unit_price: float
    line_total: float
    attrs: Optional[Any] = {}

class QuoteOut(BaseModel):
    quote_id: str
    customer_ref: str
    valid_until: datetime
    list_total: float
    transfer_total: float
    installments_total: float
    notes: List[str]
    lines: List[QuoteLineOut]

# --- Helper: Load Embeddings from Separate Table (BLOB) ---
def load_embeddings():
    cnx = get_db_connection()
    cursor = cnx.cursor(dictionary=True)
    cursor.execute("SELECT sku, vec FROM embeddings WHERE vec IS NOT NULL")
    embeddings = {}
    for row in cursor.fetchall():
        # 'vec' is a BLOB of 384 float32 values (1536 bytes)
        emb = np.frombuffer(row['vec'], dtype=np.float32)
        embeddings[row['sku']] = emb
    cursor.close()
    cnx.close()
    return embeddings

# --- Hybrid Search ---
@app.get("/search")
def hybrid_search(
    q: str = Query(..., min_length=1),
    limit: int = 20,
    alpha: float = 0.6
):
    cnx = get_db_connection()
    cursor = cnx.cursor(dictionary=True)

    # 1. Full-text search (BM25) and LIKE on name, brand, sku
    cursor.execute("""
        SELECT sku, name, brand, unit_price,
               MATCH(searchable_text) AGAINST (%s IN BOOLEAN MODE) AS bm25_score
        FROM products
        WHERE MATCH(searchable_text) AGAINST (%s IN BOOLEAN MODE)
           OR name LIKE %s
           OR brand LIKE %s
           OR sku LIKE %s
        ORDER BY bm25_score DESC
        LIMIT %s
    """, (
        q + '*', q + '*', f"%{q}%", f"%{q}%", f"%{q}%", limit
    ))
    bm25_results = cursor.fetchall()

    # 2. Vector similarity search using embeddings table
    embeddings = load_embeddings()
    model = SentenceTransformer('all-MiniLM-L6-v2')
    query_emb = model.encode(q)
    vector_scores = []
    for sku, emb in embeddings.items():
        score = float(np.dot(query_emb, emb) / (np.linalg.norm(query_emb) * np.linalg.norm(emb)))
        vector_scores.append((sku, score))
    vector_scores = sorted(vector_scores, key=lambda x: x[1], reverse=True)[:limit]

    # 3. Merge results
    bm25_dict = {row['sku']: row for row in bm25_results}
    vector_dict = dict(vector_scores)
    all_skus = set(bm25_dict.keys()) | set(vector_dict.keys())
    results = []
    for sku in all_skus:
        bm25 = bm25_dict.get(sku, {})
        bm25_score = bm25.get('bm25_score', 0)
        vector_score = vector_dict.get(sku, 0)
        hybrid_score = alpha * vector_score + (1 - alpha) * bm25_score

        # Fetch product info if not in bm25 results
        if bm25:
            prod = bm25
        else:
            cursor.execute("SELECT sku, name, brand, unit_price FROM products WHERE sku = %s", (sku,))
            prod = cursor.fetchone()

        if prod:
            results.append({
                "sku": prod['sku'],
                "name": prod['name'],
                "brand": prod.get('brand'),
                "unit_price": float(prod['unit_price']),
                "hybrid_score": hybrid_score
            })

    cursor.close()
    cnx.close()
    # Sort by hybrid score and return top N
    return {"results": sorted(results, key=lambda x: x['hybrid_score'], reverse=True)[:limit]}

# --- Product Details by SKU ---
@app.get("/products/{sku}", response_model=Product)
def get_product(sku: str):
    cnx = get_db_connection()
    cursor = cnx.cursor(dictionary=True)
    cursor.execute("SELECT sku, name, brand, category, unit_price FROM products WHERE sku = %s", (sku,))
    prod = cursor.fetchone()
    cursor.close()
    cnx.close()
    if not prod:
        raise HTTPException(status_code=404, detail="Product not found")
    return prod

# --- Quote Creation ---
@app.post("/quotes")
def create_quote(quote: QuoteCreateIn):
    cnx = get_db_connection()
    cursor = cnx.cursor(dictionary=True)
    try:
        cnx.start_transaction()
        # Validate customerRef
        if not quote.customerRef or not isinstance(quote.customerRef, str):
            raise HTTPException(status_code=400, detail="Customer reference must be a non-empty string.")
        if not quote.lines or not isinstance(quote.lines, list):
            raise HTTPException(status_code=400, detail="Lines must be a non-empty list.")

        quote_id = 'CRQ-' + uuid.uuid4().hex[:8].upper()
        valid_until = datetime.now() + timedelta(hours=24)

        # Get pricing config
        cursor.execute("SELECT transfer_discount, installments_markup FROM config_pricing WHERE id = 1")
        config = cursor.fetchone()
        if not config:
            raise HTTPException(status_code=500, detail="Pricing config not found in the database.")

        transfer_discount = float(config['transfer_discount'])
        installments_markup = float(config['installments_markup'])

        # Insert the quote first with placeholder totals
        cursor.execute("""
            INSERT INTO quotes (quote_id, customer_ref, valid_until, list_total, transfer_total, installments_total, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            quote_id,
            quote.customerRef,
            valid_until.strftime('%Y-%m-%d %H:%M:%S'),
            0, 0, 0,
            json.dumps(["Stock will be confirmed before fulfillment."])
        ))

        list_total = 0.0
        for idx, line in enumerate(quote.lines):
            sku = line.sku
            qty = line.qty
            attrs = line.attributes or {}

            if not sku or not isinstance(qty, int) or qty <= 0:
                raise HTTPException(status_code=400, detail=f"Invalid SKU or quantity at line {idx + 1}")

            cursor.execute("SELECT unit_price, name FROM products WHERE sku = %s", (sku,))
            product = cursor.fetchone()
            if not product:
                raise HTTPException(status_code=400, detail=f"Invalid SKU: {sku}")

            unit_price = float(product['unit_price'])
            name = product['name']
            line_total = unit_price * qty
            list_total += line_total

            cursor.execute("""
                INSERT INTO quote_lines (quote_id, line_number, sku, name, qty, unit_price, line_total, attrs)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                quote_id,
                idx + 1,
                sku,
                name,
                qty,
                unit_price,
                line_total,
                json.dumps(attrs)
            ))

        transfer_total = list_total * (1 - transfer_discount)
        installments_total = list_total * (1 + installments_markup)

        cursor.execute("""
            UPDATE quotes
            SET list_total = %s, transfer_total = %s, installments_total = %s
            WHERE quote_id = %s
        """, (
            list_total,
            transfer_total,
            installments_total,
            quote_id
        ))

        cnx.commit()
        return {"success": True, "quoteId": quote_id}
    except HTTPException as e:
        cnx.rollback()
        raise e
    except Exception as e:
        cnx.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        cnx.close()

# --- Quote Retrieval ---
@app.get("/quotes/{quote_id}", response_model=QuoteOut)
def get_quote(quote_id: str):
    cnx = get_db_connection()
    cursor = cnx.cursor(dictionary=True)
    cursor.execute("SELECT * FROM quotes WHERE quote_id = %s", (quote_id,))
    quote = cursor.fetchone()
    if not quote:
        cursor.close()
        cnx.close()
        raise HTTPException(status_code=404, detail="Quote not found")

    cursor.execute("SELECT * FROM quote_lines WHERE quote_id = %s ORDER BY line_number", (quote_id,))
    lines = cursor.fetchall()
    cursor.close()
    cnx.close()

    # Parse notes and attrs
    notes = json.loads(quote['notes']) if quote['notes'] else []
    line_objs = []
    for line in lines:
        attrs = json.loads(line['attrs']) if line['attrs'] else {}
        line_objs.append(QuoteLineOut(
            line_number=line['line_number'],
            sku=line['sku'],
            name=line['name'],
            qty=line['qty'],
            unit_price=line['unit_price'],
            line_total=line['line_total'],
            attrs=attrs
        ))

    return QuoteOut(
        quote_id=quote['quote_id'],
        customer_ref=quote['customer_ref'],
        valid_until=quote['valid_until'],
        list_total=quote['list_total'],
        transfer_total=quote['transfer_total'],
        installments_total=quote['installments_total'],
        notes=notes,
        lines=line_objs
    )

# --- Root Endpoint ---
@app.get("/")
def root():
    return {
        "message": "Casa Rom Sales API",
        "endpoints": [
            "/search?q=term",
            "/products/{sku}",
            "/quotes (POST)",
            "/quotes/{quote_id}"
        ]
    }