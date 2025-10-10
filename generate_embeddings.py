import mysql.connector
from sentence_transformers import SentenceTransformer
import numpy as np

# Load embedding model
print("Loading embedding model...")
model = SentenceTransformer('all-MiniLM-L6-v2')  # Fast and efficient model

def get_db_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='mahin1tanim2@',  # <-- Change to your actual password
        database='casa_rom_sales'
    )

def generate_embeddings():
    cnx = get_db_connection()
    cursor = cnx.cursor(dictionary=True)
    
    try:
        # Fetch all products
        cursor.execute("SELECT sku, name, searchable_text FROM products")
        products = cursor.fetchall()
        
        print(f"Found {len(products)} products to process...")
        
        for idx, product in enumerate(products):
            sku = product['sku']
            name = product['name'] or ''
            searchable_text = product['searchable_text'] or ''
            
            # Combine name and searchable_text for embedding
            text = f"{name} {searchable_text}".strip()
            
            if not text:
                print(f"Skipping {sku} - no text content")
                continue
            
            # Generate embedding
            embedding = model.encode(text, convert_to_numpy=True)
            embedding_bytes = embedding.tobytes()
            embedding_dim = embedding.shape[0]
            model_name = 'all-MiniLM-L6-v2'
            
            # Check if embedding already exists
            cursor.execute("SELECT sku FROM embeddings WHERE sku = %s", (sku,))
            exists = cursor.fetchone()
            
            if exists:
                # Update existing embedding
                cursor.execute("""
                    UPDATE embeddings
                    SET vec = %s, dims = %s, model = %s
                    WHERE sku = %s
                """, (embedding_bytes, embedding_dim, model_name, sku))
            else:
                # Insert new embedding
                cursor.execute("""
                    INSERT INTO embeddings (sku, vec, dims, model)
                    VALUES (%s, %s, %s, %s)
                """, (sku, embedding_bytes, embedding_dim, model_name))
            
            if (idx + 1) % 10 == 0:
                print(f"Processed {idx + 1}/{len(products)} products...")
                cnx.commit()
        
        cnx.commit()
        print(f"✅ Successfully generated embeddings for {len(products)} products!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        cnx.rollback()
    finally:
        cursor.close()
        cnx.close()

if __name__ == "__main__":
    generate_embeddings()
