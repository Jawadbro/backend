import mysql.connector
import numpy as np
from sentence_transformers import SentenceTransformer

def get_db_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='mahin1tanim2@',  # <-- Change to your actual password
        database='casa_rom_sales'
    )

def fulltext_boolean_search(query, limit=20):
    """
    Perform full-text search using MySQL FULLTEXT index with BOOLEAN MODE
    Returns products with BM25 relevance scores
    """
    cnx = get_db_connection()
    cursor = cnx.cursor(dictionary=True)
    
    try:
        # Sanitize query to prevent SQL injection
        safe_query = query.replace("'", "''").replace("\\", "\\\\")
        
        sql = f"""
            SELECT sku, name, brand, unit_price,
                   MATCH(name, searchable_text) AGAINST ('+{safe_query}*' IN BOOLEAN MODE) AS bm25_score
            FROM products
            WHERE MATCH(name, searchable_text) AGAINST ('+{safe_query}*' IN BOOLEAN MODE)
            ORDER BY bm25_score DESC
            LIMIT {limit}
        """
        cursor.execute(sql)
        results = cursor.fetchall()
        return results
    finally:
        cursor.close()
        cnx.close()

def vector_similarity_search(query, model, limit=20):
    """
    Perform vector similarity search using embeddings
    Returns products with cosine similarity scores
    """
    query_vec = model.encode(query, convert_to_numpy=True)
    cnx = get_db_connection()
    cursor = cnx.cursor()
    
    try:
        cursor.execute("SELECT sku, vec, dims FROM embeddings")
        sims = []
        
        for sku, vec_blob, dims in cursor.fetchall():
            if vec_blob is None:
                continue
            vec = np.frombuffer(vec_blob, dtype=np.float32)
            
            # Calculate cosine similarity
            sim = np.dot(query_vec, vec) / (np.linalg.norm(query_vec) * np.linalg.norm(vec))
            sims.append({'sku': sku, 'vector_score': float(sim)})
        
        # Sort by similarity
        sims.sort(key=lambda x: x['vector_score'], reverse=True)
        return sims[:limit]
    finally:
        cursor.close()
        cnx.close()

def normalize_scores(score_dict):
    """
    Normalize scores to 0-1 range using min-max normalization
    """
    if not score_dict:
        return {}
    
    values = list(score_dict.values())
    min_score = min(values)
    max_score = max(values)
    
    if max_score == min_score:
        return {k: 1.0 for k in score_dict}
    
    return {k: (v - min_score) / (max_score - min_score) for k, v in score_dict.items()}

def get_product_details(skus):
    """
    Fetch full product details for a list of SKUs
    """
    if not skus:
        return []
    
    cnx = get_db_connection()
    cursor = cnx.cursor(dictionary=True)
    
    try:
        placeholders = ','.join(['%s'] * len(skus))
        sql = f"SELECT sku, name, brand, unit_price FROM products WHERE sku IN ({placeholders})"
        cursor.execute(sql, skus)
        products = cursor.fetchall()
        
        # Create a dict for easy lookup
        product_dict = {p['sku']: p for p in products}
        return product_dict
    finally:
        cursor.close()
        cnx.close()

def hybrid_search(query, alpha=0.6, limit=20):
    """
    Perform hybrid search combining BM25 and vector similarity
    
    Args:
        query: Search query string
        alpha: Weight for vector score (0-1). BM25 weight = 1-alpha
        limit: Maximum number of results to return
    
    Returns:
        List of products with hybrid scores
    """
    # Load embedding model
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    # Get results from both search methods
    bm25_results = fulltext_boolean_search(query, limit * 2)
    vector_results = vector_similarity_search(query, model, limit * 2)
    
    # Extract scores
    bm25_scores = {r['sku']: r['bm25_score'] for r in bm25_results}
    vector_scores = {r['sku']: r['vector_score'] for r in vector_results}
    
    # Get all unique SKUs
    all_skus = set(bm25_scores.keys()) | set(vector_scores.keys())
    
    if not all_skus:
        return []
    
    # Normalize scores
    bm25_norm = normalize_scores(bm25_scores) if bm25_scores else {}
    vector_norm = normalize_scores(vector_scores) if vector_scores else {}
    
    # Combine scores with weighted fusion
    fused = []
    for sku in all_skus:
        bm25 = bm25_norm.get(sku, 0)
        vec = vector_norm.get(sku, 0)
        hybrid_score = alpha * vec + (1 - alpha) * bm25
        
        fused.append({
            'sku': sku,
            'hybrid_score': hybrid_score,
            'bm25_score': bm25,
            'vector_score': vec
        })
    
    # Sort by hybrid score
    fused.sort(key=lambda x: x['hybrid_score'], reverse=True)
    top_results = fused[:limit]
    
    # Get full product details
    top_skus = [r['sku'] for r in top_results]
    product_details = get_product_details(top_skus)
    
    # Merge scores with product details
    final_results = []
    for result in top_results:
        sku = result['sku']
        if sku in product_details:
            product = product_details[sku]
            product.update({
                'hybrid_score': round(result['hybrid_score'], 4),
                'bm25_score': round(result['bm25_score'], 4),
                'vector_score': round(result['vector_score'], 4)
            })
            final_results.append(product)
    
    return final_results
