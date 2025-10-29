# Casa Rom Sales Quotation System – Backend
*A full‑stack backend service supporting product search and quotation generation*

## Overview
This backend service powers the quotation system for the “Casa Rom” project. It is built using **Python** and FastAPI, connects to a MySQL database, and supports:
- Importing a large CSV dataset of products (unique number, brand name, price) into MySQL
- Hybrid product search using **BM25 + vector similarity**
- Generation and removal of sales quotations, including pricing logic for transfer discounts and installment markups
- Embedding generation for vector‑search indexing

## Architecture
- `app.py` — FastAPI app entrypoint, defines HTTP endpoints
- `db.py` — Database connection & setup logic (MySQL)
- `generate_embeddings.py` — Script/service to compute and store embeddings for products
- `search_service.py` — Search logic handling BM25 + vector similarity hybrid queries
- `quote_service.py` — Quotation generation logic, handles add/remove quote, applies discount/markup logic
- `requirements.txt` — Python dependencies

## Getting Started

### Prerequisites
- Python 3.9+
- MySQL database (e.g., MySQL 8)
- Access (or key) to embedding model or vector‑embedding service (if applicable)
- Product CSV file (~10,000 rows) containing:
  - unique product number
  - brand name
  - price

### Setup
1. Clone the repository:
   ```bash
   git clone https://github.com/Jawadbro/backend.git
   cd backend
   ```
2. Create and activate a Python virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # on Unix/macOS
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Configure environment variables (create a `.env` file or set in your environment):
   ```text
   DB_HOST=your_mysql_host
   DB_PORT=3306
   DB_USER=your_user
   DB_PASSWORD=your_password
   DB_NAME=your_db_name
   EMBEDDING_MODEL_KEY=your_optional_embedding_api_key
   ```
5. Initialize database and import the CSV data:
   - Use the MySQL client (or a script) to create the necessary schema (products table, embeddings table, quotes table)
   - Run `generate_embeddings.py` (or the relevant import logic) to ingest the CSV and compute embeddings
6. Start the FastAPI server:
   ```bash
   uvicorn app:app --reload --host 0.0.0.0 --port 8000
   ```
7. API endpoints (examples)
   - `GET /search?query=<term>&top_k=<int>` — search for products
   - `POST /quote` — create a new sales quotation
   - `DELETE /quote/<quote_id>` — remove a quotation
   - (Add further endpoints as documented in code)

## Key Features
- **Hybrid Search**: Combines traditional BM25 keyword search with vector similarity to ensure both precision and semantic relevance.
- **Embeddings Pipeline**: Pre‑computes embeddings for each product so that search queries with semantic intent get matched via vector distance.
- **Quotation Logic**: Automates pricing adjustments — e.g., handling transfer discounts, installment-based markups, dynamic price calculation.
- **Clean Separation**: Search and quotation services are modular (`search_service.py`, `quote_service.py`), making it easier to maintain and extend.

## Deployment & Scaling
- For production, consider using a managed MySQL service and enabling connection pooling.
- The FastAPI app can be deployed behind a production-grade ASGI server (e.g., `gunicorn + uvicorn workers`).
- Vector search may require an efficient index (in‑memory or on‑disk) if dataset grows beyond 10k. Consider dedicated vector DB (e.g., Pinecone, Weaviate) if scaling further.
- Configure logging, monitoring, and error tracking (e.g., Sentry) for robust production operations.

## Documentation & Maintenance
- Inline docstrings and type hints in code support readability.
- The `generate_embeddings.py` script includes comments explaining data ingestion and embedding logic.
- The search logic in `search_service.py` clearly documents how BM25 ranking and vector similarity are combined.
- The quotation logic in `quote_service.py` explains how discounts and markups are applied.
- For major changes (dataset schema, new services), please update this README and add migration scripts if needed.

## Contribution
This is currently a private/contract‑based project (discretion required). Please reach out to the project lead for access or coordination.
If you are collaborating:
- Use feature branches.
- Write unit tests for new logic (especially search/quotation logic).
- Add/update documentation when adding new endpoints or modifying logic.

## License & Confidentiality
This codebase is proprietary and shared under confidentiality. Unauthorized sharing or public disclosure is prohibited.

## Contact
For questions, hints or issues, please contact the backend owner.
