# Streamlit Analytics Dashboard

This directory contains the Streamlit application for the **Modern Lakehouse Architecture** platform.

The dashboard provides real-time insights and a specialized UI for AI/ML-driven features like semantic search.

## 🚀 Pages & Features

### 1. Flash Sale Analytics (`app.py`)
A real-time insights dashboard monitoring recent sales, revenue, and product performance.
- **Data Source**: ClickHouse 
- **Capabilities**: Real-time sales aggregation, 24-hour revenue trends, top 10 best-selling products.

### 2. Review Search (`pages/Review_Search.py`)
A semantic search interface for finding customer reviews conceptually similar to a given query instead of relying on exact keyword matches.
- **Data Source**: PostgreSQL (with `pgvector` extension)
- **Model**: `all-MiniLM-L6-v2` via `sentence-transformers` for embedding generation.
- **Capabilities**: Computes a vector embedding of the user's query and performs a k-NN cosine similarity search against the stored reviews.

## 🛠️ Usage

### Via Docker Compose (Recommended)
The Streamlit app runs automatically as part of the main `docker-compose.yaml` stack.
It is accessible at [http://localhost:8501](http://localhost:8501).

```bash
# To view container logs
docker compose logs -f streamlit
```

### Local Development
To run Streamlit outside of Docker, you must first connect to the backend services (PostgreSQL and ClickHouse).

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set environment variables**:
   Configure these to point to your local port-forwarded database ports.
   ```bash
   export POSTGRES_DB=oneshop
   export POSTGRES_USER=your_pg_user
   export POSTGRES_PASSWORD=your_pg_password
   export POSTGRES_HOST=localhost
   export POSTGRES_PORT=5432

   export CLICKHOUSE_HOST=localhost
   export CLICKHOUSE_PORT=8123
   export CLICKHOUSE_USER=default
   export CLICKHOUSE_PASSWORD=mysecret
   ```

3. **Run the App**:
   ```bash
   streamlit run app.py
   ```
