# Scheduling Embeddings with Airflow

This project utilizes [Apache Airflow](https://airflow.apache.org/) to orchestrate the generation of vector embeddings for customer reviews stored in PostgreSQL using the pgvector extension.

## Architecture Change

Previously, embeddings were continuously calculated via a standalone `embedding-loadgen` Docker container. This has now been shifted to run as a scheduled Airflow pipeline (DAG). 

- Why? This architecture change dramatically saves idle resources and gives you better observability and retry logic. 
- The embeddings are calculated using `sentence-transformers/all-MiniLM-L6-v2`.

## Airflow Configuration

### 1. The Custom Dockerfile
Airflow runs in a set of Docker containers (`webserver`, `scheduler`, `init`, etc). To ensure that the Python executor possesses the correct ML dependencies, the `./airflow/dockerfile` was modified to include:
- `sentence-transformers`: For encoding text into dense vector representations.
- `psycopg2-binary`: For executing PostgreSQL insert and select queries directly from the Airflow workers.

### 2. The DAG `generate_product_embeddings`
Located at `./airflow/dags/embedding_generator_dag.py`, this DAG runs on a strict `@hourly` schedule.

When triggered, it works under the assumption of **idempotency**:
- It searches the `public.reviews` for any new messages where `review_embedding IS NULL`.
- If new rows exist, it pulls them down to the Airflow worker, calculates the vectors in batches of `100`, and uploads them back into the PostgreSQL instance. 
- If no rows exist, the pipeline silently skips execution, preventing unnecessary load on the system.

## How to Run & Monitor

Since Airflow configuration is provided via `airflow.yaml`, execute the following Docker Compose command to deploy the stack:

**Deploying Airflow + the Database Network:**
```bash
docker compose up -d
docker compose -f airflow.yaml up -d
```
*(Wait a few minutes for the `airflow-init` container to provision the DB)*

**Accessing the Airflow Dashboard:**
- Open your browser to `http://localhost:8085`
- The default credentials (defined in your `airflow.yaml` environment variables) are used to log in.
- Navigate to the **DAGs** page, unpause the `generate_product_embeddings` DAG, and view its hourly executions inside the Grid/Graph views!
