from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
import logging

logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False, # Update this based on your alert preferences
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='generate_product_embeddings',
    default_args=default_args,
    description='A DAG to generate embeddings for customer reviews hourly',
    schedule_interval='@hourly', # Running hourly as requested
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['embeddings', 'machine_learning'],
)
def embedding_generator_dag():
    
    @task(task_id='calculate_and_upload_embeddings')
    def process_embeddings():
        import psycopg2
        from sentence_transformers import SentenceTransformer
        from psycopg2.extras import execute_values
        
        # Load environment variables populated by Airflow / Docker Compose
        DB_CONFIG = {
            "dbname": os.getenv("POSTGRES_DB", "postgres"),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
            "host": "postgres", # Must match docker-compose service name
            "port": 5432,
        }
        
        MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
        BATCH_SIZE = 100
        
        logger.info(f"Loading model: {MODEL_NAME}")
        model = SentenceTransformer(MODEL_NAME)
        
        conn = None
        try:
            # We connect to the main postgres warehouse (from docker compose)
            logger.info(f"Connecting to database {DB_CONFIG['dbname']} at {DB_CONFIG['host']}...")
            conn = psycopg2.connect(**DB_CONFIG)
            
            with conn.cursor() as cur:
                # Fetch reviews without an embedding
                cur.execute("SELECT review_id, review FROM public.reviews WHERE review_embedding IS NULL")
                reviews = cur.fetchall()
            
            if not reviews:
                logger.info("No new reviews need embeddings. Exiting.")
                return
                
            logger.info(f"Found {len(reviews)} reviews requiring embeddings.")
            
            to_update = []
            for review_id, review_text in reviews:
                embedding = model.encode(review_text).tolist()
                to_update.append((embedding, review_id))
                
                # Batch processing
                if len(to_update) >= BATCH_SIZE:
                    update_query = """
                        UPDATE public.reviews AS r
                        SET review_embedding = CAST(v.embedding AS vector)
                        FROM (VALUES %s) AS v(embedding, id)
                        WHERE r.review_id = v.id;
                    """
                    with conn.cursor() as cur:
                        execute_values(cur, update_query, to_update)
                    conn.commit()
                    logger.info(f"Successfully processed batch of {len(to_update)} records.")
                    to_update = []
            
            # Process remainders
            if to_update:
                update_query = """
                    UPDATE public.reviews AS r
                    SET review_embedding = CAST(v.embedding AS vector)
                    FROM (VALUES %s) AS v(embedding, id)
                    WHERE r.review_id = v.id;
                """
                with conn.cursor() as cur:
                    execute_values(cur, update_query, to_update)
                conn.commit()
                logger.info(f"Successfully processed final batch of {len(to_update)} records.")
            
            logger.info("Embedding generation and update process completed successfully.")
            
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error occurred: {e}")
            raise
        finally:
            if conn is not None and not conn.closed:
                conn.close()

    # Define DAG structure
    process_embeddings()

# Instantiate the DAG
embedding_dag = embedding_generator_dag()
