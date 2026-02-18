import random
import time
import os
import logging
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Faker
fake = Faker()

# Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgresuser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgrespw")
POSTGRES_DB = os.getenv("POSTGRES_DB", "oneshop")

CATEGORIES = ["widgets", "gadgets", "doodads", "clearance", "electronics", "home", "clothing"]
ITEM_COUNT = 1000
PRICE_MIN = 5.00
PRICE_MAX = 500.00
INVENTORY_MIN = 100
INVENTORY_MAX = 5000
MAX_RETRIES = 30
RETRY_DELAY = 2

def get_db_connection():
    """Establish a database connection with retries."""
    retries = 0
    while retries < MAX_RETRIES:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            logger.info("Successfully connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"Database connection failed (attempt {retries + 1}/{MAX_RETRIES}): {e}")
            retries += 1
            time.sleep(RETRY_DELAY)
        except Exception as e:
            logger.error(f"Unexpected error connecting to database: {e}")
            raise
            
    raise Exception("Could not connect to database after multiple attempts")

def generate_item():
    """Generate a single random item."""
    name = fake.bs().title()
    category = random.choice(CATEGORIES)
    price = round(random.uniform(PRICE_MIN, PRICE_MAX), 2)
    inventory = random.randint(INVENTORY_MIN, INVENTORY_MAX)
    return (name, category, price, inventory)

def seed_items(conn):
    """Seed items into the database."""
    try:
        cur = conn.cursor()
        
        # Schema is managed by postgres_bootstrap.sql
        # We assume the table exists.
        
        # Generate data
        items = [generate_item() for _ in range(ITEM_COUNT)]
        
        # Bulk insert
        execute_values(
            cur,
            "INSERT INTO items (name, category, price, inventory) VALUES %s",
            items
        )
        
        conn.commit()
        logger.info(f"Successfully inserted {ITEM_COUNT} items.")
        cur.close()
    except Exception as e:
        logger.error(f"Error seeding items: {e}")
        conn.rollback()
        raise

def main():
    """Main execution function."""
    logger.info("Starting items-loadgen seeder...")
    
    conn = None
    try:
        conn = get_db_connection()
        seed_items(conn)
    except Exception as e:
        logger.error(f"Seeder failed: {e}")
        exit(1)
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")
        logger.info("Seeder finished.")

if __name__ == "__main__":
    main()