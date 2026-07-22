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

CATEGORIES = ["widgets", "gadgets", "doodads", "clearance", "electronics", "home", "clothing"]
PRICE_MIN = 5.00
PRICE_MAX = 500.00
INVENTORY_MIN = 100
INVENTORY_MAX = 5000
MAX_RETRIES = 30
RETRY_DELAY = 2
SEED_MODES = {"skip-if-present", "append"}


def required_env(name):
    """Return a required environment value without supplying unsafe defaults."""
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {name}")
    return value


def get_seed_settings():
    """Read and validate finite seed-job settings."""
    item_count = int(os.getenv("ITEM_SEED_COUNT", "1000"))
    if item_count <= 0:
        raise ValueError("ITEM_SEED_COUNT must be greater than zero")

    seed_mode = os.getenv("ITEM_SEED_MODE", "skip-if-present")
    if seed_mode not in SEED_MODES:
        raise ValueError(f"ITEM_SEED_MODE must be one of: {', '.join(sorted(SEED_MODES))}")

    return item_count, seed_mode

def get_db_connection():
    """Establish a database connection with retries."""
    postgres_host = os.getenv("POSTGRES_HOST", "postgres")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    postgres_user = required_env("POSTGRES_USER")
    postgres_password = required_env("POSTGRES_PASSWORD")
    postgres_db = os.getenv("POSTGRES_DB", "oneshop")

    retries = 0
    while retries < MAX_RETRIES:
        try:
            conn = psycopg2.connect(
                host=postgres_host,
                port=postgres_port,
                dbname=postgres_db,
                user=postgres_user,
                password=postgres_password
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
            
    raise ConnectionError("Could not connect to database after multiple attempts")

def generate_item():
    """Generate a single random item."""
    name = fake.bs().title()
    category = random.choice(CATEGORIES)
    price = round(random.uniform(PRICE_MIN, PRICE_MAX), 2)
    inventory = random.randint(INVENTORY_MIN, INVENTORY_MAX)
    return (name, category, price, inventory)

def seed_items(conn, item_count, seed_mode):
    """Seed items with transaction-scoped concurrency and explicit rerun semantics."""
    cur = None
    try:
        cur = conn.cursor()

        # Serialize concurrent seeders before checking whether data already exists.
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("items-loadgen",))
        cur.execute("SELECT count(*) FROM items")
        existing_count = cur.fetchone()[0]

        if existing_count > 0 and seed_mode == "skip-if-present":
            conn.commit()
            logger.info("Items already exist; skip-if-present mode made no changes.")
            return 0

        # Generate data
        items = [generate_item() for _ in range(item_count)]

        # Bulk insert
        execute_values(
            cur,
            "INSERT INTO items (name, category, price, inventory) VALUES %s",
            items
        )

        conn.commit()
        logger.info("Successfully inserted %s items.", item_count)
        return item_count
    except Exception as e:
        logger.error(f"Error seeding items: {e}")
        conn.rollback()
        raise
    finally:
        if cur:
            cur.close()

def main():
    """Main execution function."""
    logger.info("Starting items-loadgen seeder...")

    conn = None
    try:
        item_count, seed_mode = get_seed_settings()
        conn = get_db_connection()
        seed_items(conn, item_count, seed_mode)
    except Exception as e:
        logger.error(f"Seeder failed: {e}")
        raise SystemExit(1) from e
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")
        logger.info("Seeder finished.")

if __name__ == "__main__":
    main()
