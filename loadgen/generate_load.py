
import json
import math
import os
import io
import random
import time
import uuid
import logging
from minio import Minio
from minio.error import S3Error
import barnum
import psycopg2
from psycopg2 import sql, Error

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LoadGenerator:
    def __init__(self):
        # Load Generation Configuration
        self.users_seed_count = int(os.getenv("USERS_SEED_COUNT", 10000))
        self.item_seed_count = int(os.getenv("ITEM_SEED_COUNT", 1000))
        self.item_inventory_min = int(os.getenv("ITEM_INVENTORY_MIN", 1000))
        self.item_inventory_max = int(os.getenv("ITEM_INVENTORY_MAX", 5000))
        self.item_price_min = int(os.getenv("ITEM_PRICE_MIN", 5))
        self.item_price_max = int(os.getenv("ITEM_PRICE_MAX", 500))
        self.purchase_gen_count = int(os.getenv("PURCHASE_GEN_COUNT", 100))
        self.purchase_gen_every_ms = int(os.getenv("PURCHASE_GEN_EVERY_MS", 100))
        self.pageview_multiplier = int(os.getenv("PAGEVIEW_MULTIPLIER", 75))

        # Postgres Configuration
        self.postgres_host = os.getenv("POSTGRES_HOST", "postgres")
        self.postgres_port = os.getenv("POSTGRES_PORT", "5432")
        self.postgres_user = os.getenv("POSTGRES_USER")
        self.postgres_pass = os.getenv("POSTGRES_PASSWORD")
        self.postgres_db = os.getenv("POSTGRES_DB", "oneshop")

        if not self.postgres_user or not self.postgres_pass:
            raise EnvironmentError("Missing required Postgres credentials (POSTGRES_USER, POSTGRES_PASSWORD).")

        # MinIO Configuration
        self.minio_url = os.getenv("MINIO_URL", "http://minio:9000")
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY")
        self.minio_bucket_name = os.getenv("MINIO_BUCKET_NAME", "pageviews")

        if not self.minio_access_key or not self.minio_secret_key:
            raise EnvironmentError("Missing required MinIO credentials (MINIO_ACCESS_KEY, MINIO_SECRET_KEY).")

        # Constants
        self.channels = ["organic search", "paid search", "referral", "social", "display"]
        self.categories = ["widgets", "gadgets", "doodads", "clearance"]
        
        # SQL Templates
        self.item_insert_sql = "INSERT INTO items (name, category, price, inventory) VALUES (%s, %s, %s, %s)"
        self.user_insert_sql = "INSERT INTO users (first_name, last_name, email) VALUES (%s, %s, %s)"
        self.purchase_insert_sql = "INSERT INTO purchases (user_id, item_id, quantity, purchase_price, created_at) VALUES (%s, %s, %s, %s, %s)"

        # Initialize MinIO Client
        self.minio_client = Minio(
            self.minio_url.replace("http://", "").replace("https://", ""),
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=False
        )

    def setup_bucket(self):
        """Ensures the MinIO bucket exists and is empty."""
        try:
            if self.minio_client.bucket_exists(self.minio_bucket_name):
                logger.info(f"Bucket '{self.minio_bucket_name}' already exists. Deleting it...")
                self._delete_bucket_contents()
                self.minio_client.remove_bucket(self.minio_bucket_name)
                logger.info(f"Bucket '{self.minio_bucket_name}' deleted successfully.")
            
            logger.info(f"Creating bucket '{self.minio_bucket_name}'...")
            self.minio_client.make_bucket(self.minio_bucket_name)
            logger.info(f"Bucket '{self.minio_bucket_name}' created.")
        except S3Error as e:
            logger.error(f"Error checking, deleting, or creating bucket: {e}")
            raise

    def _delete_bucket_contents(self):
        """Helper to delete all objects in the bucket."""
        try:
            objects = self.minio_client.list_objects(self.minio_bucket_name, recursive=True)
            for obj in objects:
                logger.info(f"Deleting object: {obj.object_name}")
                self.minio_client.remove_object(self.minio_bucket_name, obj.object_name)
        except S3Error as err:
            logger.error(f"Error deleting bucket contents: {err}")
            raise

    def generate_pageview(self, viewer_id, target_id, page_type):
        """Generates a pageview event dictionary."""
        return {
            "user_id": viewer_id,
            "url": f"/{page_type}/{target_id}",
            "channel": random.choice(self.channels),
            "received_at": int(time.time()),
        }

    def write_to_bucket(self, event):
        """Writes a single event to the MinIO bucket."""
        try:
            event_bytes = io.BytesIO(json.dumps(event).encode("utf-8"))
            object_name = f"event_{int(time.time())}_{uuid.uuid4()}.json"

            self.minio_client.put_object(
                self.minio_bucket_name,
                object_name,
                data=event_bytes,
                length=event_bytes.getbuffer().nbytes,
                content_type="application/json"
            )
            logger.debug(f"Uploaded: {object_name}")
        except S3Error as e:
            logger.error(f"Error uploading {object_name}: {e}")

    def get_db_connection(self):
        """Establishes and returns a database connection."""
        return psycopg2.connect(
            host=self.postgres_host,
            port=self.postgres_port,
            user=self.postgres_user,
            password=self.postgres_pass,
            dbname=self.postgres_db
        )

    def run(self):
        """Main execution method."""
        self.setup_bucket()

        try:
            with self.get_db_connection() as connection:
                with connection.cursor() as cursor:
                    logger.info("Seeding data...")
                    
                    # Seed Items
                    cursor.executemany(
                        self.item_insert_sql,
                        [
                            (
                                barnum.create_nouns(),
                                random.choice(self.categories),
                                random.randint(self.item_price_min * 100, self.item_price_max * 100) / 100,
                                random.randint(self.item_inventory_min, self.item_inventory_max),
                            )
                            for _ in range(self.item_seed_count)
                        ],
                    )

                    # Seed Users
                    cursor.executemany(
                        self.user_insert_sql,
                        [
                            (
                                barnum.create_name()[0],
                                barnum.create_name()[1],
                                None if random.random() < 0.1 else barnum.create_email()
                            )
                            for _ in range(self.users_seed_count)
                        ],
                    )
                    connection.commit()

                    logger.info("Getting item ID and PRICEs...")
                    cursor.execute("SELECT id, price FROM items")
                    item_prices = [(row[0], row[1]) for row in cursor.fetchall()]

                    logger.info("Preparing to loop + seed kafka pageviews and purchases")
                    
                    for _ in range(self.purchase_gen_count):
                        # Generate purchase data
                        if not item_prices: 
                             logger.warning("No items found to purchase.")
                             break
                             
                        purchase_item = random.choice(item_prices)
                        purchase_user = random.randint(0, self.users_seed_count - 1)
                        purchase_quantity = random.randint(1, 5)
                        purchase_ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - random.randint(0, 24 * 60 * 60)))
                        
                        # Write purchaser pageview
                        self.write_to_bucket(self.generate_pageview(purchase_user, purchase_item[0], "products"))

                        # Write random pageviews
                        pageview_oscillator = int(
                            self.pageview_multiplier + (math.sin(time.time() / 1000) * 50)
                        )
                        for _ in range(pageview_oscillator):
                            rand_user = random.randint(0, self.users_seed_count)
                            self.write_to_bucket(self.generate_pageview(
                                rand_user,
                                random.randint(0, self.item_seed_count),
                                "products",
                            ))
                        
                        # Write purchase row
                        cursor.execute(
                            self.purchase_insert_sql,
                            (
                                purchase_user,
                                purchase_item[0],
                                purchase_quantity,
                                purchase_item[1] * purchase_quantity,
                                purchase_ts
                            ),
                        )
                        connection.commit()

                        # Pause
                        time.sleep(self.purchase_gen_every_ms / 1000)

        except Error as e:
            logger.error(f"Database error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    generator = LoadGenerator()
    generator.run()