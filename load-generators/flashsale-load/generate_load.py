import math
import os
import random
import time
import sys
import logging

import barnum
import psycopg2
from psycopg2 import sql, Error

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# CONFIG
users_seed_count = int(os.getenv("USERS_SEED_COUNT", 1000))
item_seed_count = int(os.getenv("ITEM_SEED_COUNT", 100))

item_inventory_min = 1000
item_inventory_max = 5000
item_price_min = 5
item_price_max = 500

purchase_gen_every_ms = int(os.getenv("PURCHASE_GEN_EVERY_MS", 500))

channels = ["organic search", "paid search", "referral", "social", "display"]
categories = ["widgets", "gadgets", "doodads", "clearance"]
campaigns = ["BOGO23", "FLASH2025"]

postgres_host = os.getenv("POSTGRES_HOST", "debezium-postgres")
postgres_port = os.getenv("POSTGRES_PORT", "5432")
postgres_user = os.getenv("POSTGRES_USER", "postgresuser")
postgres_pass = os.getenv("POSTGRES_PASSWORD", "postgrespw")
postgres_db = os.getenv("POSTGRES_DB", "oneshop")

# INSERT TEMPLATES
item_insert = "INSERT INTO items (name, category, price, inventory) VALUES (%s, %s, %s, %s)"
user_insert = "INSERT INTO users (first_name, last_name, email) VALUES (%s, %s, %s)"
purchase_insert = "INSERT INTO purchases (user_id, item_id, campaign_id, quantity, purchase_price, created_at) VALUES (%s, %s, %s, %s, %s, %s)"

def get_connection():
    try:
        conn = psycopg2.connect(
            host=postgres_host,
            port=postgres_port,
            user=postgres_user,
            password=postgres_pass,
            dbname=postgres_db
        )
        return conn
    except Error as e:
        logger.error(f"Error connecting to Postgres: {e}")
        raise

def wait_for_db():
    retries = 30
    while retries > 0:
        try:
            conn = get_connection()
            conn.close()
            logger.info(f"Successfully connected to PostgreSQL at {postgres_host}:{postgres_port}/{postgres_db}")
            return
        except Exception as e:
            logger.warning(f"Waiting for database... ({30-retries}/30) - {e}")
            time.sleep(2)
            retries -= 1
    raise Exception("Could not connect to database")

def seed_if_needed():
    try:
        with get_connection() as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                # Check Users
                cursor.execute("SELECT count(*) FROM users")
                current_users = cursor.fetchone()[0]
                logger.info(f"Checking users: current={current_users}, target={users_seed_count}")
                
                if current_users < users_seed_count:
                    to_seed = users_seed_count - current_users
                    logger.info(f"Seeding {to_seed} users...")
                    batch_size = 1000
                    for i in range(0, to_seed, batch_size):
                        batch = [
                            (barnum.create_name()[0], barnum.create_name()[1], barnum.create_email())
                            for _ in range(min(batch_size, to_seed - i))
                        ]
                        cursor.executemany(user_insert, batch)
                        logger.info(f"Seeded batch of {len(batch)} users")
                
                # Check Items
                cursor.execute("SELECT count(*) FROM items")
                current_items = cursor.fetchone()[0]
                logger.info(f"Checking items: current={current_items}, target={item_seed_count}")
                
                if current_items < item_seed_count:
                    to_seed = item_seed_count - current_items
                    logger.info(f"Seeding {to_seed} items...")
                    batch = [
                        (
                            barnum.create_nouns(),
                            random.choice(categories),
                            random.randint(item_price_min * 100, item_price_max * 100) / 100.0,
                            random.randint(item_inventory_min, item_inventory_max),
                        )
                        for _ in range(to_seed)
                    ]
                    cursor.executemany(item_insert, batch)
                    logger.info(f"Seeded {len(batch)} items")

                logger.info("Seeding complete.")
                
    except Error as e:
        logger.error(f"Error during seeding: {e}")
        raise

def generate_load():
    while True:
        try:
            with get_connection() as connection:
                connection.autocommit = True
                with connection.cursor() as cursor:
                    logger.info("Getting item metrics...")
                    cursor.execute("SELECT id, price FROM items")
                    item_prices = [(row[0], row[1]) for row in cursor.fetchall()]
                    
                    if not item_prices:
                        logger.error("No items found! Cannot generate purchases.")
                        time.sleep(5)
                        continue

                    cursor.execute("SELECT count(*) FROM users")
                    user_count = cursor.fetchone()[0]
                    
                    if user_count == 0:
                        logger.error("No users found! Cannot generate purchases.")
                        time.sleep(5)
                        continue

                    logger.info(f"Starting continuous load generation with {len(item_prices)} items and {user_count} users...")
                    
                    while True:
                        purchase_item = random.choice(item_prices)
                        # Ensure user_id is valid (assuming serial 1-based IDs, but max ID might be > count if deletions happened)
                        # Safer to just pick a random ID up to count if we assume no holes, or query IDs. 
                        # For generated data, sequential 1..N is likely safe.
                        purchase_user = random.randint(1, user_count) 
                        
                        purchase_quantity = random.randint(1, 5)
                        campaign_id = random.choice(campaigns)
                        purchase_ts = time.strftime('%Y-%m-%d %H:%M:%S')

                        cursor.execute(purchase_insert, (
                            purchase_user,
                            purchase_item[0],
                            campaign_id,
                            purchase_quantity,
                            purchase_item[1] * purchase_quantity,
                            purchase_ts
                        ))
                        connection.commit() # Force commit
                        
                        logger.info(f"Inserted purchase for user {purchase_user}")
                        
                        # Debug: Verify data persistence
                        cursor.execute("SELECT count(*) FROM purchases")
                        cnt = cursor.fetchone()[0]
                        logger.info(f"DEBUG: Current purchases count: {cnt}")
                        
                        import socket
                        ip = socket.gethostbyname(postgres_host)
                        logger.info(f"DEBUG: Hostname {postgres_host} resolves to {ip}")

                        cursor.execute("SELECT inet_server_addr(), current_database()")
                        db_info = cursor.fetchone()
                        logger.info(f"DEBUG: Connected to PG Server: {db_info[0]}, DB: {db_info[1]}")

                        time.sleep(purchase_gen_every_ms / 1000.0)

        except Error as e:
            logger.error(f"Error during load generation: {e}")
            time.sleep(5) # Backoff

if __name__ == "__main__":
    logger.info("Starting Flash Sale Load Generator...")
    wait_for_db()
    seed_if_needed()
    generate_load()