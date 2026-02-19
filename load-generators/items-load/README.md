# Items Load Generator

A Python-based synthetic data generator for populating the PostgreSQL `items` table. It simulates an e-commerce catalog with realistic product names, categories, and pricing.

## 🚀 Features
-   Generates realistic product data using `Faker`.
-   Populates `users`, `items`, and `purchases` tables (depending on script logic).
-   Idempotent: Checks if data exists before inserting.

## 🛠️ Usage

### Via Docker Compose
This is the recommended way to run the generator within the platform network.

```bash
# Build the image
docker compose build items-loadgen

# Run the generator
docker compose run items-loadgen
```

### Local Development
To run the script locally (outside Docker), you need to set the environment variables manually or use a local `.env` file.

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Run Script**:
    ```bash
    export POSTGRES_HOST=localhost
    export POSTGRES_USER=postgresuser
    export POSTGRES_PASSWORD=postgrespw
    export POSTGRES_DB=oneshop
    
    python item_seeder.py
    ```

## ⚙️ Configuration
The generator is configured via environment variables (defined in `.env`):

| Variable | Description | Default |
| :--- | :--- | :--- |
| `POSTGRES_HOST` | Database host | `postgres` |
| `POSTGRES_PORT` | Database port | `5432` |
| `POSTGRES_DB` | Database name | `oneshop` |
| `ITEM_SEED_COUNT` | Number of items to generate | `1000` |
| `ITEM_PRICE_MIN` | Minimum price | `5` |
| `ITEM_PRICE_MAX` | Maximum price | `500` |
