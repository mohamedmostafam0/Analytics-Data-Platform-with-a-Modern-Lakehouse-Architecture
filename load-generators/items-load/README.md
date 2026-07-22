# Items Load Generator

A Python-based synthetic data generator for populating the PostgreSQL `items` table. It simulates an e-commerce catalog with realistic product names, categories, and pricing.

## 🚀 Features
-   Generates realistic product data using `Faker`.
-   Populates the `items` table.
-   Uses a PostgreSQL advisory lock to serialize concurrent seed jobs.
-   Defaults to `skip-if-present`, so a successful rerun makes no changes when the
    table already contains data.

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
    export POSTGRES_USER=<local-user>
    export POSTGRES_PASSWORD=<local-password>
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
| `ITEM_SEED_MODE` | `skip-if-present` or explicit `append` | `skip-if-present` |

`POSTGRES_USER` and `POSTGRES_PASSWORD` are required and have no application
defaults. The container uses a digest-pinned Python 3.12 base and runs as UID/GID
`10001`.
