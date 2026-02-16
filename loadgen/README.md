# Load Generator Service

This service generates synthetic data to simulate a realistic e-commerce environment. It populates a Postgres database with users, items, and purchases, and simultaneously generates "pageview" events in MinIO (S3) to simulate user browsing behavior.

## 🚀 Features

- **Synthetic Data**: Uses `barnum` and `random` to generate realistic names, emails, products, and categories.
- **Dual-Destination**: Writes relational data to Postgres and unstructured JSON events to MinIO.
- **Configurable**: Behavior can be tuned via environment variables.

## ⚙️ Configuration

The following environment variables can be set in the root `.env` file:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `USERS_SEED_COUNT` | 10000 | Number of user profiles to create in Postgres. |
| `ITEM_SEED_COUNT` | 1000 | Number of product items to create. |
| `PURCHASE_GEN_COUNT` | 100 | Number of purchase transactions to simulate per run. |
| `PURCHASE_GEN_EVERY_MS` | 100 | Delay (ms) between generated purchases. |
| `PAGEVIEW_MULTIPLIER` | 75 | Average number of pageviews per purchase. |
| `POSTGRES_HOST` | postgres | Hostname of the Postgres service. |
| `MINIO_URL` | http://minio:9000 | URL of the MinIO service. |

## 🏃 Usage

Run via Docker Compose:

```bash
docker-compose run loadgen
```

This will run the script once and exit. You can run it multiple times to generate more data.
