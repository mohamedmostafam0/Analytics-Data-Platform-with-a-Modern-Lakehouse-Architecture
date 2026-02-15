# ETL Pipeline Tests

This directory contains end-to-end (E2E) tests for the Data Lakehouse pipeline. The tests use `pytest` and run directly inside the Spark container.

## 🧪 Test Suite

- **`test_pipeline_e2e.py`**: Runs a full integration validation:
    1.  Verifies that Bronze, Silver, and Gold tables exist in the customized Iceberg catalog.
    2.  Checks that data is correctly flowing from source (simulated) to final tables.
    3.  Validates data quality (e.g., no null IDs, positive prices).
- **`test_etl_utils.py`**: Unit tests for utility functions in `etl_utils.py`.

## 🏃 Running Tests

Tests are designed to run inside the Docker container where Spark is available.

1.  **Ensure the stack is up**:
    ```bash
    docker-compose up -d
    ```

2.  **Run the tests**:
    ```bash
    docker exec spark-iceberg pytest /home/iceberg/scripts/tests/
    ```

## ⚙️ Test Configuration

- **`conftest.py`**: Configures the `pytest` environment, mainly providing a `spark` fixture that initializes a SparkSession optimized for testing (using `local` master).
