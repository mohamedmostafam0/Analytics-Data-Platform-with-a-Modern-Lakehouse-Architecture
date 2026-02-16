"""Auto-create the Trino Iceberg database connection in Superset."""
from superset.app import create_app

app = create_app()
with app.app_context():
    from superset.models.core import Database
    from superset.extensions import db as sa_db

    if not sa_db.session.query(Database).filter_by(database_name="Trino Iceberg").first():
        trino_db = Database(
            database_name="Trino Iceberg",
            sqlalchemy_uri="trino://trino@trino:8080/iceberg",
        )
        sa_db.session.add(trino_db)
        sa_db.session.commit()
        print("Created Trino Iceberg connection")
    else:
        print("Trino Iceberg connection already exists")
