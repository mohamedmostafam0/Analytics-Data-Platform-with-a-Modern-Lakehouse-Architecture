FROM apache/superset
USER root
RUN /app/.venv/bin/python -m ensurepip --upgrade && \
    /app/.venv/bin/python -m pip install psycopg2-binary trino[sqlalchemy] sqlalchemy-trino
USER superset
