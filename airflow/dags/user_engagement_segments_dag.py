from airflow import DAG
from airflow.models import Variable
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "mohamed mostafa",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

SEGMENT_BUCKET = "customer-segments"


def export_segmented_users_to_csv(**context):
    """Query Trino for segmented users and write result to a local CSV."""
    ds = context["ds"]
    output_path = f"/tmp/segmented_users_{ds}.csv"

    query = """
        SELECT user_id, email, full_name, total_pageviews, active_days,
               last_active_date, days_since_last_active, engagement_segment
        FROM iceberg.gold.user_engagement_segments
        WHERE segment_date = CURRENT_DATE
    """

    trino_hook = TrinoHook(trino_conn_id="trino_default")
    df = trino_hook.get_pandas_df(sql=query)
    df.to_csv(output_path, index=False)

    context["ti"].xcom_push(key="csv_path", value=output_path)


def upload_csv_to_minio(**context):
    """Upload the exported CSV to MinIO using an Airflow S3 connection."""
    ds = context["ds"]
    local_file_path = f"/tmp/segmented_users_{ds}.csv"
    object_key = f"segment_users/segmented_users_{ds}.csv"

    s3_hook = S3Hook(aws_conn_id="minio_default")

    # Create bucket if it doesn't exist
    if not s3_hook.check_for_bucket(SEGMENT_BUCKET):
        s3_hook.create_bucket(bucket_name=SEGMENT_BUCKET)

    s3_hook.load_file(
        filename=local_file_path,
        key=object_key,
        bucket_name=SEGMENT_BUCKET,
        replace=True,
    )

    s3_path = f"s3://{SEGMENT_BUCKET}/{object_key}"
    print(f"Uploaded {local_file_path} to {s3_path}")
    context["ti"].xcom_push(key="s3_path", value=s3_path)


with DAG(
    dag_id="user_engagement_segments_dag",
    description="Segment users by engagement, export to CSV, and upload to MinIO",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["gold", "engagement", "export"],
) as dag:

    segment_users = TrinoOperator(
        task_id="segment_users",
        sql="sql/trino.sql",
        trino_conn_id="trino_default",
    )

    export_csv = PythonOperator(
        task_id="export_to_csv",
        python_callable=export_segmented_users_to_csv,
    )

    upload_to_minio = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_csv_to_minio,
    )

    notify_success = EmailOperator(
        task_id="notify_success",
        to="engmohamedmostafa.m@gmail.com",
        subject="[Airflow] User Engagement Segments Exported",
        html_content="""
            <p>Hello Team,</p>
            <p>The user engagement segments have been refreshed and exported.</p>
            <p>Execution date: <code>{{ ds }}</code></p>
            <p>– Airflow</p>
        """,
    )

    segment_users >> export_csv >> upload_to_minio >> notify_success