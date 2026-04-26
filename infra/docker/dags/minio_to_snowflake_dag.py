from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import boto3
import os
import snowflake.connector

# ======================
# CONFIG
# ======================

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admindoc"
BUCKET = "bronze-transaction"
LOCAL_DIR = "/tmp/minio_files"

# ======================
# 1️⃣ Download from MinIO
# ======================

def download_from_minio(**kwargs):
    os.makedirs(LOCAL_DIR, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    objects = s3.list_objects_v2(Bucket=BUCKET)
    downloaded_files = []

    if "Contents" in objects:
        for obj in objects["Contents"]:
            key = obj["Key"]

            if key.endswith("/"):
                continue

            local_path = os.path.join(LOCAL_DIR, os.path.basename(key))
            s3.download_file(BUCKET, key, local_path)
            downloaded_files.append(local_path)

    print("Downloaded files:", downloaded_files)
    return downloaded_files


# ======================
# 2️⃣ Load to Snowflake
# ======================

def load_to_snowflake(**kwargs):

    ti = kwargs["ti"]
    files = ti.xcom_pull(task_ids="download_minio")

    if not files:
        print("No new files found. Task will succeed.")
        return

    conn = BaseHook.get_connection("snowflake_conn")

    sf = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson["account"],
        warehouse=conn.extra_dejson["warehouse"],
        database=conn.extra_dejson["database"],
        schema=conn.schema,
        role=conn.extra_dejson["role"],
    )

    cur = sf.cursor()

    try:
        print("Connected to Snowflake ✅")

        # Create stage if not exists
        cur.execute("CREATE STAGE IF NOT EXISTS internal_stage")

        # Upload only if not already present
        for file in files:
            cur.execute(
                f"PUT file://{file} @internal_stage AUTO_COMPRESS=TRUE"
            )

        print("Files uploaded to stage.")

        # COPY only new files (Snowflake handles dedup automatically)
        copy_result = cur.execute("""
            COPY INTO bronze_stocks_quotes_raw
            FROM @internal_stage
            FILE_FORMAT = (TYPE = JSON)
        """).fetchall()

        print("COPY RESULT:")
        total_rows = 0

        for row in copy_result:
            print(row)
            if len(row) > 3:
                total_rows += row[3]  # rows_loaded column

        print(f"Total Rows Loaded: {total_rows}")

        if total_rows == 0:
            print("No new rows loaded (files already processed).")
        else:
            print("Data loaded successfully 🎉")

    except Exception as e:
        print("ERROR:", e)
        raise e

    finally:
        cur.close()
        sf.close()


# ======================
# DAG
# ======================

default_args = {
    "owner": "hazem",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="minio_to_snowflake_pipeline",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    download_task = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    load_task = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
    )

    download_task >> load_task