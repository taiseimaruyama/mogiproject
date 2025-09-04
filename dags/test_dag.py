from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

with DAG(
    dag_id="test_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # ローカルファイルを出力するタスク
    make_file = BashOperator(
        task_id="make_file",
        bash_command="echo 'Hello Data' > /tmp/hello.txt"
    )

    # GCS へアップロード
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="/tmp/hello.txt",
        dst="hello.txt",
        bucket="my-gcs-bucket-2025-demo"
    )

    # S3 へアップロード
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename="/tmp/hello.txt",
        dest_key="hello.txt",
        dest_bucket="domoproject"
    )

    # BigQuery にロード
    load_to_bq = BigQueryInsertJobOperator(
        task_id="load_to_bq",
        configuration={
            "query": {
                "query": "SELECT 'Hello from BigQuery' AS message",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "{{ var.value.gcp_project_id }}",
                    "datasetId": "{{ var.value.bigquery_dataset }}",
                    "tableId": "test_table"
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        }
    )

    make_file >> [upload_to_gcs, upload_to_s3] >> load_to_bq
