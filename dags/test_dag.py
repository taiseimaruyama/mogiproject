from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

with DAG(
    dag_id="test_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Step1: ダミーファイル生成
    generate_file = BashOperator(
        task_id="generate_file",
        bash_command="echo 'id,name\n1,Alice\n2,Bob' > /tmp/sample.csv",
    )

    # Step2: S3 にアップロード
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename="/tmp/sample.csv",
        dest_key="data/sample.csv",
        dest_bucket="your-s3-bucket-name",  # 🔧 修正ポイント
        aws_conn_id="aws_default",
        replace=True,
    )

    # Step3: GCS にアップロード
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="/tmp/sample.csv",
        dst="data/sample.csv",
        bucket="your-gcs-bucket-name",      # 🔧 修正ポイント
        gcp_conn_id="google_cloud_default",
        mime_type="text/csv",
    )

    # Step4: BigQuery にロード
    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket="your-gcs-bucket-name",      # 🔧 修正ポイント
        source_objects=["data/sample.csv"],
        destination_project_dataset_table="your_project_id.your_dataset.sales_table",  # 🔧 修正ポイント
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    # DAG の依存関係
    generate_file >> upload_to_s3 >> upload_to_gcs >> load_to_bigquery
