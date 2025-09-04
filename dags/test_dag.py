from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import os

# Airflow Variables から取得
GCS_BUCKET = Variable.get("gcs_bucket")        # 例: my-gcs-bucket-2025-demo
BQ_DATASET = Variable.get("bq_dataset")        # 例: your_dataset_name
S3_BUCKET = Variable.get("s3_bucket")          # 例: domoproject

OUTPUT_DIR = "/opt/airflow/output"
INPUT_DIR = "/opt/airflow/input"


# ===== 前処理関数 =====
def preprocess_retail():
    df = pd.read_csv(os.path.join(INPUT_DIR, "retail.csv"))

    # 欠損値補完
    df = df.fillna(0)

    # KPI算出: stockout_rate と lost_sales を追加
    df["stockout_rate"] = (df["stockout"] / df["sales"]).fillna(0)
    df["lost_sales"] = df["stockout"] * df["price"]

    out_path = os.path.join(OUTPUT_DIR, "retail_processed.csv")
    df.to_csv(out_path, index=False)


def preprocess_ads():
    df = pd.read_csv(os.path.join(INPUT_DIR, "ads.csv"))

    # 欠損値補完
    df = df.fillna(0)

    out_path = os.path.join(OUTPUT_DIR, "ads_processed.csv")
    df.to_csv(out_path, index=False)


# ===== DAG 定義 =====
with DAG(
    dag_id="test_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["demo"],
) as dag:

    # 1. Retail 前処理
    preprocess_retail_task = PythonOperator(
        task_id="preprocess_retail",
        python_callable=preprocess_retail,
    )

    # 2. Ads 前処理
    preprocess_ads_task = PythonOperator(
        task_id="preprocess_ads",
        python_callable=preprocess_ads,
    )

    # 3. retail_processed.csv → GCS
    upload_retail_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_retail_to_gcs",
        src=os.path.join(OUTPUT_DIR, "retail_processed.csv"),
        dst="retail/retail_processed.csv",
        bucket=GCS_BUCKET,
    )

    # 4. GCS → BigQuery (retail_metrics テーブル)
    load_retail_to_bq = GCSToBigQueryOperator(
        task_id="load_retail_to_bq",
        bucket=GCS_BUCKET,
        source_objects=["retail/retail_processed.csv"],
        destination_project_dataset_table=f"{BQ_DATASET}.retail_metrics",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        skip_leading_rows=1,
    )

    # 5. ads_processed.csv → S3
    upload_ads_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_ads_to_s3",
        filename=os.path.join(OUTPUT_DIR, "ads_processed.csv"),
        dest_key="ads/ads_processed.csv",
        dest_bucket=S3_BUCKET,
        replace=True,
    )

    # DAG の依存関係
    preprocess_retail_task >> upload_retail_to_gcs >> load_retail_to_bq
    preprocess_ads_task >> upload_ads_to_s3
