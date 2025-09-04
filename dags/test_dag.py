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
GCS_BUCKET = Variable.get("gcs_bucket")
BQ_DATASET = Variable.get("bq_dataset")
S3_BUCKET = Variable.get("s3_bucket")

OUTPUT_DIR = "/opt/airflow/output"
INPUT_DIR = "/opt/airflow/input"

# 小売データの前処理
def preprocess_retail():
    df = pd.read_csv(os.path.join(INPUT_DIR, "retail.csv"))
    df = df.fillna(0)  # 欠損値処理
    df["stockout_rate"] = (df["stockout"] / df["stock"]).fillna(0)
    df["lost_sales"] = df["stockout"] * df["price"]
    out_path = os.path.join(OUTPUT_DIR, "retail_processed.csv")
    df.to_csv(out_path, index=False)

# 広告データの前処理
def preprocess_ads():
    df = pd.read_csv(os.path.join(INPUT_DIR, "ads.csv"))
    df = df.fillna(0)
    df["CTR"] = df["clicks"] / df["impressions"]
    df["ROAS"] = df["revenue"] / df["spend"]
    out_path = os.path.join(OUTPUT_DIR, "ads_processed.csv")
    df.to_csv(out_path, index=False)

with DAG(
    dag_id="test_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["demo", "retail", "ads"],
) as dag:

    preprocess_retail_task = PythonOperator(
        task_id="preprocess_retail",
        python_callable=preprocess_retail,
    )

    preprocess_ads_task = PythonOperator(
        task_id="preprocess_ads",
        python_callable=preprocess_ads,
    )

    upload_retail_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_retail_to_gcs",
        src=os.path.join(OUTPUT_DIR, "retail_processed.csv"),
        dst="retail/retail_processed.csv",
        bucket=GCS_BUCKET,
    )

    load_retail_to_bq = GCSToBigQueryOperator(
        task_id="load_retail_to_bq",
        bucket=GCS_BUCKET,
        source_objects=["retail/retail_processed.csv"],
        destination_project_dataset_table=f"{BQ_DATASET}.retail_processed",
        source_format="CSV",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    upload_ads_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_ads_to_s3",
        filename=os.path.join(OUTPUT_DIR, "ads_processed.csv"),
        dest_key="ads/ads_processed.csv",
        dest_bucket=S3_BUCKET,
        replace=True,
    )

    preprocess_retail_task >> upload_retail_to_gcs >> load_retail_to_bq
    preprocess_ads_task >> upload_ads_to_s3
