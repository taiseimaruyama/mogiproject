from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import os


# ==== 前処理関数 ====
def preprocess_retail(**context):
    df = pd.read_csv("input/retail.csv")
    # サンプル前処理
    df["stockout_rate"] = (df["stockout"] / df["stock"]) * 100
    df["lost_sales"] = df["stockout"] * df["price"]

    os.makedirs("output", exist_ok=True)
    output_path = "output/retail_processed.csv"
    df.to_csv(output_path, index=False)
    return output_path


def preprocess_ads(**context):
    df = pd.read_csv("input/ads.csv")
    # サンプル前処理
    df["CTR"] = df["clicks"] / df["impressions"]
    df["ROAS"] = df["revenue"] / df["spend"]

    os.makedirs("output", exist_ok=True)
    output_path = "output/ads_processed.csv"
    df.to_csv(output_path, index=False)
    return output_path


# ==== DAG 定義 ====
with DAG(
    dag_id="kpi_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Retail 前処理
    preprocess_retail_task = PythonOperator(
        task_id="preprocess_retail",
        python_callable=preprocess_retail,
    )

    # Ads 前処理
    preprocess_ads_task = PythonOperator(
        task_id="preprocess_ads",
        python_callable=preprocess_ads,
    )

    # Retail → GCS
    upload_retail_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_retail_gcs",
        src="output/retail_processed.csv",
        dst="retail/retail_processed.csv",
        bucket=Variable.get("gcs_bucket"),
    )

    # GCS → BigQuery
    load_retail_bigquery = GCSToBigQueryOperator(
        task_id="load_retail_bigquery",
        bucket=Variable.get("gcs_bucket"),
        source_objects=["retail/retail_processed.csv"],
        destination_project_dataset_table=f"{Variable.get('bq_dataset')}.retail_kpi",
        write_disposition="WRITE_TRUNCATE",
        source_format="CSV",
        autodetect=True,
    )

    # Ads → S3
    upload_ads_s3 = LocalFilesystemToS3Operator(
        task_id="upload_ads_s3",
        filename="output/ads_processed.csv",
        dest_key="ads/ads_processed.csv",
        dest_bucket=Variable.get("s3_bucket"),
        replace=True,
    )

    # DAG依存関係
    preprocess_retail_task >> upload_retail_gcs >> load_retail_bigquery
    preprocess_ads_task >> upload_ads_s3
