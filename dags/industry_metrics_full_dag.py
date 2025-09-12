from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import os

# 入出力ディレクトリ
INPUT_DIR = "/opt/airflow/input"
OUTPUT_DIR = "/opt/airflow/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# GCS バケット
GCS_BUCKET = Variable.get("gcs_bucket", default_var="my-gcs-bucket-2025-demo")
# S3 バケット
S3_BUCKET = Variable.get("s3_bucket", default_var="domoproject")

# BigQuery の設定
BQ_PROJECT = "striking-yen-470200-u3"
BQ_DATASET = "analytics_dataset"
BQ_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.retail_metrics"

# ファイル名ユーティリティ（年+日付+時刻入り）
def dated_filename(prefix, suffix, ds=None):
    if ds is None:
        ds = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(OUTPUT_DIR, f"{prefix}_{ds}{suffix}")

# ---------- Retail: 前処理 ----------
def preprocess_retail(ds=None, **kwargs):
    df = pd.read_csv(f"{INPUT_DIR}/retail.csv", parse_dates=["date"])
    df.loc[df["stock"] < 0, "stock"] = 0
    out_path = dated_filename("retail_clean", ".csv")
    df.to_csv(out_path, index=False)
    return out_path

# ---------- Retail: KPI ----------
def calc_retail_metrics(ds=None, **kwargs):
    df = pd.read_csv(dated_filename("retail_clean", ".csv"), parse_dates=["date"])
    total_days = df["date"].nunique()
    stockouts = df[df["stock"] == 0]
    stockout_days = stockouts["date"].nunique()
    avg_sales = df["sales"].mean()

    metrics = {
        "total_days": total_days,
        "stockout_days": stockout_days,
        "stockout_rate": stockout_days / total_days,
        "lost_sales_estimate": avg_sales * stockout_days,
    }
    out_path = dated_filename("retail_metrics", ".csv")
    pd.DataFrame([metrics]).to_csv(out_path, index=False)
    return out_path

# ---------- Ads: 前処理 ----------
def preprocess_ads(ds=None, **kwargs):
    df = pd.read_csv(f"{INPUT_DIR}/ads.csv")
    df["CTR"] = df["clicks"] / df["impressions"]
    df["ROAS"] = df["revenue"] / df["spend"]
    out_path = dated_filename("ads_clean", ".csv")
    df.to_csv(out_path, index=False)
    return out_path

# ---------- Ads: KPI ----------
def calc_ads_metrics(ds=None, **kwargs):
    df = pd.read_csv(dated_filename("ads_clean", ".csv"))
    metrics = {
        "avg_CTR": df["CTR"].mean(),
        "avg_ROAS": df["ROAS"].mean(),
    }
    out_path = dated_filename("ads_metrics", ".csv")
    pd.DataFrame([metrics]).to_csv(out_path, index=False)
    return out_path

# ---------- DAG 設定 ----------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="industry_metrics_full_dag",
    default_args=default_args,
    description="Retail & Ads KPI pipeline with GCS + S3 + BigQuery",
    schedule_interval=None,
    catchup=False,
    tags=["prod", "retail", "ads", "bigquery", "s3"],
) as dag:

    # ✅ BigQuery Dataset を作成（存在しない場合のみ作成）
    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset",
        dataset_id=BQ_DATASET,
        project_id=BQ_PROJECT,
        exists_ok=True,
    )

    # Retail pipeline
    retail_preprocess = PythonOperator(
        task_id="preprocess_retail", python_callable=preprocess_retail
    )
    retail_metrics = PythonOperator(
        task_id="calc_retail_metrics", python_callable=calc_retail_metrics
    )
    upload_retail_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_retail_metrics_to_gcs",
        src="{{ ti.xcom_pull(task_ids='calc_retail_metrics') }}",
        dst="metrics/{{ ds }}/retail_metrics.csv",
        bucket=GCS_BUCKET,
        mime_type="text/csv",
    )
    load_retail_to_bq = GCSToBigQueryOperator(
        task_id="load_retail_metrics_to_bq",
        bucket=GCS_BUCKET,
        source_objects=["metrics/{{ ds }}/retail_metrics.csv"],
        destination_project_dataset_table=BQ_TABLE,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    # Ads pipeline
    ads_preprocess = PythonOperator(
        task_id="preprocess_ads", python_callable=preprocess_ads
    )
    ads_metrics = PythonOperator(
        task_id="calc_ads_metrics", python_callable=calc_ads_metrics
    )
    upload_ads_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_ads_metrics_to_s3",
        filename="{{ ti.xcom_pull(task_ids='calc_ads_metrics') }}",
        dest_key="metrics/{{ ds }}/ads_metrics.csv",
        dest_bucket=S3_BUCKET,
        replace=True,
    )

    # 依存関係
    create_bq_dataset >> retail_preprocess >> retail_metrics >> upload_retail_to_gcs >> load_retail_to_bq
    ads_preprocess >> ads_metrics >> upload_ads_to_s3
