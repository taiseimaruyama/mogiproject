from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import os

# 入出力ディレクトリ
INPUT_DIR = "/opt/airflow/input"
OUTPUT_DIR = "/opt/airflow/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# バケット名は Secrets 経由（Airflow Variables）
GCS_BUCKET = Variable.get("gcs_bucket", default_var="my-gcs-bucket-2025-demo")
S3_BUCKET = Variable.get("s3_bucket", default_var="domoproject")

# BigQuery は本番用に固定
BQ_TABLE = "striking-yen-470200-u3.sales_dataset.retail_metrics"

def dated_filename(prefix, suffix, ds=None):
    if ds is None:
        ds = datetime.now().strftime("%Y-%m-%d")
    return os.path.join(OUTPUT_DIR, f"{prefix}_{ds}{suffix}")

# ---------- 小売業: 前処理 ----------
def preprocess_retail(ds=None, **kwargs):
    try:
        df = pd.read_csv(f"{INPUT_DIR}/retail.csv", parse_dates=["date"])
        df.loc[df["stock"] < 0, "stock"] = 0
        out_path = dated_filename("retail_clean", ".csv", ds)
        df.to_csv(out_path, index=False)
        return out_path
    finally:
        if "df" in locals():
            df.to_csv(dated_filename("retail_clean_backup", ".csv", ds), index=False)

# ---------- 小売業: KPI ----------
def calc_retail_metrics(ds=None, **kwargs):
    try:
        df = pd.read_csv(dated_filename("retail_clean", ".csv", ds), parse_dates=["date"])
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
        out_path = dated_filename("retail_metrics", ".csv", ds)
        pd.DataFrame([metrics]).to_csv(out_path, index=False)
        return out_path
    finally:
        if "metrics" in locals():
            pd.DataFrame([metrics]).to_csv(dated_filename("retail_metrics_backup", ".csv", ds), index=False)

# ---------- 広告業: 前処理 ----------
def preprocess_ads(ds=None, **kwargs):
    try:
        df = pd.read_csv(f"{INPUT_DIR}/ads.csv")
        df["CTR"] = df["clicks"] / df["impressions"]
        df["ROAS"] = df["revenue"] / df["spend"]
        out_path = dated_filename("ads_clean", ".csv", ds)
        df.to_csv(out_path, index=False)
        return out_path
    finally:
        if "df" in locals():
            df.to_csv(dated_filename("ads_clean_backup", ".csv", ds), index=False)

# ---------- 広告業: KPI ----------
def calc_ads_metrics(ds=None, **kwargs):
    try:
        df = pd.read_csv(dated_filename("ads_clean", ".csv", ds))
        metrics = {
            "avg_CTR": df["CTR"].mean(),
            "avg_ROAS": df["ROAS"].mean(),
        }
        out_path = dated_filename("ads_metrics", ".csv", ds)
        pd.DataFrame([metrics]).to_csv(out_path, index=False)
        return out_path
    finally:
        if "metrics" in locals():
            pd.DataFrame([metrics]).to_csv(dated_filename("ads_metrics_backup", ".csv", ds), index=False)

# ---------- DAG設定 ----------
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
    description="Retail & Ads KPI pipeline with GCS/S3 upload + BigQuery load",
    schedule_interval=None,
    catchup=False,
    tags=["poc", "retail", "ads", "bigquery", "s3"],
) as dag:

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
    )

    load_retail_to_bq = GCSToBigQueryOperator(
        task_id="load_retail_metrics_to_bq",
        bucket=GCS_BUCKET,
        source_objects=["metrics/{{ ds }}/retail_metrics.csv"],
        destination_project_dataset_table=BQ_TABLE,  # 固定
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
    retail_preprocess >> retail_metrics >> upload_retail_to_gcs >> load_retail_to_bq
    ads_preprocess >> ads_metrics >> upload_ads_to_s3
