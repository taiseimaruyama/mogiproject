from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from datetime import datetime, timedelta
import pandas as pd
import os

# 入出力ディレクトリ
INPUT_DIR = "/opt/airflow/input"
OUTPUT_DIR = "/opt/airflow/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 実行日を Airflow の execution_date から取得
def dated_filename(prefix, suffix, ds=None):
    if ds is None:
        ds = datetime.now().strftime("%Y-%m-%d")
    return os.path.join(OUTPUT_DIR, f"{prefix}_{ds}{suffix}")

# ---------- 小売業: 前処理 ----------
def preprocess_retail(ds=None, **kwargs):
    df = pd.read_csv(f"{INPUT_DIR}/retail.csv", parse_dates=["date"])
    df.loc[df["stock"] < 0, "stock"] = 0
    out_path = dated_filename("retail_clean", ".csv", ds)
    df.to_csv(out_path, index=False)
    return out_path

# ---------- 小売業: KPI 計算 ----------
def calc_retail_metrics(ds=None, **kwargs):
    df = pd.read_csv(dated_filename("retail_clean", ".csv", ds), parse_dates=["date"])
    stockout_rate = (df["stock"] == 0).mean()
    lost_sales_estimate = df.loc[df["stock"] == 0, "sales"].sum()
    metrics = pd.DataFrame([{
        "date": ds,
        "stockout_rate": stockout_rate,
        "lost_sales_estimate": lost_sales_estimate,
    }])
    out_path = dated_filename("retail_metrics", ".csv", ds)
    metrics.to_csv(out_path, index=False)
    return out_path

# ---------- 広告: 前処理 ----------
def preprocess_ads(ds=None, **kwargs):
    df = pd.read_csv(f"{INPUT_DIR}/ads.csv", parse_dates=["date"])
    df["CTR"] = df["clicks"] / df["impressions"]
    df["ROAS"] = df["revenue"] / df["spend"]
    out_path = dated_filename("ads_clean", ".csv", ds)
    df.to_csv(out_path, index=False)
    return out_path

# ---------- 広告: KPI 計算 ----------
def calc_ads_metrics(ds=None, **kwargs):
    df = pd.read_csv(dated_filename("ads_clean", ".csv", ds), parse_dates=["date"])
    metrics = pd.DataFrame([{
        "date": ds,
        "avg_CTR": df["CTR"].mean(),
        "avg_ROAS": df["ROAS"].mean(),
    }])
    out_path = dated_filename("ads_metrics", ".csv", ds)
    metrics.to_csv(out_path, index=False)
    return out_path


# ---------- DAG 定義 ----------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="industry_metrics_full_dag",
    default_args=default_args,
    description="Retail & Ads metrics DAG",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["retail", "ads"],
) as dag:

    # 小売業タスク
    t1 = PythonOperator(
        task_id="preprocess_retail",
        python_callable=preprocess_retail,
    )

    t2 = PythonOperator(
        task_id="calc_retail_metrics",
        python_callable=calc_retail_metrics,
    )

    upload_retail_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_retail_metrics_to_gcs",
        src="{{ ti.xcom_pull(task_ids='calc_retail_metrics') }}",
        dst="metrics/{{ ds }}/retail_metrics.csv",   # ✅ 修正ポイント
        bucket=os.environ.get("GCS_BUCKET"),
        mime_type="text/csv",
    )

    # 広告タスク
    t3 = PythonOperator(
        task_id="preprocess_ads",
        python_callable=preprocess_ads,
    )

    t4 = PythonOperator(
        task_id="calc_ads_metrics",
        python_callable=calc_ads_metrics,
    )

    upload_ads_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_ads_metrics_to_s3",
        filename="{{ ti.xcom_pull(task_ids='calc_ads_metrics') }}",
        dest_key="metrics/{{ ds }}/ads_metrics.csv",   # ✅ S3もフォルダ構造を揃える
        dest_bucket=os.environ.get("S3_BUCKET"),
    )

    # 依存関係
    t1 >> t2 >> upload_retail_to_gcs
    t3 >> t4 >> upload_ads_to_s3
