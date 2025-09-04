from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

# 入出力ディレクトリ
INPUT_DIR = "/opt/airflow/input"
OUTPUT_DIR = "/opt/airflow/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------- 小売業: 前処理 ----------
def preprocess_retail():
    df = pd.read_csv(f"{INPUT_DIR}/retail.csv", parse_dates=["date"])
    df.loc[df["stock"] < 0, "stock"] = 0
    df.to_csv(f"{OUTPUT_DIR}/retail_clean.csv", index=False)

# ---------- 小売業: 検証 ----------
def validate_retail():
    df = pd.read_csv(f"{OUTPUT_DIR}/retail_clean.csv")
    assert (df["sales"] >= 0).all(), "❌ salesに負の値があります"
    assert (df["stock"] >= 0).all(), "❌ stockに負の値があります"
    print("✅ retail_clean.csv 検証OK")

# ---------- 小売業: KPI ----------
def calc_retail_metrics():
    df = pd.read_csv(f"{OUTPUT_DIR}/retail_clean.csv", parse_dates=["date"])
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
    pd.DataFrame([metrics]).to_csv(f"{OUTPUT_DIR}/retail_metrics.csv", index=False)

# ---------- 広告業: 前処理 ----------
def preprocess_ads():
    df = pd.read_csv(f"{INPUT_DIR}/ads.csv")
    df["CTR"] = df["clicks"] / df["impressions"]
    df["ROAS"] = df["revenue"] / df["spend"]
    df.to_csv(f"{OUTPUT_DIR}/ads_clean.csv", index=False)

# ---------- 広告業: 検証 ----------
def validate_ads():
    df = pd.read_csv(f"{OUTPUT_DIR}/ads_clean.csv")
    assert (df["CTR"].between(0, 1)).all(), "❌ CTRが範囲外です"
    assert (df["ROAS"] >= 0).all(), "❌ ROASに負の値があります"
    print("✅ ads_clean.csv 検証OK")

# ---------- 広告業: KPI ----------
def calc_ads_metrics():
    df = pd.read_csv(f"{OUTPUT_DIR}/ads_clean.csv")
    metrics = {
        "avg_CTR": df["CTR"].mean(),
        "avg_ROAS": df["ROAS"].mean(),
    }
    pd.DataFrame([metrics]).to_csv(f"{OUTPUT_DIR}/ads_metrics.csv", index=False)

# ---------- DAG設定 ----------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="industry_metrics_from_input",
    default_args=default_args,
    description="PoC: Retail & Ads KPI from existing CSV",
    schedule_interval="@daily",
    catchup=False,
    tags=["poc", "retail", "ads", "bigquery", "s3"],
) as dag:

    # Retail pipeline
    retail_preprocess = PythonOperator(task_id="preprocess_retail", python_callable=preprocess_retail)
    retail_validate = PythonOperator(task_id="validate_retail", python_callable=validate_retail)
    retail_metrics = PythonOperator(task_id="calc_retail_metrics", python_callable=calc_retail_metrics)

    upload_retail_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_retail_metrics_to_gcs",
        src=f"{OUTPUT_DIR}/retail_metrics.csv",
        dst="metrics/retail_metrics.csv",
        bucket="my-gcs-bucket-2025-demo",   # ✅ 修正版バケット名
    )

    load_retail_to_bq = GCSToBigQueryOperator(
        task_id="load_retail_metrics_to_bq",
        bucket="my-gcs-bucket-2025-demo",   # ✅ 修正版バケット名
        source_objects=["metrics/retail_metrics.csv"],
        destination_project_dataset_table="striking-yen-470200-u3.sales_dataset.retail_metrics",  # ✅ プロジェクトIDとデータセット
        schema_fields=[
            {"name": "total_days", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "stockout_days", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "stockout_rate", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "lost_sales_estimate", "type": "FLOAT", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    # Ads pipeline
    ads_preprocess = PythonOperator(task_id="preprocess_ads", python_callable=preprocess_ads)
    ads_validate = PythonOperator(task_id="validate_ads", python_callable=validate_ads)
    ads_metrics = PythonOperator(task_id="calc_ads_metrics", python_callable=calc_ads_metrics)

    upload_ads_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_ads_metrics_to_s3",
        filename=f"{OUTPUT_DIR}/ads_metrics.csv",
        dest_key="metrics/ads_metrics.csv",
        dest_bucket="domoproject",   # ✅ 修正版バケット名
        replace=True,
    )

    # 依存関係
    retail_preprocess >> retail_validate >> retail_metrics >> upload_retail_to_gcs >> load_retail_to_bq
    ads_preprocess >> ads_validate >> ads_metrics >> upload_ads_to_s3
