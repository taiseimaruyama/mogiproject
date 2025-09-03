from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

OUTPUT_DIR = "/opt/airflow/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------- 小売業: データ生成 ----------
def generate_retail_data():
    days = pd.date_range("2024-01-01", "2024-03-31", freq="D")
    df = pd.DataFrame({
        "date": days,
        "product_id": np.random.choice(["A", "B", "C"], size=len(days)),
        "sales": np.random.randint(50, 200, size=len(days)),
        "stock": np.random.randint(-5, 100, size=len(days)),  # マイナス在庫も混ぜる
    })
    df.to_csv(f"{OUTPUT_DIR}/retail.csv", index=False)

# ---------- 小売業: 前処理 ----------
def preprocess_retail():
    df = pd.read_csv(f"{OUTPUT_DIR}/retail.csv", parse_dates=["date"])
    # 在庫が負なら0に補正
    df.loc[df["stock"] < 0, "stock"] = 0
    df.to_csv(f"{OUTPUT_DIR}/retail_clean.csv", index=False)

# ---------- 小売業: データ検証 ----------
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

# ---------- 広告業: データ生成 ----------
def generate_ads_data():
    days = pd.date_range("2024-01-01", "2024-03-31", freq="D")
    df = pd.DataFrame({
        "date": days,
        "campaign_id": np.random.choice(["CAMP1", "CAMP2"], size=len(days)),
        "impressions": np.random.randint(1000, 5000, size=len(days)),
        "clicks": np.random.randint(0, 500, size=len(days)),
        "spend": np.random.uniform(1000, 5000, size=len(days)),
        "revenue": np.random.uniform(2000, 8000, size=len(days)),
    })
    df.to_csv(f"{OUTPUT_DIR}/ads.csv", index=False)

# ---------- 広告業: 前処理 ----------
def preprocess_ads():
    df = pd.read_csv(f"{OUTPUT_DIR}/ads.csv")
    df["CTR"] = df["clicks"] / df["impressions"]
    df["ROAS"] = df["revenue"] / df["spend"]
    df.to_csv(f"{OUTPUT_DIR}/ads_clean.csv", index=False)

# ---------- 広告業: データ検証 ----------
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
    dag_id="industry_metrics_full",
    default_args=default_args,
    description="Retail→BigQuery, Ads→S3 PoC with preprocessing & validation",
    schedule_interval="@daily",
    catchup=False,
    concurrency=4,
    max_active_runs=1,
    tags=["poc", "retail", "ads", "bigquery", "s3", "validation"],
) as dag:

    # 小売業タスク
    retail_generate = PythonOperator(task_id="generate_retail_data", python_callable=generate_retail_data)
    retail_preprocess = PythonOperator(task_id="preprocess_retail", python_callable=preprocess_retail)
    retail_validate = PythonOperator(task_id="validate_retail", python_callable=validate_retail)
    retail_metrics = PythonOperator(task_id="calc_retail_metrics", python_callable=calc_retail_metrics)

    upload_retail_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_retail_metrics_to_gcs",
        src=f"{OUTPUT_DIR}/retail_metrics.csv",
        dst="metrics/retail_metrics.csv",
        bucket="my-gcs-bucket",
    )

    load_retail_to_bq = GCSToBigQueryOperator(
        task_id="load_retail_metrics_to_bq",
        bucket="my-gcs-bucket",
        source_objects=["metrics/retail_metrics.csv"],
        destination_project_dataset_table="my_project.my_dataset.retail_metrics",
        schema_fields=[
            {"name": "total_days", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "stockout_days", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "stockout_rate", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "lost_sales_estimate", "type": "FLOAT", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    # 広告業タスク
    ads_generate = PythonOperator(task_id="generate_ads_data", python_callable=generate_ads_data)
    ads_preprocess = PythonOperator(task_id="preprocess_ads", python_callable=preprocess_ads)
    ads_validate = PythonOperator(task_id="validate_ads", python_callable=validate_ads)
    ads_metrics = PythonOperator(task_id="calc_ads_metrics", python_callable=calc_ads_metrics)

    upload_ads_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_ads_metrics_to_s3",
        filename=f"{OUTPUT_DIR}/ads_metrics.csv",
        dest_key="metrics/ads_metrics.csv",
        dest_bucket="my-s3-bucket",
        replace=True,
    )

    # 依存関係
    retail_generate >> retail_preprocess >> retail_validate >> retail_metrics >> upload_retail_to_gcs >> load_retail_to_bq
    ads_generate >> ads_preprocess >> ads_validate >> ads_metrics >> upload_ads_to_s3

