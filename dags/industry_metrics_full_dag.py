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

# ファイル名を execution_date に基づいて生成
def dated_filename(prefix, suffix, ds=None):
    if ds is None:
        ds = datetime.now().strftime("%Y%m%dT%H%M%S")  # ✅ 現在の実行時刻を使う
    return os.path.join(OUTPUT_DIR, f"{prefix}_{ds}{suffix}")

# ---------- 小売業: 前処理 ----------
def preprocess_retail(ds=None, **kwargs):
    df = pd.read_csv(f"{INPUT_DIR}/retail.csv", parse_dates=["date"])
    df.loc[df["stock"] < 0, "stock"] = 0
    out_path = dated_filename("retail_clean", ".csv", ds)
    df.to_csv(out_path, index=False)
    print(f"✅ Preprocessed retail data saved: {out_path}")

# ---------- 小売業: KPI 計算 ----------
def calc_retail_metrics(ds=None, **kwargs):
    df = pd.read_csv(dated_filename("retail_clean", ".csv", ds), parse_dates=["date"])
    stockout_days = (df["stock"] == 0).sum()
    total_days = len(df)
    stockout_rate = stockout_days / total_days if total_days > 0 else 0
    lost_sales_estimate = df.loc[df["stock"] == 0, "sales"].sum()

    metrics = pd.DataFrame([{
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stockout_rate": stockout_rate,
        "lost_sales_estimate": lost_sales_estimate,
    }])

    out_path = dated_filename("retail_metrics", ".csv", ds)
    metrics.to_csv(out_path, index=False)
    print(f"✅ Retail metrics saved: {out_path}")

# ---------- 広告: 前処理 ----------
def preprocess_ads(ds=None, **kwargs):
    df = pd.read_csv(f"{INPUT_DIR}/ads.csv")
    df["CTR"] = df["clicks"] / df["impressions"]
    df["ROAS"] = df["revenue"] / df["spend"]
    out_path = dated_filename("ads_clean", ".csv", ds)
    df.to_csv(out_path, index=False)
    print(f"✅ Preprocessed ads data saved: {out_path}")

# ---------- 広告: KPI 計算 ----------
def calc_ads_metrics(ds=None, **kwargs):
    df = pd.read_csv(dated_filename("ads_clean", ".csv", ds))
    avg_ctr = df["CTR"].mean()
    avg_roas = df["ROAS"].mean()

    metrics = pd.DataFrame([{
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "avg_CTR": avg_ctr,
        "avg_ROAS": avg_roas,
    }])

    out_path = dated_filename("ads_metrics", ".csv", ds)
    metrics.to_csv(out_path, index=False)
    print(f"✅ Ads metrics saved: {out_path}")

# ---------- DAG 定義 ----------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="industry_metrics_full_dag",
    default_args=default_args,
    description="Retail + Ads metrics pipeline",
    schedule_interval=None,  # 手動 or CI でトリガー
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["retail", "ads", "metrics"],
) as dag:

    # ---------- Retail ----------
    preprocess_retail_task = PythonOperator(
        task_id="preprocess_retail",
        python_callable=preprocess_retail,
    )

    calc_retail_metrics_task = PythonOperator(
        task_id="calc_retail_metrics",
        python_callable=calc_retail_metrics,
    )

    upload_retail_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_retail_metrics_to_gcs",
        src=dated_filename("retail_metrics", ".csv"),
        dst="metrics/retail_metrics_{{ ts_nodash }}.csv",  # ✅ 実行時刻入り
        bucket="{{ var.value.gcs_bucket }}",
    )

    load_retail_to_bq = GCSToBigQueryOperator(
        task_id="load_retail_metrics_to_bq",
        bucket="{{ var.value.gcs_bucket }}",
        source_objects=["metrics/retail_metrics_{{ ts_nodash }}.csv"],
        destination_project_dataset_table="retail.metrics",
        source_format="CSV",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    # ---------- Ads ----------
    preprocess_ads_task = PythonOperator(
        task_id="preprocess_ads",
        python_callable=preprocess_ads,
    )

    calc_ads_metrics_task = PythonOperator(
        task_id="calc_ads_metrics",
        python_callable=calc_ads_metrics,
    )

    upload_ads_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_ads_metrics_to_s3",
        filename=dated_filename("ads_metrics", ".csv"),
        dest_key="metrics/ads_metrics_{{ ts_nodash }}.csv",  # ✅ 実行時刻入り
        dest_bucket="{{ var.value.s3_bucket }}",
        replace=True,
    )

    # ---------- Task依存関係 ----------
    preprocess_retail_task >> calc_retail_metrics_task >> upload_retail_to_gcs >> load_retail_to_bq
    preprocess_ads_task >> calc_ads_metrics_task >> upload_ads_to_s3
