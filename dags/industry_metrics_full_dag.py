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
            df.to_csv(dated_filename("retail_clean_backup", ".csv", ds), index=False
