import pandas as pd
import os

OUTPUT_DIR = "output"

def test_forecast_file_exists():
    assert os.path.exists(f"{OUTPUT_DIR}/retail_metrics.csv")
    assert os.path.exists(f"{OUTPUT_DIR}/ads_metrics.csv")

def test_forecast_format_retail():
    df = pd.read_csv(f"{OUTPUT_DIR}/retail_metrics.csv")
    assert "stockout_rate" in df.columns

def test_forecast_format_ads():
    df = pd.read_csv(f"{OUTPUT_DIR}/ads_metrics.csv")
    assert "avg_CTR" in df.columns
