import pytest
import os
import pandas as pd

OUTPUT_DIR = "output"

@pytest.mark.skipif(not os.path.exists(f"{OUTPUT_DIR}/retail_metrics.csv"), reason="retail_metrics.csv not found")
def test_retail_metrics_exists():
    df = pd.read_csv(f"{OUTPUT_DIR}/retail_metrics.csv")
    assert "stockout_rate" in df.columns

@pytest.mark.skipif(not os.path.exists(f"{OUTPUT_DIR}/ads_metrics.csv"), reason="ads_metrics.csv not found")
def test_ads_metrics_exists():
    df = pd.read_csv(f"{OUTPUT_DIR}/ads_metrics.csv")
    assert "avg_CTR" in df.columns
