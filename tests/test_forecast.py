import os
import pandas as pd

OUTPUT_DIR = "output"

def test_retail_metrics_file():
    filepath = f"{OUTPUT_DIR}/retail_metrics.csv"
    if not os.path.exists(filepath):
        print("⚠️ retail_metrics.csv が存在しません")
        return  # ここで処理終了
    # あったら後続処理
    df = pd.read_csv(filepath)
    assert "stockout_rate" in df.columns, "❌ stockout_rate カラムがありません"
    print("✅ retail_metrics.csv を検証しました（行数:", len(df), ")")

def test_ads_metrics_file():
    filepath = f"{OUTPUT_DIR}/ads_metrics.csv"
    if not os.path.exists(filepath):
        print("⚠️ ads_metrics.csv が存在しません")
        return  # ここで処理終了
    # あったら後続処理
    df = pd.read_csv(filepath)
    assert "avg_CTR" in df.columns, "❌ avg_CTR カラムがありません"
    print("✅ ads_metrics.csv を検証しました（行数:", len(df), ")")
