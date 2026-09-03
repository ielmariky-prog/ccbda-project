"""
Local forecasting prototype for Project 11_02 (Ralph - step 6).

This is NOT the production code. It is a local prototype using statsmodels
ARIMA to validate the end-to-end forecast format on Dalibor's dummy parquets,
before we port the same logic to Amazon SageMaker (Canvas or built-in DeepAR).

Goal: read Dalibor's clean parquet, train an ARIMA model, generate the next
12 predictions (= 1 hour at 5-min granularity), and emit a CSV that matches
exactly what Francesco expects (item_id, timestamp, p10, p50, p90).

This lets the rest of the pipeline (DynamoDB write, QuickSight read,
anomaly check, SNS alert) be developed in parallel with realistic numbers.

Usage:
    .venv/bin/python forecast_local_prototype.py
"""

from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA

# Configuration
PROFILE = "ralph"
REGION = "eu-west-1"

SOURCE_BUCKET = "proj1102-data"
TICKERS = ["TSLA", "MSFT", "NVDA"]

FORECAST_HORIZON = 12  # 12 steps of 5 min = 1 hour ahead
INTERVAL_ALPHA = 0.20  # 80% prediction interval -> p10 / p90

LOCAL_DIR = Path(__file__).parent
INPUT_DIR = LOCAL_DIR / "input"
OUTPUT_DIR = LOCAL_DIR / "output"
INPUT_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)


# Helpers
def download_parquet(s3, ticker: str) -> Path:
    """Download Dalibor's parquet for one ticker into ./input/."""
    key = f"forecast-ready/{ticker}.parquet"
    local = INPUT_DIR / f"{ticker}.parquet"
    s3.download_file(SOURCE_BUCKET, key, str(local))
    return local


def fit_and_forecast(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Fit ARIMA(1,1,1) on target_value and produce p10/p50/p90 forecasts.

    ARIMA(1,1,1) is the smallest sensible specification - one autoregressive
    term, first-order differencing for non-stationarity, one moving-average
    term. It trains in well under a second on 100 rows.
    """
    series = df["target_value"].astype(float).reset_index(drop=True)
    model = ARIMA(series, order=(1, 1, 1))
    fitted = model.fit()

    fc = fitted.get_forecast(steps=FORECAST_HORIZON)
    p50 = fc.predicted_mean
    ci = fc.conf_int(alpha=INTERVAL_ALPHA)  # columns: lower, upper
    p10 = ci.iloc[:, 0]
    p90 = ci.iloc[:, 1]

    # Build future timestamps continuing from the last observed one
    last_ts = pd.to_datetime(df["timestamp"].iloc[-1])
    future_ts = [last_ts + timedelta(minutes=5 * (i + 1)) for i in range(FORECAST_HORIZON)]
    future_ts_str = [ts.strftime("%Y-%m-%dT%H:%M:%SZ") for ts in future_ts]

    return pd.DataFrame(
        {
            "item_id": ticker,
            "timestamp": future_ts_str,
            "p10": p10.round(4).values,
            "p50": p50.round(4).values,
            "p90": p90.round(4).values,
        }
    )


def write_csv(predictions: pd.DataFrame, ticker: str) -> Path:
    """Write a CSV exactly as Francesco's QuickSight dashboard expects."""
    out_path = OUTPUT_DIR / f"{ticker}_predictions.csv"
    predictions.to_csv(out_path, index=False)
    return out_path


# Run
def main() -> None:
    session = boto3.Session(profile_name=PROFILE, region_name=REGION)
    s3 = session.client("s3")

    for ticker in TICKERS:
        print(f"\n=== {ticker} ===")
        local = download_parquet(s3, ticker)
        df = pd.read_parquet(local)
        print(f"  Loaded {len(df)} rows  (last price = {df.target_value.iloc[-1]:.4f})")

        preds = fit_and_forecast(df, ticker)
        out = write_csv(preds, ticker)
        print(f"  Wrote {out}")
        print(preds.to_string(index=False))


if __name__ == "__main__":
    main()
