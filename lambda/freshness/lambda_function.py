"""
Parquet freshness check - proj1102 (Ralph).

Hourly heartbeat that the forecast-ready parquets aren't going stale. For each
ticker it reads the newest `timestamp` from the parquet, computes age in
minutes vs. now(), and publishes the MAX age across tickers as a custom
CloudWatch metric `proj1102/oldest_parquet_age_minutes`.

A CloudWatch alarm on that metric > 120 publishes to proj1102-infra-alarms,
so we get exactly ONE alarm email per stale->fresh transition instead of one
per check while ingestion is down. Per-ticker ages are logged so the alarm
email plus a CloudWatch Logs glance tells you which ticker is the culprit.

Env vars (all optional with sensible defaults):
    DATA_BUCKET     proj1102-data
    READY_PREFIX    forecast-ready
    TICKERS         TSLA,MSFT,NVDA
    APPREGION       eu-west-1
"""

from __future__ import annotations

import io
import os
from datetime import datetime, timezone

import boto3
import pandas as pd

REGION        = os.environ.get("APPREGION",   "eu-west-1")
DATA_BUCKET   = os.environ.get("DATA_BUCKET",  "proj1102-data")
READY_PREFIX  = os.environ.get("READY_PREFIX", "forecast-ready")
DAILY_PREFIX  = os.environ.get("DAILY_PREFIX", "daily")
TICKERS       = os.environ.get("TICKERS",      "TSLA,MSFT,NVDA").split(",")

NAMESPACE          = "proj1102"
METRIC_NAME        = "oldest_parquet_age_minutes"
METRIC_DAILY_NAME  = "oldest_daily_parquet_age_minutes"

s3 = boto3.client("s3",         region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def _age_minutes(prefix: str, ticker: str) -> float | None:
    """Age in minutes of the newest row in <prefix>/<ticker>.parquet, or None on error."""
    try:
        obj = s3.get_object(Bucket=DATA_BUCKET, Key=f"{prefix}/{ticker}.parquet")
        df  = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        last_ts_str = df["timestamp"].max()
        last_ts = datetime.fromisoformat(last_ts_str.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - last_ts).total_seconds() / 60.0
    except Exception as e:
        print(f"[ERROR] {prefix}/{ticker}: {e}")
        return None


def _publish_max_age(prefix: str, metric_name: str, label: str) -> dict[str, float]:
    ages: dict[str, float] = {}
    for t in TICKERS:
        a = _age_minutes(prefix, t)
        if a is not None:
            ages[t] = a
            print(f"  [{label}] {t}: last row {a:.1f} min ago")
        else:
            print(f"  [{label}] {t}: unreachable - counted as stale")
    max_age = max(ages.values()) if ages else 9999.0
    cw.put_metric_data(
        Namespace=NAMESPACE,
        MetricData=[{
            "MetricName": metric_name,
            "Value":      max_age,
            "Unit":       "None",
            "Timestamp":  datetime.now(timezone.utc),
        }],
    )
    print(f"  [{label}] published {metric_name} = {max_age:.1f}")
    return ages


def lambda_handler(event, context):
    print("=== hourly parquets (forecast-ready/) ===")
    ages_hourly = _publish_max_age(READY_PREFIX, METRIC_NAME, "hourly")

    print("=== daily parquets (daily/) ===")
    ages_daily  = _publish_max_age(DAILY_PREFIX, METRIC_DAILY_NAME, "daily")

    return {
        "statusCode": 200,
        "hourly_ages_minutes": ages_hourly,
        "daily_ages_minutes":  ages_daily,
        "hourly_max_age":      max(ages_hourly.values()) if ages_hourly else None,
        "daily_max_age":       max(ages_daily.values())  if ages_daily  else None,
    }
