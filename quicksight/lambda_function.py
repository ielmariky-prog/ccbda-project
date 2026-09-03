"""QuickSight refresh Lambda. QuickSight + Athena live in Ralph's account, so on
a schedule this pulls the live pipeline data from the team bucket and overwrites
the CSVs that back the Athena tables (actuals + forecast, for both the hourly and
the daily horizon). The Athena tables are EXTERNAL and point at fixed prefixes,
so overwriting the CSV is enough for the dashboard to see fresh data.

Env vars: SRC_BUCKET (team bucket), DST_BUCKET (Ralph's bucket), TICKERS, APPREGION."""

from __future__ import annotations

import io
import os

import boto3
import pandas as pd

REGION        = os.environ.get("APPREGION", "eu-west-1")
SRC_BUCKET    = os.environ.get("SRC_BUCKET", "proj1102-data")
DST_BUCKET    = os.environ.get("DST_BUCKET", "proj1102-ralph-forecast-148557232117")
TICKERS       = os.environ.get("TICKERS", "TSLA,MSFT,NVDA").split(",")

READY_PREFIX  = "forecast-ready"
OUTPUT_PREFIX = "forecast-output"
ACTUALS_KEY   = "quicksight/actuals/actuals.csv"
FORECAST_KEY  = "quicksight/forecast/forecast.csv"

# daily horizon (7-day forecasts) uses a separate model and separate tables
DAILY_READY_PREFIX  = "daily"
DAILY_OUTPUT_PREFIX = "daily-forecast-output"
DAILY_ACTUALS_KEY   = "quicksight/daily-actuals/daily-actuals.csv"
DAILY_FORECAST_KEY  = "quicksight/daily-forecast/daily-forecast.csv"

s3 = boto3.client("s3", region_name=REGION)


def _build_actuals() -> pd.DataFrame:
    # real prices from the live forecast-ready parquets; skip a ticker if its read
    # fails so one bad file doesn't break the whole refresh
    frames = []
    for t in TICKERS:
        try:
            obj = s3.get_object(Bucket=SRC_BUCKET, Key=f"{READY_PREFIX}/{t}.parquet")
            df  = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            frames.append(df[["item_id", "timestamp", "target_value", "sentiment_score"]])
        except Exception as e:
            print(f"[WARN] actuals skip {t}: {e}")
    if not frames:
        return pd.DataFrame(columns=["item_id", "timestamp", "target_value", "sentiment_score"])
    return pd.concat(frames, ignore_index=True).sort_values(["item_id", "timestamp"])


def _build_forecast() -> pd.DataFrame:
    # all prediction CSVs the forecast Lambda has written, per ticker
    frames = []
    for t in TICKERS:
        try:
            resp = s3.list_objects_v2(Bucket=SRC_BUCKET, Prefix=f"{OUTPUT_PREFIX}/{t}/")
            for obj in resp.get("Contents", []):
                if not obj["Key"].endswith(".csv"):
                    continue
                body = s3.get_object(Bucket=SRC_BUCKET, Key=obj["Key"])["Body"].read()
                df   = pd.read_csv(io.BytesIO(body)).rename(columns={"ticker": "item_id"})
                frames.append(df[["item_id", "timestamp", "p10", "p50", "p90"]])
        except Exception as e:
            print(f"[WARN] forecast skip {t}: {e}")
    if not frames:
        return pd.DataFrame(columns=["item_id", "timestamp", "p10", "p50", "p90"])
    # file names sort chronologically, so keep="last" lets the newest run win
    out = pd.concat(frames, ignore_index=True)
    return (
        out.drop_duplicates(["item_id", "timestamp"], keep="last")
           .sort_values(["item_id", "timestamp"])
    )


def _build_daily_actuals() -> pd.DataFrame:
    # daily real closes, one parquet per ticker
    frames = []
    for t in TICKERS:
        try:
            obj = s3.get_object(Bucket=SRC_BUCKET, Key=f"{DAILY_READY_PREFIX}/{t}.parquet")
            df  = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            frames.append(df[["item_id", "timestamp", "target_value"]])
        except Exception as e:
            print(f"[WARN] daily-actuals skip {t}: {e}")
    if not frames:
        return pd.DataFrame(columns=["item_id", "timestamp", "target_value"])
    return pd.concat(frames, ignore_index=True).sort_values(["item_id", "timestamp"])


def _build_daily_forecast() -> pd.DataFrame:
    # all daily 7-day forecast CSVs
    frames = []
    for t in TICKERS:
        try:
            resp = s3.list_objects_v2(Bucket=SRC_BUCKET, Prefix=f"{DAILY_OUTPUT_PREFIX}/{t}/")
            for obj in resp.get("Contents", []):
                if not obj["Key"].endswith(".csv"):
                    continue
                body = s3.get_object(Bucket=SRC_BUCKET, Key=obj["Key"])["Body"].read()
                df   = pd.read_csv(io.BytesIO(body)).rename(columns={"ticker": "item_id"})
                frames.append(df[["item_id", "timestamp", "p10", "p50", "p90"]])
        except Exception as e:
            print(f"[WARN] daily-forecast skip {t}: {e}")
    if not frames:
        return pd.DataFrame(columns=["item_id", "timestamp", "p10", "p50", "p90"])
    out = pd.concat(frames, ignore_index=True)
    return (
        out.drop_duplicates(["item_id", "timestamp"], keep="last")
           .sort_values(["item_id", "timestamp"])
    )


def _put_csv(df: pd.DataFrame, key: str) -> None:
    s3.put_object(
        Bucket=DST_BUCKET,
        Key=key,
        Body=df.to_csv(index=False).encode("utf-8"),
        ContentType="text/csv",
    )


def lambda_handler(event, context):
    # hourly horizon
    actuals = _build_actuals()
    _put_csv(actuals, ACTUALS_KEY)
    print(f"[OK] actuals  -> s3://{DST_BUCKET}/{ACTUALS_KEY}  ({len(actuals)} rows)")

    forecast = _build_forecast()
    _put_csv(forecast, FORECAST_KEY)
    print(f"[OK] forecast -> s3://{DST_BUCKET}/{FORECAST_KEY}  ({len(forecast)} rows)")

    # daily horizon
    daily_actuals = _build_daily_actuals()
    _put_csv(daily_actuals, DAILY_ACTUALS_KEY)
    print(f"[OK] daily-actuals  -> s3://{DST_BUCKET}/{DAILY_ACTUALS_KEY}  ({len(daily_actuals)} rows)")

    daily_forecast = _build_daily_forecast()
    _put_csv(daily_forecast, DAILY_FORECAST_KEY)
    print(f"[OK] daily-forecast -> s3://{DST_BUCKET}/{DAILY_FORECAST_KEY}  ({len(daily_forecast)} rows)")

    return {
        "statusCode": 200,
        "actuals_rows": len(actuals),
        "forecast_rows": len(forecast),
        "daily_actuals_rows": len(daily_actuals),
        "daily_forecast_rows": len(daily_forecast),
    }
