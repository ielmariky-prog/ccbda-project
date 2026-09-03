"""
Daily forecast Lambda - proj1102 strategic horizon.

Counterpart to the hourly forecast Lambda. Once per weekday (just after
the daily ingest Lambda refreshes the parquets) this:

  1. Reads s3://proj1102-data/daily/<TICKER>.parquet for each ticker.
  2. Calls the daily DeepAR serverless endpoint with the last
     CONTEXT_LENGTH (60) closes as context.
  3. Receives 7-day p10/p50/p90 quantile predictions.
  4. Backfills `actual` and `delta` on any prior daily prediction whose
     timestamp is now covered by the fresh history.
  5. Writes new prediction rows to DynamoDB proj1102-daily-forecasts.
  6. Writes s3://proj1102-data/daily-forecast-output/<TICKER>/predictions_<RUN_TS>.csv
     for the QuickSight refresh pipeline.

Differences from the hourly forecast Lambda:
  - No `is_anomaly` / `severity` - a single off-band daily close is not
    actionable, so we don't fire SNS alerts on the daily horizon. The
    daily table is read-only context for the dashboard.
  - No `dynamic_feat` - sentiment decays in hours-to-days, the daily
    horizon is too coarse for sentiment to be a useful signal.

Env vars:
    DAILY_BUCKET           proj1102-data
    DAILY_READY_PREFIX     daily
    DAILY_OUTPUT_PREFIX    daily-forecast-output
    DDB_TABLE              proj1102-daily-forecasts
    SAGEMAKER_ENDPOINT     proj1102-deepar-daily-endpoint
    TICKERS                TSLA,MSFT,NVDA
    CONTEXT_LENGTH         60
    PREDICTION_LENGTH      7
    APPREGION              eu-west-1
"""

from __future__ import annotations

import io
import json
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key

REGION             = os.environ.get("APPREGION", "eu-west-1")
BUCKET             = os.environ["DAILY_BUCKET"]
READY_PREFIX       = os.environ.get("DAILY_READY_PREFIX",  "daily").rstrip("/")
OUTPUT_PREFIX      = os.environ.get("DAILY_OUTPUT_PREFIX", "daily-forecast-output").rstrip("/")
DDB_TABLE          = os.environ.get("DDB_TABLE", "proj1102-daily-forecasts")
ENDPOINT           = os.environ["SAGEMAKER_ENDPOINT"]
TICKERS            = os.environ.get("TICKERS", "TSLA,MSFT,NVDA").split(",")
CONTEXT_LENGTH     = int(os.environ.get("CONTEXT_LENGTH", "60"))
PREDICTION_LENGTH  = int(os.environ.get("PREDICTION_LENGTH", "7"))

s3    = boto3.client("s3", region_name=REGION)
sm_rt = boto3.client("sagemaker-runtime", region_name=REGION)
ddb   = boto3.resource("dynamodb", region_name=REGION)
table = ddb.Table(DDB_TABLE)


def _read_parquet(ticker: str) -> pd.DataFrame:
    key = f"{READY_PREFIX}/{ticker}.parquet"
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read())).sort_values("timestamp").reset_index(drop=True)


def _to_decimal(x) -> Decimal:
    return Decimal(str(round(float(x), 4)))


def _next_business_days(last_iso: str, n: int) -> list[str]:
    """`n` next weekday timestamps after `last_iso`, in the same ISO format the parquet uses."""
    dt = datetime.fromisoformat(last_iso.replace("Z", "+00:00"))
    out = []
    while len(out) < n:
        dt += timedelta(days=1)
        if dt.weekday() < 5:                  # Mon-Fri only
            out.append(dt.strftime("%Y-%m-%dT00:00:00Z"))
    return out


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def backfill_actuals(ticker: str, df: pd.DataFrame) -> int:
    """Fill `actual` + `delta` on any DDB row whose timestamp now exists in the parquet."""
    price_by_ts = dict(zip(df["timestamp"], df["target_value"]))
    items, kwargs = [], {"KeyConditionExpression": Key("ticker").eq(ticker)}
    while True:
        r = table.query(**kwargs)
        items.extend(r.get("Items", []))
        if "LastEvaluatedKey" not in r:
            break
        kwargs["ExclusiveStartKey"] = r["LastEvaluatedKey"]

    updated = 0
    for row in items:
        if row.get("actual") is not None:
            continue
        ts = row["timestamp"]
        if ts not in price_by_ts:
            continue
        actual = float(price_by_ts[ts])
        p50    = float(row["p50"])
        table.update_item(
            Key={"ticker": ticker, "timestamp": ts},
            UpdateExpression="SET actual = :a, delta = :d",
            ExpressionAttributeValues={
                ":a": _to_decimal(actual),
                ":d": _to_decimal(actual - p50),
            },
        )
        updated += 1
    return updated


def predict(ticker: str, df: pd.DataFrame) -> dict:
    """Call the daily DeepAR endpoint, return {timestamps, p10, p50, p90}."""
    context = df.tail(CONTEXT_LENGTH)
    target  = context["target_value"].astype(float).tolist()
    start   = context["timestamp"].iloc[0].replace("T", " ").replace("Z", "")
    last_ts = df["timestamp"].iloc[-1]

    payload = {
        "instances": [{"start": start, "target": target}],
        "configuration": {
            "num_samples": 100,
            "output_types": ["quantiles"],
            "quantiles": ["0.1", "0.5", "0.9"],
        },
    }
    resp = sm_rt.invoke_endpoint(
        EndpointName=ENDPOINT,
        ContentType="application/json",
        Body=json.dumps(payload).encode("utf-8"),
    )
    q = json.loads(resp["Body"].read())["predictions"][0]["quantiles"]

    timestamps = _next_business_days(last_ts, PREDICTION_LENGTH)
    return {
        "timestamps": timestamps,
        "p10": q["0.1"],
        "p50": q["0.5"],
        "p90": q["0.9"],
    }


def write_dynamodb(ticker: str, forecast: dict) -> int:
    with table.batch_writer() as batch:
        for ts, p10, p50, p90 in zip(
            forecast["timestamps"], forecast["p10"], forecast["p50"], forecast["p90"]
        ):
            batch.put_item(Item={
                "ticker":       ticker,
                "timestamp":    ts,
                "p10":          _to_decimal(p10),
                "p50":          _to_decimal(p50),
                "p90":          _to_decimal(p90),
                "actual":       None,
                "delta":        None,
                "predicted_at": _now_iso(),
            })
    return len(forecast["timestamps"])


def write_s3_csv(ticker: str, forecast: dict) -> str:
    out = pd.DataFrame({
        "item_id":   ticker,
        "timestamp": forecast["timestamps"],
        "p10":       [round(float(x), 4) for x in forecast["p10"]],
        "p50":       [round(float(x), 4) for x in forecast["p50"]],
        "p90":       [round(float(x), 4) for x in forecast["p90"]],
    })
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    key = f"{OUTPUT_PREFIX}/{ticker}/predictions_{run_ts}.csv"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=out.to_csv(index=False).encode("utf-8"),
        ContentType="text/csv",
    )
    return f"s3://{BUCKET}/{key}"


def lambda_handler(event, context):
    summary = []
    for ticker in TICKERS:
        try:
            df = _read_parquet(ticker)
            if len(df) < CONTEXT_LENGTH:
                print(f"[SKIP] {ticker}: only {len(df)} rows, need {CONTEXT_LENGTH}")
                summary.append({"ticker": ticker, "status": "skipped_short_history"})
                continue

            backfilled = backfill_actuals(ticker, df)
            forecast   = predict(ticker, df)
            n_rows     = write_dynamodb(ticker, forecast)
            csv_uri    = write_s3_csv(ticker, forecast)

            print(f"[OK] {ticker}: backfilled {backfilled}, "
                  f"wrote {n_rows} daily predictions, "
                  f"P50 {forecast['p50'][0]:.2f} -> {forecast['p50'][-1]:.2f}")
            summary.append({
                "ticker":     ticker, "status": "ok",
                "backfilled": backfilled, "predicted": n_rows, "csv": csv_uri,
            })
        except Exception as e:
            print(f"[ERROR] {ticker}: {e}")
            summary.append({"ticker": ticker, "status": "error", "message": str(e)})

    return {"statusCode": 200, "body": json.dumps(summary)}
