"""Forecast Lambda (step 6). For each ticker: backfill the actuals on old
prediction rows, call the DeepAR endpoint for the next hour, and write the new
predictions to DynamoDB and to a CSV in S3."""

from __future__ import annotations

import io
import json
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key

REGION                 = os.environ.get("APPREGION", "eu-west-1")
BUCKET                 = os.environ["FORECAST_BUCKET"]
READY_PREFIX           = os.environ.get("FORECAST_READY_PREFIX", "forecast-ready").rstrip("/")
OUTPUT_PREFIX          = os.environ.get("FORECAST_OUTPUT_PREFIX", "forecast-output").rstrip("/")
DDB_TABLE              = os.environ.get("DDB_TABLE", "proj1102-forecasts")
ENDPOINT               = os.environ["SAGEMAKER_ENDPOINT"]
TICKERS                = os.environ.get("TICKERS", "TSLA,MSFT,NVDA").split(",")
CONTEXT_LENGTH         = int(os.environ.get("CONTEXT_LENGTH", "120"))
PREDICTION_LENGTH      = int(os.environ.get("PREDICTION_LENGTH", "12"))
FREQ_MINUTES           = 5

s3        = boto3.client("s3", region_name=REGION)
sm_rt     = boto3.client("sagemaker-runtime", region_name=REGION)
ddb       = boto3.resource("dynamodb", region_name=REGION)
table     = ddb.Table(DDB_TABLE)


def _read_ready_parquet(ticker: str) -> pd.DataFrame:
    key = f"{READY_PREFIX}/{ticker}.parquet"
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def _to_decimal(x) -> Decimal:
    # DynamoDB needs Decimal, not float
    return Decimal(str(round(float(x), 4)))


def _add_minutes(iso_ts: str, minutes: int) -> str:
    dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00").replace(" ", "T"))
    return (dt + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _severity(actual: float, p10: float, p90: float) -> str:
    # how far actual is outside the p10-p90 band
    if p10 <= actual <= p90:
        return "low"
    band = max(p90 - p10, 1e-9)
    excess = (p10 - actual) if actual < p10 else (actual - p90)
    return "medium" if excess < band else "high"


def _is_market_open(ts_iso: str) -> bool:
    # US trading hours 13:30-20:00 UTC, Mon-Fri. Outside this prices are frozen
    # at the last trade, so we don't flag anomalies (avoids weekend alert spam).
    dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
    if dt.weekday() >= 5:
        return False
    minute_of_day = dt.hour * 60 + dt.minute
    return 13 * 60 + 30 <= minute_of_day < 20 * 60


def backfill_actuals(ticker: str, df: pd.DataFrame) -> int:
    # fill actual / is_anomaly / delta on rows whose timestamp is now in the parquet
    price_by_ts = dict(zip(df["timestamp"], df["target_value"]))

    items, kwargs = [], {"KeyConditionExpression": Key("ticker").eq(ticker)}
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    updated = 0
    for row in items:
        if row.get("actual") is not None:
            continue
        ts = row["timestamp"]
        if ts not in price_by_ts:
            continue
        actual = float(price_by_ts[ts])
        p10, p90, p50 = float(row["p10"]), float(row["p90"]), float(row["p50"])
        delta = actual - p50
        if _is_market_open(ts):
            is_anomaly = actual < p10 or actual > p90
            severity   = _severity(actual, p10, p90)
        else:
            is_anomaly = False
            severity   = "low"
        table.update_item(
            Key={"ticker": ticker, "timestamp": ts},
            UpdateExpression="SET actual = :a, is_anomaly = :an, delta = :d, severity = :s",
            ExpressionAttributeValues={
                ":a":  _to_decimal(actual),
                ":an": is_anomaly,
                ":d":  _to_decimal(delta),
                ":s":  severity,
            },
        )
        updated += 1
    return updated


def predict(ticker: str, df: pd.DataFrame) -> dict:
    # call DeepAR with the last CONTEXT_LENGTH points + sentiment as dynamic_feat
    context = df.tail(CONTEXT_LENGTH)
    target = context["target_value"].astype(float).tolist()
    start = context["timestamp"].iloc[0].replace("T", " ").replace("Z", "")
    last_ts = df["timestamp"].iloc[-1]

    # dynamic_feat needs values over context + horizon; carry last sentiment forward
    context_sent = context["sentiment_score"].ffill().fillna(0).astype(float).tolist()
    last_sent    = context_sent[-1] if context_sent else 0.0
    dyn_feat     = context_sent + [last_sent] * PREDICTION_LENGTH

    payload = {
        "instances": [{
            "start":        start,
            "target":       target,
            "dynamic_feat": [dyn_feat],
        }],
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
    q = json.loads(resp["Body"].read().decode("utf-8"))["predictions"][0]["quantiles"]

    timestamps = [_add_minutes(last_ts, FREQ_MINUTES * (i + 1))
                  for i in range(PREDICTION_LENGTH)]
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
                "ticker":     ticker,
                "timestamp":  ts,
                "p10":        _to_decimal(p10),
                "p50":        _to_decimal(p50),
                "p90":        _to_decimal(p90),
                "actual":     None,
                "is_anomaly": None,
                "delta":      None,
                "severity":   None,
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
    # unique file per run so the QuickSight refresh can concat them all
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
            df = _read_ready_parquet(ticker)
            if len(df) < CONTEXT_LENGTH:
                print(f"[SKIP] {ticker}: only {len(df)} rows, need {CONTEXT_LENGTH}")
                summary.append({"ticker": ticker, "status": "skipped_short_history"})
                continue

            backfilled = backfill_actuals(ticker, df)
            forecast   = predict(ticker, df)
            n_rows     = write_dynamodb(ticker, forecast)
            csv_uri    = write_s3_csv(ticker, forecast)

            print(f"[OK] {ticker}: backfilled {backfilled}, "
                  f"wrote {n_rows} predictions, "
                  f"P50 {forecast['p50'][0]:.2f} -> {forecast['p50'][-1]:.2f}")
            summary.append({
                "ticker":     ticker,
                "status":     "ok",
                "backfilled": backfilled,
                "predicted":  n_rows,
                "csv":        csv_uri,
            })
        except Exception as e:
            print(f"[ERROR] {ticker}: {e}")
            summary.append({"ticker": ticker, "status": "error", "message": str(e)})

    return {"statusCode": 200, "body": json.dumps(summary)}
