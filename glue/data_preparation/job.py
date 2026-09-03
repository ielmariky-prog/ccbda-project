"""
Glue Python Shell job - proj1102 data-preparation pipeline.

Triggered by: lambda/glue_trigger/lambda_function.py
Reads from:
    SQS proj1102-prices-queue              -> price rows (target time series)
    S3  proj1102-data/sentiment/<TICKER>.csv -> sentiment, written by the
                                              Comprehend Lambda as rows arrive

S3 layout (one incremental file per Glue run, never rewrite history):
    s3://proj1102-data/forecast-ready/<TICKER>/<RUN_TS>.parquet

Each run writes only NEW rows for this interval:
    gap-fill rows from (last known timestamp + 5 min) up to first new price
    + deduplicated, rounded price rows from SQS

To find the anchor the job lists processed/<TICKER>/, takes the lexicographically
last key (ISO-8601 filenames sort chronologically), and reads only that one file.

Glue job default arguments (prefix with -- in the job definition):
    output_bucket      proj1102-data
    prices_queue_url   https://sqs.eu-west-1.amazonaws.com/834922934600/proj1102-prices-queue
    forecast_ready_prefix   forecast-ready
    sentiment_prefix   sentiment
    max_sqs_messages   500
    interval_minutes   5

Required additional Python modules (--additional-python-modules):
    pandas, pyarrow
"""

from __future__ import annotations

import io
import sys
import json
import boto3
import pandas as pd
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from botocore.exceptions import ClientError

try:
    from awsglue.utils import getResolvedOptions
    args = getResolvedOptions(sys.argv, [
        "output_bucket",
        "prices_queue_url",
        "forecast_ready_prefix",
        "sentiment_prefix",
        "max_sqs_messages",
        "interval_minutes",
    ])
except (ImportError, SystemExit):
    args = {
        "output_bucket":        "proj1102-data",
        "prices_queue_url":     "https://sqs.eu-west-1.amazonaws.com/834922934600/proj1102-prices-queue",
        "forecast_ready_prefix": "forecast-ready",
        "sentiment_prefix":     "sentiment",
        "max_sqs_messages":     "500",
        "interval_minutes":     "5",
    }

BUCKET                = args["output_bucket"]
PRICES_QUEUE_URL      = args["prices_queue_url"]
FORECAST_READY_PREFIX = args["forecast_ready_prefix"]
SENTIMENT_PREFIX      = args["sentiment_prefix"]
MAX_MSGS         = int(args["max_sqs_messages"])
INTERVAL_MIN     = int(args["interval_minutes"])

FORECAST_COLUMNS = ["item_id", "timestamp", "target_value", "sentiment_score"]

s3  = boto3.client("s3",  region_name="eu-west-1")
sqs = boto3.client("sqs", region_name="eu-west-1")


# SQS helpers

def drain_queue(queue_url: str, max_messages: int) -> list:
    messages = []
    while len(messages) < max_messages:
        resp  = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=min(10, max_messages - len(messages)),
            WaitTimeSeconds=1,
        )
        batch = resp.get("Messages", [])
        if not batch:
            break
        messages.extend(batch)
    return messages


def delete_messages(queue_url: str, messages: list) -> None:
    for i in range(0, len(messages), 10):
        batch   = messages[i : i + 10]
        entries = [
            {"Id": str(j), "ReceiptHandle": m["ReceiptHandle"]}
            for j, m in enumerate(batch)
        ]
        sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)


# Timestamp helpers

def to_utc(ts_str: str) -> datetime:
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(timezone.utc)


def round_to_interval(dt: datetime, minutes: int) -> datetime:
    total_min = dt.hour * 60 + dt.minute + dt.second / 60
    rounded   = round(total_min / minutes) * minutes
    h, m      = divmod(int(rounded), 60)
    if h == 24:
        h, m = 23, 60 - minutes
    return dt.replace(hour=h, minute=m, second=0, microsecond=0, tzinfo=timezone.utc)


def fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# Message parsing

def parse_price_messages(messages: list) -> tuple:
    """Returns (parsed_rows, ok_messages, failed_messages)."""
    parsed, ok_msgs, failed = [], [], []
    for msg in messages:
        try:
            body    = json.loads(msg["Body"])
            payload = body.get("payload", body)
            parsed.append({
                "item_id":      str(payload["ticker"]),
                "timestamp":    str(payload["timestamp"]),
                "target_value": float(payload["price"]),
            })
            ok_msgs.append(msg)
        except Exception as exc:
            print(f"  WARN: bad price message {msg.get('MessageId')}: {exc} - will retry")
            failed.append(msg)
    return parsed, ok_msgs, failed


# S3 helpers

def load_existing_parquet(ticker: str) -> pd.DataFrame | None:
    """Load forecast_ready_prefix/{ticker}.parquet from S3, or return None if it doesn't exist yet."""
    key = f"{FORECAST_READY_PREFIX}/{ticker}.parquet"
    try:
        resp = s3.get_object(Bucket=BUCKET, Key=key)
        df   = pd.read_parquet(io.BytesIO(resp["Body"].read()))
        print(f"  [{ticker}] loaded {len(df)} existing rows from s3://{BUCKET}/{key}")
        return df
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise


def load_sentiment_csv(ticker: str) -> pd.DataFrame | None:
    """Load Ralph's sentiment CSV and return [timestamp, sentiment_score], or None."""
    key = f"{SENTIMENT_PREFIX}/{ticker}.csv"
    try:
        resp = s3.get_object(Bucket=BUCKET, Key=key)
        df   = pd.read_csv(io.BytesIO(resp["Body"].read()))
        if "ticker" in df.columns and "item_id" not in df.columns:
            df = df.rename(columns={"ticker": "item_id"})
        df["timestamp"]       = df["timestamp"].apply(
            lambda t: fmt_ts(round_to_interval(to_utc(str(t)), INTERVAL_MIN))
        )
        df["sentiment_score"] = pd.to_numeric(df["sentiment_score"], errors="coerce")
        print(f"  [{ticker}] loaded {len(df)} sentiment rows")
        return df[["timestamp", "sentiment_score"]].drop_duplicates("timestamp")
    except Exception:
        return None


def write_parquet(ticker: str, df: pd.DataFrame) -> None:
    key = f"{FORECAST_READY_PREFIX}/{ticker}.parquet"
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    buf.seek(0)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.read())
    print(f"  [{ticker}] wrote {len(df)} rows -> s3://{BUCKET}/{key}")


# Core per-ticker pipeline

def process_ticker(ticker: str, new_rows: list) -> None:
    """
    1. Load the full existing processed/{ticker}.parquet (history + anchor).
    2. Round and deduplicate all new SQS rows for this ticker.
    3. Build a complete 5-minute grid from (anchor+interval) to last SQS timestamp.
       The anchor price seeds the forward-fill so gaps before the first SQS row
       AND gaps between SQS rows are all filled with the last known price.
    4. Concat existing history + new portion, join sentiment, overwrite the file.
    """
    existing = load_existing_parquet(ticker)

    if existing is not None and not existing.empty:
        last_row   = existing.tail(1).iloc[0]
        last_ts    = str(last_row["timestamp"])
        last_price = float(last_row["target_value"])
        anchor_dt  = to_utc(last_ts)
        print(f"  [{ticker}] anchor: last_ts={last_ts}  price={last_price}")
    else:
        last_ts = last_price = anchor_dt = None

    # Round + sort + dedup all new SQS rows for this ticker
    df_new = pd.DataFrame(new_rows, columns=["item_id", "timestamp", "target_value"])
    df_new["timestamp"] = df_new["timestamp"].apply(
        lambda t: fmt_ts(round_to_interval(to_utc(t), INTERVAL_MIN))
    )
    df_new = (
        df_new
        .sort_values("timestamp")
        .drop_duplicates(subset=["item_id", "timestamp"], keep="last")
        .reset_index(drop=True)
    )

    last_new_dt = to_utc(df_new["timestamp"].iloc[-1])
    grid_start  = (anchor_dt + timedelta(minutes=INTERVAL_MIN)) if anchor_dt else to_utc(df_new["timestamp"].iloc[0])

    if grid_start > last_new_dt:
        print(f"  [{ticker}] all SQS rows already covered by existing data - skipping")
        return

    # Build complete 5-min grid over the new range
    full_index = pd.date_range(
        start=grid_start, end=last_new_dt, freq=f"{INTERVAL_MIN}min", tz="UTC"
    )
    grid_df = pd.DataFrame({
        "_dt":       full_index,
        "timestamp": full_index.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "item_id":   ticker,
    })

    new_portion = grid_df.merge(df_new[["timestamp", "target_value"]], on="timestamp", how="left")
    new_portion["sentiment_score"] = None

    # Prepend anchor row as ffill seed, then drop it after filling
    if anchor_dt is not None:
        seed = pd.DataFrame([{
            "_dt": anchor_dt, "timestamp": last_ts,
            "item_id": ticker, "target_value": last_price, "sentiment_score": None,
        }])
        new_portion = pd.concat([seed, new_portion], ignore_index=True).sort_values("_dt")

    new_portion["target_value"] = new_portion["target_value"].ffill()

    if anchor_dt is not None:
        new_portion = new_portion.iloc[1:]  # drop seed - already in existing history

    new_portion = new_portion.drop(columns=["_dt"]).reset_index(drop=True)

    if new_portion["target_value"].isna().any():
        new_portion["target_value"] = new_portion["target_value"].bfill()

    n_gaps = len(new_portion) - df_new["timestamp"].isin(new_portion["timestamp"]).sum()
    print(f"  [{ticker}] new portion: {len(new_portion)} rows ({n_gaps} gap-filled)")

    # Join sentiment as-of with a freshness tolerance
    # News is sparse so an exact-timestamp join leaves almost every row null;
    # a backward as-of carries the last known score forward. But without a
    # tolerance the sentiment from yesterday morning still influences today's
    # forecasts. `tolerance` caps that - a price row picks up the most recent
    # sentiment only if the headline is within SENTIMENT_TOLERANCE_HOURS;
    # otherwise sentiment_score stays NaN.
    SENTIMENT_TOLERANCE_HOURS = 4

    sentiment_df = load_sentiment_csv(ticker)
    if sentiment_df is not None:
        new_portion = new_portion.drop(columns=["sentiment_score"], errors="ignore")
        sent = (
            sentiment_df
            .assign(_ts=pd.to_datetime(sentiment_df["timestamp"], utc=True))
            .dropna(subset=["sentiment_score"])
            .sort_values("_ts")
        )
        if sent.empty:
            new_portion["sentiment_score"] = None
        else:
            new_portion = (
                pd.merge_asof(
                    new_portion.assign(_ts=pd.to_datetime(new_portion["timestamp"], utc=True))
                               .sort_values("_ts"),
                    sent[["_ts", "sentiment_score"]],
                    on="_ts",
                    direction="backward",
                    tolerance=pd.Timedelta(hours=SENTIMENT_TOLERANCE_HOURS),
                )
                .drop(columns="_ts")
                .sort_values("timestamp")
                .reset_index(drop=True)
            )

    # Concat existing history + new portion and write back
    if existing is not None and not existing.empty:
        if "sentiment_score" not in existing.columns:
            existing["sentiment_score"] = None
        result = pd.concat([existing[FORECAST_COLUMNS], new_portion[FORECAST_COLUMNS]], ignore_index=True)
    else:
        result = new_portion[FORECAST_COLUMNS].copy()

    result = (
        result
        .drop_duplicates(subset=["item_id", "timestamp"], keep="last")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    assert result.duplicated(subset=["item_id", "timestamp"]).sum() == 0, \
        f"[{ticker}] BUG: duplicate (item_id, timestamp) after pipeline"

    write_parquet(ticker, result)


# Main

def main() -> None:
    # Drain prices queue
    print("Draining prices queue ...")
    price_msgs = drain_queue(PRICES_QUEUE_URL, MAX_MSGS)
    print(f"  Received {len(price_msgs)} price messages")

    parsed_rows, ok_msgs, _ = parse_price_messages(price_msgs)
    if ok_msgs:
        delete_messages(PRICES_QUEUE_URL, ok_msgs)
    print(f"  Parsed {len(parsed_rows)} rows, {len(price_msgs) - len(ok_msgs)} failed (will retry)")

    by_ticker: dict = defaultdict(list)
    for row in parsed_rows:
        by_ticker[row["item_id"]].append(row)

    if not by_ticker:
        print("\nNo new price messages - nothing to process.")
        return

    print(f"\nProcessing {len(by_ticker)} ticker(s): {sorted(by_ticker.keys())}")
    for ticker, rows in sorted(by_ticker.items()):
        print(f"\n--- {ticker} ({len(rows)} new SQS rows) ---")
        try:
            process_ticker(ticker, rows)
        except Exception as exc:
            print(f"  [{ticker}] ERROR: {exc}")

    print("\nDone.")


# Glue runs this as __main__, so main() still runs in prod; the guard lets tests import the helpers.
if __name__ == "__main__":
    main()
