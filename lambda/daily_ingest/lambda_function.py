"""
Daily ingestion Lambda - proj1102 strategic horizon.

Fetches 5 years of daily closing prices for TSLA/MSFT/NVDA from Yahoo
Finance (v8 chart API, no auth required) and overwrites the per-ticker
parquet under s3://proj1102-data/daily/<TICKER>.parquet.

Runs once per weekday after US market close (20:30 UTC), so the next
forecast Lambda invocation has a fresh dataset including the day's close.

Re-fetching the full 5y window every day (instead of appending) is
deliberately simple and idempotent - Yahoo's API returns the canonical
adjusted series, so overwriting catches any restatements automatically.
The payload is small (~28 KB per ticker) so the bandwidth doesn't matter.

Env vars:
    DATA_BUCKET   (default proj1102-data)
    TICKERS       (default TSLA,MSFT,NVDA)
    APPREGION     (default eu-west-1)
"""

from __future__ import annotations

import io
import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3
import pandas as pd

REGION       = os.environ.get("APPREGION",  "eu-west-1")
DATA_BUCKET  = os.environ.get("DATA_BUCKET", "proj1102-data")
TICKERS      = os.environ.get("TICKERS", "TSLA,MSFT,NVDA").split(",")
DAILY_PREFIX = "daily"
LOOKBACK     = "5y"

s3 = boto3.client("s3", region_name=REGION)


def _fetch_daily(symbol: str) -> pd.DataFrame:
    """Yahoo Finance v8 chart endpoint - JSON, no auth, public."""
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        f"?range={LOOKBACK}&interval=1d"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "proj1102-daily-ingest/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())

    result   = data["chart"]["result"][0]
    times    = result["timestamp"]
    closes   = result["indicators"]["quote"][0]["close"]
    volumes  = result["indicators"]["quote"][0].get("volume", [0] * len(times))

    rows = []
    for t, c, v in zip(times, closes, volumes):
        if c is None:
            continue                                  # market closed for that bar
        ts = datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%dT00:00:00Z")
        rows.append({
            "item_id":      symbol,
            "timestamp":    ts,
            "target_value": float(c),
            "volume":       float(v or 0),
        })
    return pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)


def lambda_handler(event, context):
    summary = {}
    for t in TICKERS:
        try:
            df = _fetch_daily(t)
            buf = io.BytesIO()
            df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
            buf.seek(0)
            s3.put_object(
                Bucket=DATA_BUCKET,
                Key=f"{DAILY_PREFIX}/{t}.parquet",
                Body=buf.read(),
            )
            print(f"[OK] {t}: {len(df)} daily rows  last={df['timestamp'].iloc[-1]}")
            summary[t] = {"rows": len(df), "last_ts": df["timestamp"].iloc[-1]}
        except Exception as e:
            print(f"[ERROR] {t}: {e}")
            summary[t] = {"error": str(e)}
    return {"statusCode": 200, "summary": summary}
