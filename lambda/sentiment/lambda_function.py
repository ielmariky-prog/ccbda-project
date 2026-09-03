"""Sentiment Lambda (step 5). Triggered by SQS on proj1102-news-queue. For each
news headline it calls Amazon Comprehend, turns the class probabilities into a
score in [-1, +1] (P(positive) - P(negative)), and appends a row to the
per-ticker CSV in S3. Rows are grouped per ticker and written back with one
read-modify-write per ticker.

Env vars: SENTIMENT_BUCKET, SENTIMENT_PREFIX (default sentiment), APPREGION.
Keep reserved concurrency at 1 so the per-ticker CSV read-modify-write doesn't race."""

from __future__ import annotations

import csv
import io
import json
import os
from collections import defaultdict
from typing import Iterable

import boto3
from botocore.exceptions import ClientError

REGION   = os.environ.get("APPREGION", "eu-west-1")
BUCKET   = os.environ["SENTIMENT_BUCKET"]
PREFIX   = os.environ.get("SENTIMENT_PREFIX", "sentiment").rstrip("/")

CSV_COLUMNS = ["ticker", "timestamp", "sentiment_score", "confidence", "headline"]

comprehend = boto3.client("comprehend", region_name=REGION)
s3         = boto3.client("s3",         region_name=REGION)


def score_headline(headline: str) -> tuple[float, float]:
    # returns (score in [-1,1], confidence = max class probability)
    resp = comprehend.detect_sentiment(Text=headline, LanguageCode="en")
    scores = resp["SentimentScore"]
    score = float(scores["Positive"]) - float(scores["Negative"])
    confidence = max(
        scores["Positive"], scores["Negative"], scores["Neutral"], scores["Mixed"],
    )
    return round(score, 4), round(float(confidence), 4)


def _s3_key(ticker: str) -> str:
    return f"{PREFIX}/{ticker}.csv"


def _read_existing(ticker: str) -> list[dict]:
    # existing rows for the ticker, or [] if the CSV doesn't exist yet
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=_s3_key(ticker))
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            return []
        raise
    body = obj["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(body))
    return list(reader)


def _write_csv(ticker: str, rows: Iterable[dict]) -> None:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS)
    writer.writeheader()
    for row in rows:
        writer.writerow({k: row.get(k, "") for k in CSV_COLUMNS})
    s3.put_object(
        Bucket=BUCKET,
        Key=_s3_key(ticker),
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )


def append_rows(rows_by_ticker: dict[str, list[dict]]) -> None:
    # dedup by headline (the same article comes back with a fresh fetch timestamp
    # each cycle, so keying on headline stops the CSV growing without bound)
    for ticker, new_rows in rows_by_ticker.items():
        existing = _read_existing(ticker)
        merged_by_key = {r["headline"]: r for r in existing}
        for r in new_rows:
            merged_by_key[r["headline"]] = r
        merged = sorted(merged_by_key.values(), key=lambda r: r["timestamp"])
        _write_csv(ticker, merged)
        print(f"[OK] {ticker}: {len(new_rows)} new row(s), {len(merged)} total in CSV")


def lambda_handler(event, context):
    rows_by_ticker: dict[str, list[dict]] = defaultdict(list)
    batch_item_failures: list[dict] = []

    records = event.get("Records", [])
    print(f"Received batch of {len(records)} SQS record(s).")

    for rec in records:
        message_id = rec.get("messageId")
        try:
            msg = json.loads(rec["body"])
            ticker    = msg["ticker"]
            timestamp = msg["timestamp"]
            headline  = (msg.get("headline") or "").strip()

            if not headline:
                print(f"[SKIP] {message_id}: empty headline for {ticker}")
                continue

            score, confidence = score_headline(headline)
            row = {
                "ticker":          ticker,
                "timestamp":       timestamp,
                "sentiment_score": score,
                "confidence":      confidence,
                "headline":        headline,
            }
            rows_by_ticker[ticker].append(row)
            print(f"[SCORED] {ticker} @ {timestamp}: score={score:+.3f} conf={confidence:.3f}")

        except Exception as e:
            # per-record failure: ask SQS to retry just this record
            print(f"[ERROR] {message_id}: {e}")
            batch_item_failures.append({"itemIdentifier": message_id})

    if rows_by_ticker:
        try:
            append_rows(rows_by_ticker)
        except Exception as e:
            # if the S3 write fails, retry every scored record so sentiment isn't lost
            print(f"[ERROR] S3 write failed: {e}")
            scored_ids = {rec.get("messageId") for rec in records}
            failed_ids = {f["itemIdentifier"] for f in batch_item_failures}
            for mid in scored_ids - failed_ids:
                batch_item_failures.append({"itemIdentifier": mid})

    return {"batchItemFailures": batch_item_failures}
