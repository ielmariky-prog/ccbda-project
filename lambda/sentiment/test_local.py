"""
Local smoke test for lambda_function.py.

Runs the Lambda handler with a synthetic SQS event containing three news
headlines (one per ticker). Calls Comprehend for real (requires the `ralph`
AWS profile to be configured), but skips the S3 write unless SENTIMENT_BUCKET
is set in the environment.

Usage:
    cd lambda/sentiment
    python3 test_local.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Make the Lambda module importable when run directly
sys.path.insert(0, str(Path(__file__).resolve().parent))

# Default to a local-only run: no S3 writes. Override SENTIMENT_BUCKET to do a
# real write.
os.environ.setdefault("AWS_PROFILE",        "ralph")
os.environ.setdefault("AWS_DEFAULT_REGION", "eu-west-1")
os.environ.setdefault("APPREGION",          "eu-west-1")
os.environ.setdefault("SENTIMENT_BUCKET",   "__local__")  # sentinel, see below

import lambda_function as fn  # noqa: E402

# Monkey-patch the S3 write so the test never hits S3 unless explicitly opted in
if os.environ["SENTIMENT_BUCKET"] == "__local__":
    print("(DRY RUN: skipping S3 read/write - set SENTIMENT_BUCKET to enable)\n")
    fn._read_existing = lambda ticker: []
    fn._write_csv     = lambda ticker, rows: print(
        f"  [DRY RUN] would write {len(list(rows))} rows to s3://.../{ticker}.csv"
    )


SAMPLE_EVENT = {
    "Records": [
        {
            "messageId": "msg-1",
            "body": json.dumps({
                "ticker":    "TSLA",
                "timestamp": "2026-05-11T10:30:00Z",
                "headline":  "Tesla beats delivery expectations, stock jumps 8%",
                "url":       "https://example.com/tsla-1",
                "source":    "alpha_vantage",
            }),
        },
        {
            "messageId": "msg-2",
            "body": json.dumps({
                "ticker":    "BTC-USD",
                "timestamp": "2026-05-11T10:30:00Z",
                "headline":  "Major exchange hacked, Bitcoin price plunges 15%",
                "url":       "https://example.com/btc-1",
                "source":    "alpha_vantage",
            }),
        },
        {
            "messageId": "msg-3",
            "body": json.dumps({
                "ticker":    "ETH-USD",
                "timestamp": "2026-05-11T10:30:00Z",
                "headline":  "Ethereum upgrade successfully reduces transaction fees",
                "url":       "https://example.com/eth-1",
                "source":    "alpha_vantage",
            }),
        },
    ]
}


def main() -> int:
    print(f"Region:  {os.environ['APPREGION']}")
    print(f"Bucket:  {os.environ['SENTIMENT_BUCKET']}")
    print(f"Prefix:  {os.environ.get('SENTIMENT_PREFIX', 'sentiment')}")
    print()

    result = fn.lambda_handler(SAMPLE_EVENT, context=None)

    print("\nHandler return:")
    print(json.dumps(result, indent=2))

    failures = result.get("batchItemFailures", [])
    if failures:
        print(f"\n❌ {len(failures)} record(s) failed.")
        return 1
    print(f"\nAll {len(SAMPLE_EVENT['Records'])} record(s) scored successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
