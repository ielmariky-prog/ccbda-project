# Sentiment Lambda — `proj1102` step 5

Author: Ralph
Position in pipeline: between **Ilias's data-ingestion Lambda** (produces) and **Dalibor's Glue job** (consumes).

```
news API ──► [Ilias] ──► SQS news-queue ──► [Ralph — this Lambda] ──► S3 sentiment/<TICKER>.csv ──► [Dalibor's Glue] ──► forecast-ready parquet
```

## What it does

1. Reads SQS messages of the form
   ```json
   {
     "ticker":    "TSLA",
     "timestamp": "2026-05-11T10:30:00Z",
     "headline":  "Tesla unveils new battery tech",
     "url":       "https://...",
     "source":    "alpha_vantage"
   }
   ```
2. Calls **Amazon Comprehend `DetectSentiment`** on the headline.
3. Converts the per-class probabilities into one number in `[-1, +1]`:
   ```
   score = P(positive) − P(negative)
   ```
4. Appends a row to **`s3://proj1102-data/sentiment/<TICKER>.csv`** with the schema Dalibor's Glue expects:
   ```
   ticker, timestamp, sentiment_score, confidence, headline
   ```
5. If anything fails for a specific SQS record, returns it in `batchItemFailures` so SQS retries just that one record (partial batch response).

## Deployment

### Environment variables
| Name                | Value                       |
|---------------------|-----------------------------|
| `SENTIMENT_BUCKET`  | `proj1102-data`             |
| `SENTIMENT_PREFIX`  | `sentiment` (default)       |
| `APPREGION`         | `eu-west-1` (default)       |

### IAM permissions needed
- `comprehend:DetectSentiment`
- `s3:GetObject` and `s3:PutObject` on `arn:aws:s3:::proj1102-data/sentiment/*`
- `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes` on `proj1102-news-queue`

### Trigger
SQS event source on `proj1102-news-queue`:
- **Batch size:** 1–10 messages (small batches to keep the read-modify-write of the CSV cheap)
- **Reserved concurrency:** **1** for now — see *"Concurrency note"* below
- **Visibility timeout:** ≥ Lambda timeout (suggested: 60 s)
- **Report batch item failures:** ✅ enabled (required for the partial-batch retry behaviour)

### Concurrency note
This Lambda does **read-modify-write** on a single CSV per ticker (`s3://.../sentiment/TSLA.csv`). Two concurrent invocations could overwrite each other's appended rows. While the news queue is low-rate (a few messages per Glue interval), capping reserved concurrency at **1** is the safest setup. If we ever need to scale up, the cleanest fix is to switch to **sharded files** (one per Lambda invocation, e.g. `sentiment/TSLA/<RUN_TS>.csv`) and update Dalibor's Glue job to list + merge them.

## Local smoke test

```bash
cd lambda/sentiment
python3 -m venv .venv && source .venv/bin/activate
pip install boto3
python3 test_local.py     # sends a fake SQS event through lambda_handler
```

The test reads three mock news messages, runs Comprehend on them, and prints the resulting rows. It does **not** write to S3 unless `SENTIMENT_BUCKET` is set.

## Open questions for the team
- **Concurrency model**: stay at 1 concurrent Lambda + monolithic CSV, or go to sharded files and let Dalibor list + merge? Lower priority while news rate is low.
- **Headline length**: Comprehend has a 5 KB / 5000 character limit. Current Alpha Vantage headlines are well below that, so no truncation needed yet.
- **Language detection**: assumed English (`LanguageCode="en"`). If non-English headlines start appearing, switch to `DetectDominantLanguage` first.
