# Project 11_02: Near Real-Time Financial Forecasting on AWS

A serverless AWS pipeline that ingests live stock prices and financial news,
scores the news sentiment, and forecasts price trajectories with two DeepAR
models on Amazon SageMaker:

- **Operational horizon:** 5-minute bars, 1 hour ahead, sentiment-aware. Emails
  an alert when an actual price falls outside the predicted band.
- **Strategic horizon:** daily closes, 7 days ahead, for week-out trend
  monitoring.

Both feed one shared QuickSight dashboard. Once deployed the pipeline runs on its
own through EventBridge schedules and Step Functions, with no human in the loop.

- **Tickers:** TSLA, MSFT, NVDA
- **Region:** eu-west-1
- **Full write-up:** [report.pdf](report.pdf)

## Architecture

The operational (hourly) pipeline, end to end. Each stage is decoupled from the
next by a queue, an S3 file, or a DynamoDB stream:

```
Alpha Vantage API
      │
      ▼
┌───────────────────────────┐   (Ilias's account, EventBridge cron)
│  data-ingestion Lambda    │
└──────────┬────────────────┘
           ├──► SQS proj1102-prices-queue ──┐
           └──► SQS proj1102-news-queue ────┼─► ┌────────────────────────┐
                                            │   │  sentiment Lambda      │  (SQS trigger)
                                            │   │  Comprehend → CSV      │
                                            │   └─────────┬──────────────┘
                                            │             ▼
                                            │   s3://proj1102-data/sentiment/<TICKER>.csv
                                            ▼
                           ┌────────────────────────────────┐
                           │  Glue data-preparation (hourly)│
                           │  drain prices + join sentiment │
                           │  → forecast-ready/<TICKER>.parquet
                           └─────────┬──────────────────────┘
                                     ▼
                           ┌────────────────────────────────┐
                           │  forecast Lambda (Step Funcs)  │
                           │  DeepAR endpoint + backfill    │
                           └─────────┬──────────────────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              ▼                      ▼                       ▼
     DynamoDB proj1102-forecasts   S3 forecast-output/   DynamoDB Stream
              │                      │                       ▼
              │                      ▼              anomaly-alert Lambda
              │            QuickSight refresh        (Arthur's account):
              │            Lambda (hourly)           filter MODIFY +
              │                      ▼               is_anomaly == true
              │            Athena → QuickSight              ▼
              │            (Direct Query)            SNS → email alerts
              ▼
     accuracy + anomaly history (backfilled)
```

Each member runs their own components in their own AWS account; the shared S3
bucket `proj1102-data` is the common data plane. Cross-account access is granted
with S3 bucket policies and DynamoDB resource policies.

## Repository layout

```
config/          pipeline config (assets, queue URLs)
docs/            CloudWatch Insights queries + team/ (handoffs, coordination plan)
glue/            Glue data-preparation job (Dalibor)
housekeeping/    pipeline health probe + Dockerfile
infrastructure/  one-off bootstrap scripts
lambda/          one folder per Lambda:
                   data_ingestion, sentiment, glue_trigger, forecast,
                   retrain, freshness, daily_ingest, forecast_daily,
                   daily_retrain, anomalie_detection
quicksight/      Athena tables (SQL) + refresh Lambda
sagemaker/       DeepAR train + deploy scripts (hourly and daily)
scripts/         local utilities (backtest, test-data generator, helpers)
stepfunctions/   orchestration state machines (hourly + daily)
storage/         DynamoDB access helper
tests/           pytest unit tests
photos/          dashboard and Glue console screenshots
first_draft/     first-delivery submission
report.pdf       final report
Makefile         common commands (check, test, health, deploy-*)
```

## Components

| Stage | Owner | Main AWS services |
|-------|-------|-------------------|
| Ingestion | Ilias | Lambda, EventBridge, SQS |
| Sentiment | Ralph | Lambda, Comprehend |
| Data preparation | Dalibor | Glue, S3 |
| Forecasting + retraining | Ralph | Lambda, SageMaker (DeepAR), Step Functions |
| Storage | shared | DynamoDB, S3 |
| Visualisation | Francesco | Athena, QuickSight |
| Alerting | Arthur | DynamoDB Streams, SNS |

## Deployment

Each component is deployed by its owner through its own `deploy.sh`; the common
ones are wired into the Makefile:

```bash
make deploy-hourly                  # sentiment + forecast + retrain + freshness
make deploy-daily                   # daily ingest + forecast + retrain
cd quicksight    && bash deploy.sh  # Athena tables + refresh Lambda
cd stepfunctions && bash deploy.sh  # orchestration state machines
```

Ingestion (Ilias) and the Glue job (Dalibor) are deployed from their own
accounts; only the source lives here.

## Retraining

Both models retrain weekly on an EventBridge cron so accuracy does not drift:

| Model | Schedule | Lambda |
|-------|----------|--------|
| Hourly (5-min, 1h ahead) | Sunday 06:00 UTC | proj1102-retrain |
| Daily (7 days ahead) | Sunday 07:00 UTC | proj1102-daily-retrain |

## Local checks

```bash
make check    # python compile + bash syntax + Step Functions JSON
make test     # pytest unit tests
make health   # query AWS and print a health board for the whole pipeline
```

## Team

| Member | Stage | Email |
|--------|-------|-------|
| Ralph Khairallah | Sentiment + forecasting | ralphkhairallah200@gmail.com |
| Ilias El Mariky | Ingestion | ielmariky@gmail.com |
| Dalibor Švonavec | Data preparation (Glue) | daliborsvonavec@gmail.com |
| Francesco Barillari | Dashboard | francesco.barillari@estudiantat.upc.edu |
| Arthur Bohin | Alerting + orchestration | arthur.bohin@gmail.com |
