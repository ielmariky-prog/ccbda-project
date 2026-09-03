# Forecast Lambda — `proj1102` step 6

Author: Ralph
Position in pipeline: consumes Dalibor's clean parquets, produces predictions for Francesco + Arthur.

```
Dalibor's Glue ──► s3://proj1102-data/forecast-ready/<TICKER>.parquet
                          │
                          ▼
              proj1102-forecast Lambda  (Ralph's account 148557232117)
                          │
            ┌─────────────┼──────────────────────┐
            │             │                      │
   DeepAR serverless   S3 CSV                DynamoDB
   endpoint (predict)  forecast-output/      proj1102-forecasts
                       <TICKER>/             (cross-account read
                       predictions_*.csv      for Francesco + Arthur)
            │             │                      │
            ▼             ▼                      ▼
        p10/p50/p90   Francesco's Athena    Francesco's widget +
                      + QuickSight charts   Arthur's SNS alerts
```

## Why everything is in Ralph's account

The team sandbox account (`834922934600`) has a tightly-scoped deployer user — it
can't even read `forecast-ready/`, and Comprehend is blocked at the account level.
Ralph's account has full SageMaker + DynamoDB + cross-account S3 to the team
bucket, so the whole forecast stack runs here. Outputs land where the team can
reach them:

- **S3 CSV** → `s3://proj1102-data/forecast-output/` — team bucket, Ralph writes cross-account.
- **DynamoDB** → `proj1102-forecasts` in Ralph's account, with a resource-based policy granting account `834922934600` read access.

## What the Lambda does — every run

For each ticker (`TSLA`, `MSFT`, `NVDA`):

1. **Read** `s3://proj1102-data/forecast-ready/<TICKER>.parquet`
   (schema `item_id, timestamp, target_value, sentiment_score`).
2. **Backfill** — any earlier DynamoDB prediction row whose `timestamp` is now in
   the parquet gets `actual`, `is_anomaly` and `delta` filled in.
3. **Predict** — last 120 price points → DeepAR serverless endpoint → P10/P50/P90
   for the next 12 steps (1 hour at 5-min granularity).
4. **Write DynamoDB** — new prediction rows (`actual` / `is_anomaly` / `delta`
   null until a later cycle backfills them).
5. **Write S3 CSV** — `forecast-output/<TICKER>/predictions_<DATE>.csv`
   (schema `item_id, timestamp, p10, p50, p90`) for Francesco's Athena.

### Anomaly rule (from the Ralph → Francesco handoff)
```
is_anomaly = actual < p10  OR  actual > p90
delta      = actual - p50
severity   = low    (actual inside the p10-p90 band)
             medium (outside by less than one band-width)
             high   (outside by a full band-width or more)
```

## DynamoDB table — `proj1102-forecasts`

| Attribute | Key | Type | Notes |
|---|---|---|---|
| `ticker` | partition | string | `TSLA` |
| `timestamp` | sort | string | ISO-8601 UTC |
| `p10` / `p50` / `p90` | — | number | prediction quantiles |
| `actual` | — | number | real price, backfilled next cycle |
| `is_anomaly` | — | boolean | `actual < p10 OR actual > p90` |
| `delta` | — | number | `actual - p50` |
| `severity` | — | string | `low` / `medium` / `high` — backfilled with `actual` |
| `predicted_at` | — | string | when this row was written |

DynamoDB **Streams** are enabled (`NEW_IMAGE`) — that's how Arthur's
`anomaly-alert` Lambda is triggered. `actual` / `is_anomaly` / `delta` /
`severity` are null on the initial INSERT and only filled on a later MODIFY
(the backfill cycle), so Arthur's Lambda must act on MODIFY events, not just
INSERT, and tolerate the nulls.

`PAY_PER_REQUEST` billing — no provisioned capacity to manage.

## Deployment

```bash
# 1. create the DynamoDB table + cross-account read policy  (one-time)
python create_table.py

# 2. train DeepAR + deploy the serverless endpoint           (one-time / on retrain)
cd ../../sagemaker && python train_and_deploy.py all

# 3. deploy the Lambda + EventBridge schedule
cd ../lambda/forecast && bash deploy.sh
```

### Environment variables
| Name | Value |
|---|---|
| `FORECAST_BUCKET` | `proj1102-data` |
| `FORECAST_READY_PREFIX` | `forecast-ready` |
| `FORECAST_OUTPUT_PREFIX` | `forecast-output` |
| `DDB_TABLE` | `proj1102-forecasts` |
| `SAGEMAKER_ENDPOINT` | `proj1102-deepar-endpoint` |
| `TICKERS` | `TSLA,MSFT,NVDA` |
| `CONTEXT_LENGTH` | `120` |
| `PREDICTION_LENGTH` | `12` |

### Dependencies
The Lambda uses **pandas + pyarrow** to read parquet — provided by the AWS-managed
**AWS SDK for pandas** layer (`AWSSDKPandas-Python312`). No custom packaging needed.

### Schedule
EventBridge rule `proj1102-forecast-schedule` — `rate(3 hours)`, lined up to run
just after Dalibor's Glue cycle.

## Cost

- **DeepAR serverless endpoint** — scales to zero when idle, ~\$0 between cycles. Pays only for the ~1 s per invocation.
- **Lambda** — well within the free tier (1 M invocations / month).
- **DynamoDB** — `PAY_PER_REQUEST`, a few hundred tiny writes per day ≈ cents.
- **Training** — ~\$0.02 per DeepAR training run on `ml.c5.xlarge` (~5 min).

## Notes / open items

- **Retraining cadence** — `train_and_deploy.py` is run manually for now. Could be
  put on a weekly EventBridge schedule once we settle on a cadence with the prof.
- **Sentiment as a feature** — `forecast-ready/` carries a `sentiment_score`
  column. Once the sentiment Lambda is feeding it with real data, DeepAR can take
  it as a *related time series* (`dynamic_feat`) for a likely accuracy bump. Not
  wired yet — current model trains on `target_value` only.
