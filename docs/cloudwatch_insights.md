# CloudWatch Logs Insights — operational queries

Lab 7 (Log analysis using cloud tools) introduced CloudWatch Logs Insights as
the right tool for ad-hoc log analysis on AWS. Every Lambda in proj1102
streams its `print()` output to its own CloudWatch log group, and Step
Functions execution events go to `/aws/vendedlogs/states/*`. The queries
below are the ones we used (and re-use) when diagnosing incidents.

All queries assume the **eu-west-1** region. Run them from the AWS console
under *CloudWatch → Logs Insights*, or via the CLI:

```bash
aws logs start-query \
  --log-group-name /aws/lambda/proj1102-forecast \
  --start-time $(date -u -d '6 hours ago' +%s) \
  --end-time   $(date -u +%s) \
  --query-string "<the query string below>" \
  --profile ralph --region eu-west-1
```

---

## 1. Forecast Lambda — per-ticker outcome of the last 24 h of runs

Confirms every hourly cycle produced 12 predictions for each ticker and
no `[SKIP]` (short history) or `[ERROR]` line was emitted.

```sql
fields @timestamp, @message
| filter @message like /\[OK\]|\[SKIP\]|\[ERROR\]/
| sort @timestamp desc
| limit 200
```

Log group: `/aws/lambda/proj1102-forecast`

Pre-condition this caught for us: the sentiment-as-feature retrain on 2026-05-15
— after deploy the next two `[OK] TSLA: backfilled 12, wrote 12 predictions`
lines proved the new dynamic_feat payload was being accepted by the endpoint.

---

## 2. Sentiment Lambda — Comprehend scoring throughput

How many news messages were processed in the last hour, and what the
distribution of scores looked like.

```sql
fields @timestamp, @message
| filter @message like /\[SCORED\]/
| parse @message /\[SCORED\] (?<ticker>\w+).*score=(?<score>[+-]?\d+\.\d+)/
| stats count() as n, avg(score) as avg_sentiment by ticker
```

Log group: `/aws/lambda/proj1102-sentiment`

---

## 3. Anomaly Lambda — which ALERTs fired

When triaging a noisy day, this lists every SNS publish the lambda did,
grouped by ticker and severity.

```sql
fields @timestamp, @message
| filter @message like /\[ALERT\]/
| parse @message /\[ALERT\] (?<ticker>\w+) @ (?<ts>\S+) severity=(?<severity>\w+)/
| stats count() as alerts by ticker, severity
```

Log group: `/aws/lambda/proj1102-anomaly-alert` (in Arthur's personal account
`162655485124`).

This is the query that confirmed the **closed-market anomaly storm** (issue
4.16): ~300 `[ALERT]` lines outside market hours, all suppressed afterwards
by the `_is_market_open` filter.

---

## 4. Step Functions executions — failure post-mortem

Pulls every execution event for the hourly state machine over the last 6 h,
filtered to state transitions and failures.

```sql
fields @timestamp, type, details.error, details.cause
| filter type in ["ExecutionFailed","TaskFailed","TaskTimedOut"]
| sort @timestamp desc
```

Log group: `/aws/vendedlogs/states/proj1102-pipeline`

---

## 5. Freshness probe — recent age values

Confirms the probe is publishing the custom metric every hour and shows
the trend in `oldest_parquet_age_minutes`.

```sql
fields @timestamp, @message
| filter @message like /published/
| parse @message /(?<metric>oldest_\w+) = (?<value>[\d.]+)/
| sort @timestamp desc
| limit 50
```

Log group: `/aws/lambda/proj1102-freshness`

---

## 6. Cross-component cost-of-failure trace

When you want to know *what was happening across the whole pipeline at the
moment a CloudWatch alarm fired* — point Logs Insights at the multi-log-group
view:

Log groups (all in eu-west-1, account `148557232117`):
```
/aws/lambda/proj1102-forecast
/aws/lambda/proj1102-sentiment
/aws/lambda/proj1102-quicksight-refresh
/aws/lambda/proj1102-daily-ingest
/aws/lambda/proj1102-daily-forecast
/aws/lambda/proj1102-freshness
/aws/lambda/proj1102-retrain
/aws/lambda/proj1102-daily-retrain
/aws/vendedlogs/states/proj1102-pipeline
/aws/vendedlogs/states/proj1102-daily-pipeline
```

Query (10-minute window around an incident):
```sql
fields @log, @timestamp, @message
| filter @message like /\[ERROR\]|\[ALERT\]|FAILED|TIMED_OUT/
| sort @timestamp desc
| limit 200
```

This is how the **NVDA-only ingestion outage** (issue 4.17) was localised
within 2 minutes: the only `[ERROR] NVDA` lines came from the ingestion
side (in Ilias's personal account), while every other component logged
normally — pinpointing the upstream API-quota failure without needing to
SSH or inspect anything.
