# QuickSight data bridge — `proj1102` (Ralph)

Francesco's QuickSight runs in **Ralph's account** (`148557232117`). When QuickSight
queries Athena it only sees databases in *that* account — so Francesco can't point
it at Athena tables in his own account. We hit this on 6 May and agreed the fix:
**Ralph lands both halves of the dashboard data in his own S3 + Athena**, and
Francesco just picks the tables in QuickSight.

This folder is that bridge — and it runs **automatically**, no manual step.

## How it works

```
live pipeline S3 (team bucket)                Ralph's account
─────────────────────────────                ───────────────
forecast-ready/<TICKER>.parquet ─┐
forecast-output/<TICKER>/*.csv ──┤
                                 ▼
                  proj1102-quicksight-refresh Lambda  ── EventBridge, every 3h
                                 │
                                 ▼
   s3://proj1102-ralph-forecast-148557232117/quicksight/{actuals,forecast}/
                                 │
                                 ▼
            Athena EXTERNAL tables proj1102_db.{actuals,forecast}
                                 │
                                 ▼
                      Francesco's QuickSight
```

The Lambda overwrites two fixed CSVs every 3 hours. The Athena tables are
`EXTERNAL` tables pointing at those fixed prefixes, so there's nothing to
re-register — the next query just sees fresh data.

| Table | Columns | Dashboard role |
|---|---|---|
| `actuals` | `item_id, timestamp, target_value, sentiment_score` | blue line — real price |
| `forecast` | `item_id, timestamp, p10, p50, p90` | orange line (p50) + shaded band (p10–p90) |

Tickers: `TSLA`, `MSFT`, `NVDA`. `forecast` replaces the old ARIMA
`predictions` table — that one can be deleted.

## Deploy

```bash
cd quicksight
bash deploy.sh            # role + Athena tables + Lambda + schedule + first run
bash deploy.sh update     # update Lambda code only
```

Uses the `ralph` AWS profile, region `eu-west-1`. Idempotent — safe to re-run.
After `deploy.sh` the dashboard data refreshes itself; to force an immediate
refresh, invoke `proj1102-quicksight-refresh` manually.

## What to tell Francesco

> The dashboard data is live in QuickSight and refreshes itself every 3h. New
> dataset → Athena → catalog `AwsDataCatalog` → database `proj1102_db` → tables
> **`actuals`** and **`forecast`**. Join on `(item_id, timestamp)` for the
> actual-vs-predicted chart. Set the QuickSight datasets to DirectQuery (or a
> SPICE refresh schedule) so they pick up the updates. Delete the old
> `predictions` dataset — `forecast` replaces it.

## Files

| File | Purpose |
|---|---|
| `lambda_function.py` | The refresh Lambda — live S3 → the two CSVs |
| `athena_tables.sql` | `CREATE EXTERNAL TABLE` DDL (run once by `deploy.sh`) |
| `deploy.sh` | Deploys the role, tables, Lambda and schedule |

## Note — this is a workaround, not the ideal end state

The real fix is Francesco getting his own QuickSight account so he can query his
own Athena directly. Until then this bridge keeps his dashboard fed automatically.
