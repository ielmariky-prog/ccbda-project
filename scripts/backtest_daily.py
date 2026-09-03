"""
Rolling walk-forward backtest of the daily DeepAR model - proj1102.

For each test day in the last ~6 months of trading history, this script:
  1. Takes the prior 60 daily closes as context.
  2. Invokes the daily DeepAR serverless endpoint for a 7-day forecast.
  3. Compares each step (t+1 … t+7) against the realized closes.

Outputs:
    sagemaker/backtest_results.csv     row per (ticker, test_day, step)
    sagemaker/backtest_summary.csv     aggregated by step

CAVEAT - IN-SAMPLE backtest. The currently deployed daily model was trained
on data up to 2026-05-15 INCLUSIVE, which covers every test point used here.
Results are therefore optimistic: the model has seen each test target during
training. A true out-of-sample walk-forward would retrain the model on data
ending at each test day (~$1-2 in incremental training cost). The numbers
below are best read as "the model successfully learned to fit this history"
rather than "the model will perform this well on unseen future data."
"""

from __future__ import annotations

import io
import json
from pathlib import Path

import boto3
import pandas as pd

PROFILE        = "ralph"
REGION         = "eu-west-1"
ENDPOINT       = "proj1102-deepar-daily-endpoint"
S3_BUCKET      = "proj1102-data"
DAILY_PREFIX   = "daily"
CONTEXT_LENGTH = 60
HORIZON        = 7
N_TEST_DAYS    = 126   # ~6 months of trading days
TICKERS        = ["TSLA", "MSFT", "NVDA"]

OUT_DIR = Path(__file__).resolve().parent.parent / "sagemaker"
OUT_DIR.mkdir(exist_ok=True)

sess = boto3.Session(profile_name=PROFILE, region_name=REGION)
s3   = sess.client("s3")
rt   = sess.client("sagemaker-runtime")


def _predict(start_iso: str, target: list[float]) -> dict:
    payload = {
        "instances": [{"start": start_iso, "target": [float(x) for x in target]}],
        "configuration": {
            "num_samples": 100,
            "output_types": ["quantiles"],
            "quantiles": ["0.1", "0.5", "0.9"],
        },
    }
    r = rt.invoke_endpoint(
        EndpointName=ENDPOINT,
        ContentType="application/json",
        Body=json.dumps(payload).encode("utf-8"),
    )
    return json.loads(r["Body"].read())["predictions"][0]["quantiles"]


def main() -> None:
    records = []
    for ticker in TICKERS:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{DAILY_PREFIX}/{ticker}.parquet")
        df  = pd.read_parquet(io.BytesIO(obj["Body"].read())) \
                .sort_values("timestamp").reset_index(drop=True)

        n         = len(df)
        start_idx = max(CONTEXT_LENGTH, n - N_TEST_DAYS)
        end_idx   = n - HORIZON
        windows   = end_idx - start_idx
        print(f"[{ticker}] {n} daily rows; running {windows} walk-forward windows "
              f"({df['timestamp'].iloc[start_idx][:10]} -> {df['timestamp'].iloc[end_idx-1][:10]})")

        for tp in range(start_idx, end_idx):
            ctx     = df.iloc[tp - CONTEXT_LENGTH:tp]
            actuals = df.iloc[tp:tp + HORIZON]
            start_iso = ctx["timestamp"].iloc[0].replace("T", " ").replace("Z", "")
            target_ctx = ctx["target_value"].tolist()
            try:
                q = _predict(start_iso, target_ctx)
            except Exception as e:
                print(f"  [ERR] {ticker} @ {df['timestamp'].iloc[tp]}: {e}")
                continue
            for step in range(HORIZON):
                records.append({
                    "ticker":  ticker,
                    "test_day": df["timestamp"].iloc[tp],
                    "step":    step + 1,
                    "actual":  float(actuals["target_value"].iloc[step]),
                    "p10":     float(q["0.1"][step]),
                    "p50":     float(q["0.5"][step]),
                    "p90":     float(q["0.9"][step]),
                })

    results = pd.DataFrame(records)
    results["abs_err"]  = (results["actual"] - results["p50"]).abs()
    results["mape_pct"] = 100 * results["abs_err"] / results["actual"]
    results["in_band"]  = (results["actual"] >= results["p10"]) & (results["actual"] <= results["p90"])

    results.to_csv(OUT_DIR / "backtest_results.csv", index=False)

    summary = (
        results.groupby("step")
               .agg(n=("actual", "count"),
                    mape_pct=("mape_pct", "mean"),
                    coverage_pct=("in_band", lambda s: 100 * s.sum() / len(s)))
               .reset_index()
    )
    summary.to_csv(OUT_DIR / "backtest_summary.csv", index=False)

    print("\n=== Per-step summary ===")
    print(summary.to_string(index=False))

    ticker_summary = (
        results.groupby("ticker")
               .agg(n=("actual", "count"),
                    mape_pct=("mape_pct", "mean"),
                    coverage_pct=("in_band", lambda s: 100 * s.sum() / len(s)))
               .reset_index()
    )
    print("\n=== Per-ticker summary ===")
    print(ticker_summary.to_string(index=False))


if __name__ == "__main__":
    main()
