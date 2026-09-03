"""
SageMaker DeepAR forecast pipeline - proj1102 step 6 (Ralph).

Phases (run with one of the subcommands below):

    python forecast_pipeline.py prep         # Phase 1: parquets -> DeepAR JSON Lines
    python forecast_pipeline.py train        # Phase 2: submit DeepAR training job
    python forecast_pipeline.py deploy       # Phase 3: deploy endpoint (or describe existing)
    python forecast_pipeline.py predict      # Phase 3b: call endpoint, save forecasts
    python forecast_pipeline.py teardown     # Delete endpoint (stop paying)
    python forecast_pipeline.py status       # Print state.json

State is persisted to `sagemaker/state.json` so each phase is resumable.

Data flow:
    Dalibor's S3:    s3://proj1102-data/processed/<TICKER>.parquet
                       (item_id, timestamp, target_value, sentiment_score=NaN)
                            │
                            ▼
    My S3 bucket:    s3://proj1102-ralph-forecast-<ACCOUNT_ID>/deepar/data/train.jsonl
                       (one JSON line per ticker, DeepAR format)
                            │
                            ▼
    SageMaker:       training job -> model artifact -> endpoint
                            │
                            ▼
    Output:          sagemaker/output/<TICKER>_forecast.csv
                       (timestamp, p10, p50, p90)
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import boto3
import pandas as pd

# Config
PROFILE     = "ralph"
REGION      = "eu-west-1"
ACCOUNT_ID  = "148557232117"

SOURCE_BUCKET = "proj1102-data"
SOURCE_PREFIX = "processed"
TICKERS       = ["TSLA", "MSFT", "NVDA"]

MY_BUCKET    = f"proj1102-ralph-forecast-{ACCOUNT_ID}"
S3_PREFIX    = "deepar"

# DeepAR hyperparameters (5-min granularity, predict 1h ahead from 10h history)
FREQ                = "5min"
CONTEXT_LENGTH      = 120   # 10 hours of history
PREDICTION_LENGTH   = 12    # 1 hour ahead

# SageMaker compute
TRAINING_INSTANCE   = "ml.c5.xlarge"     # ~$0.20/hr
ENDPOINT_INSTANCE   = "ml.t2.medium"     # ~$0.06/hr
ROLE_NAME           = "ProjectForecastS3Role"  # created earlier for Forecast attempt

LOCAL_DIR = Path(__file__).parent
INPUT_DIR  = LOCAL_DIR / "input"
OUTPUT_DIR = LOCAL_DIR / "output"
STATE_FILE = LOCAL_DIR / "state.json"


# Boto3 clients
session   = boto3.Session(profile_name=PROFILE, region_name=REGION)
s3        = session.client("s3")
sm        = session.client("sagemaker")
sm_rt     = session.client("sagemaker-runtime")
iam       = session.client("iam")


def step(msg: str) -> None:
    print(f"\n=== {msg} ===")


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


# Phase 1 - Read parquets and produce DeepAR JSON Lines
def read_ticker_series(ticker: str) -> pd.DataFrame:
    """Download Dalibor's processed parquet and return a sorted DataFrame."""
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    local = INPUT_DIR / f"{ticker}.parquet"
    key = f"{SOURCE_PREFIX}/{ticker}.parquet"
    s3.download_file(SOURCE_BUCKET, key, str(local))
    df = pd.read_parquet(local)
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def to_deepar_record(df: pd.DataFrame, holdout: int = PREDICTION_LENGTH) -> tuple[dict, dict]:
    """
    Build a DeepAR JSON-Lines record for one ticker.

    Returns (train_record, test_record) where the test record contains the
    full series and the train record drops the last `holdout` points.
    """
    if df.empty:
        raise ValueError("empty series")

    start = df["timestamp"].iloc[0].replace("T", " ").replace("Z", "")
    target = df["target_value"].astype(float).tolist()

    train_target = target[:-holdout] if holdout > 0 else target

    train = {"start": start, "target": train_target}
    test  = {"start": start, "target": target}
    return train, test


def write_jsonl(path: Path, records: Iterable[dict]) -> None:
    with path.open("w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def phase_prep() -> dict:
    step("Phase 1 - Read parquets and produce DeepAR JSON Lines")
    state = load_state()

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    train_records, test_records, summary = [], [], []

    for t in TICKERS:
        df = read_ticker_series(t)
        train, test = to_deepar_record(df, holdout=PREDICTION_LENGTH)
        train_records.append(train)
        test_records.append(test)
        summary.append({
            "ticker":   t,
            "rows":     len(df),
            "from":     df["timestamp"].iloc[0],
            "to":       df["timestamp"].iloc[-1],
            "min":      float(df["target_value"].min()),
            "max":      float(df["target_value"].max()),
            "train_n":  len(train["target"]),
        })

    train_path = INPUT_DIR / "train.jsonl"
    test_path  = INPUT_DIR / "test.jsonl"
    write_jsonl(train_path, train_records)
    write_jsonl(test_path,  test_records)

    # Upload to my own S3 bucket (SageMaker training input)
    train_key = f"{S3_PREFIX}/data/train.jsonl"
    test_key  = f"{S3_PREFIX}/data/test.jsonl"
    s3.upload_file(str(train_path), MY_BUCKET, train_key)
    s3.upload_file(str(test_path),  MY_BUCKET, test_key)

    train_s3 = f"s3://{MY_BUCKET}/{train_key}"
    test_s3  = f"s3://{MY_BUCKET}/{test_key}"

    print(f"\nUploaded {len(TICKERS)} series:")
    for s in summary:
        print(f"  {s['ticker']:<10} {s['train_n']:>5} train pts   "
              f"price=[{s['min']:.2f}, {s['max']:.2f}]   "
              f"{s['from']} -> {s['to']}")
    print(f"\nTrain: {train_s3}")
    print(f"Test:  {test_s3}")

    state.update({
        "train_s3": train_s3,
        "test_s3":  test_s3,
        "tickers":  TICKERS,
        "summary":  summary,
        "freq":              FREQ,
        "context_length":    CONTEXT_LENGTH,
        "prediction_length": PREDICTION_LENGTH,
    })
    save_state(state)
    return state


# Phase 2 - Submit DeepAR training job
DEEPAR_IMAGE = {
    "eu-west-1": "224300973850.dkr.ecr.eu-west-1.amazonaws.com/forecasting-deepar:1",
    "us-east-1": "522234722520.dkr.ecr.us-east-1.amazonaws.com/forecasting-deepar:1",
}


def phase_train():
    step("Phase 2 - Submit DeepAR training job")
    state = load_state()
    if "train_s3" not in state:
        sys.exit("missing train_s3 in state - run `prep` first")

    image = DEEPAR_IMAGE[REGION]
    role_arn = f"arn:aws:iam::{ACCOUNT_ID}:role/{ROLE_NAME}"
    job_name = f"proj1102-deepar-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

    output_s3 = f"s3://{MY_BUCKET}/{S3_PREFIX}/output"

    request = {
        "TrainingJobName": job_name,
        "AlgorithmSpecification": {
            "TrainingImage": image,
            "TrainingInputMode": "File",
        },
        "RoleArn": role_arn,
        "InputDataConfig": [
            {
                "ChannelName": "train",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": state["train_s3"],
                        "S3DataDistributionType": "FullyReplicated",
                    }
                },
                "ContentType": "json",
                "CompressionType": "None",
            },
            {
                "ChannelName": "test",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": state["test_s3"],
                        "S3DataDistributionType": "FullyReplicated",
                    }
                },
                "ContentType": "json",
                "CompressionType": "None",
            },
        ],
        "OutputDataConfig": {"S3OutputPath": output_s3},
        "ResourceConfig": {
            "InstanceType":  TRAINING_INSTANCE,
            "InstanceCount": 1,
            "VolumeSizeInGB": 10,
        },
        "StoppingCondition": {"MaxRuntimeInSeconds": 60 * 60},
        "HyperParameters": {
            "time_freq":                     FREQ,
            "context_length":                str(CONTEXT_LENGTH),
            "prediction_length":             str(PREDICTION_LENGTH),
            "epochs":                        "50",
            "num_cells":                     "40",
            "num_layers":                    "2",
            "likelihood":                    "gaussian",
            "mini_batch_size":               "64",
            "learning_rate":                 "1E-3",
            "dropout_rate":                  "0.1",
            "early_stopping_patience":       "10",
        },
    }

    print(f"Job name: {job_name}")
    print(f"Image:    {image}")
    print(f"Role:     {role_arn}")
    print(f"Output:   {output_s3}")
    print(f"Instance: {TRAINING_INSTANCE}")
    print(f"Submitting ...")

    sm.create_training_job(**request)

    state.update({
        "training_job_name": job_name,
        "training_image":    image,
        "model_output":      output_s3,
        "model_artifact":    f"{output_s3}/{job_name}/output/model.tar.gz",
        "role_arn":          role_arn,
    })
    save_state(state)

    print(f"\nTraining submitted. Watch progress with:")
    print(f"  python forecast_pipeline.py status")
    print(f"  aws sagemaker describe-training-job --training-job-name {job_name} "
          f"--profile {PROFILE} --region {REGION} --query Status")


# Phase 3 - Deploy endpoint
MODEL_NAME    = "proj1102-deepar-model"
EP_CONFIG     = "proj1102-deepar-config"
EP_NAME       = "proj1102-deepar-endpoint"


def _wait_training(job_name: str, poll: int = 30) -> str:
    """Block until training reaches a terminal state. Returns final status."""
    while True:
        r = sm.describe_training_job(TrainingJobName=job_name)
        status = r["TrainingJobStatus"]
        secondary = r.get("SecondaryStatus", "")
        print(f"  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {status} / {secondary}")
        if status in ("Completed", "Failed", "Stopped"):
            return status
        time.sleep(poll)


def phase_deploy():
    step("Phase 3 - Deploy DeepAR endpoint")
    state = load_state()

    job_name = state.get("training_job_name")
    if not job_name:
        sys.exit("no training_job_name in state - run `train` first")

    # 1) Wait for the training job to be done
    print("Waiting for training to complete (this can take 15-20 minutes)...")
    status = _wait_training(job_name)
    if status != "Completed":
        sys.exit(f"training ended with status {status}")

    # 2) Create the model artifact reference in SageMaker
    model_artifact = state["model_artifact"]
    print(f"\nCreating SageMaker model from {model_artifact}")
    try:
        sm.delete_model(ModelName=MODEL_NAME)
    except sm.exceptions.ClientError:
        pass
    sm.create_model(
        ModelName=MODEL_NAME,
        PrimaryContainer={
            "Image": state["training_image"],
            "ModelDataUrl": model_artifact,
        },
        ExecutionRoleArn=state["role_arn"],
    )

    # 3) Endpoint config (instance type for inference)
    print(f"Creating endpoint config ...")
    try:
        sm.delete_endpoint_config(EndpointConfigName=EP_CONFIG)
    except sm.exceptions.ClientError:
        pass
    sm.create_endpoint_config(
        EndpointConfigName=EP_CONFIG,
        ProductionVariants=[{
            "VariantName":          "AllTraffic",
            "ModelName":            MODEL_NAME,
            "InitialInstanceCount": 1,
            "InstanceType":         ENDPOINT_INSTANCE,
        }],
    )

    # 4) Endpoint (create or update)
    existing = None
    try:
        existing = sm.describe_endpoint(EndpointName=EP_NAME)
    except sm.exceptions.ClientError:
        pass

    if existing:
        print(f"Endpoint exists; updating to use new config ...")
        sm.update_endpoint(EndpointName=EP_NAME, EndpointConfigName=EP_CONFIG)
    else:
        print(f"Creating endpoint {EP_NAME} ...")
        sm.create_endpoint(EndpointName=EP_NAME, EndpointConfigName=EP_CONFIG)

    # 5) Wait until InService
    print("Waiting for endpoint to be InService (5-10 minutes) ...")
    while True:
        r = sm.describe_endpoint(EndpointName=EP_NAME)
        st = r["EndpointStatus"]
        print(f"  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {st}")
        if st in ("InService", "Failed"):
            break
        time.sleep(30)

    if st != "InService":
        sys.exit(f"endpoint ended in status {st}")

    state.update({
        "model_name":   MODEL_NAME,
        "ep_config":    EP_CONFIG,
        "endpoint":     EP_NAME,
    })
    save_state(state)
    print(f"\nEndpoint ready: {EP_NAME}")
    print("Next: `python forecast_pipeline.py predict`")


# Phase 3b - Call endpoint, save forecasts
def _last_context_from_test(test_path: Path, n: int) -> dict[str, dict]:
    """Read test.jsonl, return last `n` points per series for context."""
    out = {}
    with test_path.open() as f:
        for ticker, line in zip(TICKERS, f):
            rec = json.loads(line)
            out[ticker] = {
                "start":  rec["start"],
                "target": rec["target"][-n:],
            }
    return out


def _add_minutes(iso: str, minutes: int) -> str:
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00").replace(" ", "T"))
    return (dt + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")


def phase_predict():
    step("Phase 3b - Call endpoint, save forecasts")
    state = load_state()
    endpoint = state.get("endpoint")
    if not endpoint:
        sys.exit("no endpoint in state - run `deploy` first")

    contexts = _last_context_from_test(INPUT_DIR / "test.jsonl", CONTEXT_LENGTH)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results = {}
    for ticker, ctx in contexts.items():
        payload = {
            "instances": [{
                "start":  ctx["start"],
                "target": ctx["target"],
            }],
            "configuration": {
                "num_samples": 100,
                "output_types": ["quantiles"],
                "quantiles":    ["0.1", "0.5", "0.9"],
            },
        }
        print(f"  [{ticker}] calling endpoint ...")
        r = sm_rt.invoke_endpoint(
            EndpointName=endpoint,
            ContentType="application/json",
            Body=json.dumps(payload).encode("utf-8"),
        )
        resp = json.loads(r["Body"].read().decode("utf-8"))
        q = resp["predictions"][0]["quantiles"]
        p10, p50, p90 = q["0.1"], q["0.5"], q["0.9"]

        # Forecast timestamps continue 5 min after the last context point.
        # The last context point's timestamp = start + (len-1)*5 min,
        # so the first forecast point is at start + len*5 min.
        # We need to find the actual last_timestamp from the original df.
        df = read_ticker_series(ticker)
        last_ts = df["timestamp"].iloc[-PREDICTION_LENGTH - 1]  # train end
        ts = [_add_minutes(last_ts, 5 * (i + 1)) for i in range(PREDICTION_LENGTH)]

        out_df = pd.DataFrame({
            "ticker":    ticker,
            "timestamp": ts,
            "p10":       p10,
            "p50":       p50,
            "p90":       p90,
        })
        out_path = OUTPUT_DIR / f"{ticker}_forecast.csv"
        out_df.to_csv(out_path, index=False)
        results[ticker] = str(out_path)
        print(f"    P50 first/last: {p50[0]:.2f} -> {p50[-1]:.2f}  "
              f"band width @ t+1h: {p90[-1] - p10[-1]:.2f}")

    state["forecasts"] = results
    save_state(state)
    print(f"\nForecasts written to {OUTPUT_DIR}/")


# Phase 4 - Evaluate predictions against the held-out actuals
def phase_evaluate():
    step("Evaluate forecast vs held-out actuals (last 12 points / 1 hour)")
    state = load_state()
    if "forecasts" not in state:
        sys.exit("no forecasts in state - run `predict` first")

    rows = []
    for ticker in TICKERS:
        df = read_ticker_series(ticker)
        actual = df["target_value"].astype(float).tolist()[-PREDICTION_LENGTH:]
        forecast_path = state["forecasts"][ticker]
        fdf = pd.read_csv(forecast_path)
        p10, p50, p90 = fdf["p10"].tolist(), fdf["p50"].tolist(), fdf["p90"].tolist()

        # Metrics
        import math
        mae  = sum(abs(a - p) for a, p in zip(actual, p50)) / len(actual)
        rmse = math.sqrt(sum((a - p) ** 2 for a, p in zip(actual, p50)) / len(actual))
        mape = sum(abs(a - p) / a for a, p in zip(actual, p50)) / len(actual) * 100
        coverage = sum(1 for a, lo, hi in zip(actual, p10, p90) if lo <= a <= hi) / len(actual)

        rows.append({
            "ticker":    ticker,
            "mae":       round(mae, 2),
            "rmse":      round(rmse, 2),
            "mape_%":    round(mape, 2),
            "p10_p90_coverage_%": round(coverage * 100, 1),
            "actual_first": actual[0], "p50_first": p50[0],
            "actual_last":  actual[-1], "p50_last":  p50[-1],
        })

    rep = pd.DataFrame(rows)
    rep.to_csv(OUTPUT_DIR / "evaluation.csv", index=False)
    print(rep.to_string(index=False))
    print(f"\nWritten {OUTPUT_DIR}/evaluation.csv")


# Teardown - stop the endpoint to stop paying
def phase_teardown():
    step("Teardown - delete endpoint (and config + model)")
    for name, fn in [
        ("endpoint",       lambda: sm.delete_endpoint(EndpointName=EP_NAME)),
        ("endpoint config",lambda: sm.delete_endpoint_config(EndpointConfigName=EP_CONFIG)),
        ("model",          lambda: sm.delete_model(ModelName=MODEL_NAME)),
    ]:
        try:
            fn()
            print(f"  deleted {name}")
        except sm.exceptions.ClientError as e:
            print(f"  {name}: {e.response['Error'].get('Code')}")
    state = load_state()
    for k in ("endpoint", "ep_config", "model_name"):
        state.pop(k, None)
    save_state(state)


def phase_status():
    state = load_state()
    if not state:
        print("(no state.json yet - run `prep` first)")
        return
    print(json.dumps(state, indent=2))


# Entry point
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["prep", "train", "deploy", "predict", "evaluate", "teardown", "status"])
    args = parser.parse_args()

    dispatch = {
        "prep":     phase_prep,
        "train":    phase_train,
        "deploy":   phase_deploy,
        "predict":  phase_predict,
        "evaluate": phase_evaluate,
        "teardown": phase_teardown,
        "status":   phase_status,
    }
    dispatch[args.phase]()


if __name__ == "__main__":
    main()
