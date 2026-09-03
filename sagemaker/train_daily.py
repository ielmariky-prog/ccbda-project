"""
Train + deploy the *daily*-horizon DeepAR model - proj1102 strategic forecast.

This is the longer-horizon counterpart to `train_and_deploy.py`. Where the
hourly model predicts the next 1 hour at 5-min granularity, this model
predicts the next 7 trading days at daily granularity.

Why a second model: the 5-min DeepAR is calibrated for short-horizon noise
and would produce uselessly wide bands at multi-day horizons (financial
prices at high frequency are close to a random walk). A daily-frequency
model trained on years of daily closes is the right tool for week-ahead
"strategic" outlook on a separate dashboard view.

Inputs:
    s3://proj1102-data/daily/<TICKER>.parquet
        item_id, timestamp (daily, ISO-8601 with T00:00:00Z), target_value (close), volume

Outputs:
    Training job:        proj1102-deepar-daily-<RUN_TS>
    Endpoint:            proj1102-deepar-daily-endpoint (serverless)
    Model:               proj1102-deepar-daily-model
    State:               sagemaker/daily_state.json (gitignored)

Phases (subcommands):
    python train_daily.py prep      - daily parquets -> DeepAR JSONL -> S3
    python train_daily.py train     - submit DeepAR training job
    python train_daily.py deploy    - wait for training, deploy serverless endpoint
    python train_daily.py status    - print daily_state.json
    python train_daily.py all       - prep -> train -> wait -> deploy
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd

# Config - RALPH'S ACCOUNT
PROFILE     = "ralph"
REGION      = "eu-west-1"
ACCOUNT_ID  = "148557232117"

DATA_BUCKET     = "proj1102-data"
READY_PREFIX    = "daily"
ARTIFACT_BUCKET = f"proj1102-ralph-forecast-{ACCOUNT_ID}"
SM_PREFIX       = "deepar-daily"
TICKERS         = ["TSLA", "MSFT", "NVDA"]

ROLE_ARN        = f"arn:aws:iam::{ACCOUNT_ID}:role/ProjectForecastS3Role"

FREQ                = "D"
CONTEXT_LENGTH      = 60   # ~12 weeks of trading days
PREDICTION_LENGTH   = 7    # ~1.5 weeks ahead

TRAINING_INSTANCE   = "ml.c5.xlarge"
DEEPAR_IMAGE = {
    "eu-west-1": "224300973850.dkr.ecr.eu-west-1.amazonaws.com/forecasting-deepar:1",
}

MODEL_NAME    = "proj1102-deepar-daily-model"
EP_CONFIG     = "proj1102-deepar-daily-config"
EP_NAME       = "proj1102-deepar-daily-endpoint"

# Serverless inference - cap is 3072 MB per the account quota
SERVERLESS_MEMORY_MB  = 3072
SERVERLESS_MAX_CONCUR = 5

LOCAL_DIR  = Path(__file__).parent
INPUT_DIR  = LOCAL_DIR / "daily_input"
STATE_FILE = LOCAL_DIR / "daily_state.json"

session = boto3.Session(profile_name=PROFILE, region_name=REGION)
s3  = session.client("s3")
sm  = session.client("sagemaker")


def step(msg): print(f"\n=== {msg} ===")
def load_state(): return json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}
def save_state(s): STATE_FILE.write_text(json.dumps(s, indent=2))


# prep - daily parquets -> DeepAR JSONL
def phase_prep() -> dict:
    step("prep - daily parquets -> DeepAR JSON Lines")
    INPUT_DIR.mkdir(exist_ok=True)
    state = load_state()

    train_records, test_records, summary = [], [], []
    for t in TICKERS:
        obj = s3.get_object(Bucket=DATA_BUCKET, Key=f"{READY_PREFIX}/{t}.parquet")
        df = pd.read_parquet(io.BytesIO(obj["Body"].read())).sort_values("timestamp")
        target = df["target_value"].astype(float).tolist()
        start  = df["timestamp"].iloc[0].replace("T", " ").replace("Z", "")

        # hold out the last PREDICTION_LENGTH days for the test channel
        train_records.append({"start": start, "target": target[:-PREDICTION_LENGTH]})
        test_records.append({"start": start, "target": target})
        summary.append({"ticker": t, "rows": len(df),
                         "from": df["timestamp"].iloc[0], "to": df["timestamp"].iloc[-1]})

    train_path = INPUT_DIR / "train.jsonl"
    test_path  = INPUT_DIR / "test.jsonl"
    train_path.write_text("\n".join(json.dumps(r) for r in train_records) + "\n")
    test_path.write_text("\n".join(json.dumps(r) for r in test_records) + "\n")

    train_key = f"{SM_PREFIX}/data/train.jsonl"
    test_key  = f"{SM_PREFIX}/data/test.jsonl"
    s3.upload_file(str(train_path), ARTIFACT_BUCKET, train_key)
    s3.upload_file(str(test_path),  ARTIFACT_BUCKET, test_key)

    for s in summary:
        print(f"  {s['ticker']:<10} {s['rows']:>5} daily rows   {s['from']} -> {s['to']}")

    state.update({
        "train_s3":  f"s3://{ARTIFACT_BUCKET}/{train_key}",
        "test_s3":   f"s3://{ARTIFACT_BUCKET}/{test_key}",
        "tickers":   TICKERS,
        "freq":      FREQ,
        "context":   CONTEXT_LENGTH,
        "horizon":   PREDICTION_LENGTH,
    })
    save_state(state)
    print(f"\nUploaded train/test to s3://{ARTIFACT_BUCKET}/{SM_PREFIX}/data/")
    return state


# train
def phase_train() -> dict:
    step("train - submit DeepAR training job (daily)")
    state = load_state()
    if "train_s3" not in state:
        sys.exit("run `prep` first")

    job_name = f"proj1102-deepar-daily-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    output_s3 = f"s3://{ARTIFACT_BUCKET}/{SM_PREFIX}/output"

    sm.create_training_job(
        TrainingJobName=job_name,
        AlgorithmSpecification={
            "TrainingImage": DEEPAR_IMAGE[REGION],
            "TrainingInputMode": "File",
        },
        RoleArn=ROLE_ARN,
        InputDataConfig=[
            {"ChannelName": "train",
             "DataSource": {"S3DataSource": {
                 "S3DataType": "S3Prefix", "S3Uri": state["train_s3"],
                 "S3DataDistributionType": "FullyReplicated"}},
             "ContentType": "json"},
            {"ChannelName": "test",
             "DataSource": {"S3DataSource": {
                 "S3DataType": "S3Prefix", "S3Uri": state["test_s3"],
                 "S3DataDistributionType": "FullyReplicated"}},
             "ContentType": "json"},
        ],
        OutputDataConfig={"S3OutputPath": output_s3},
        ResourceConfig={"InstanceType": TRAINING_INSTANCE,
                        "InstanceCount": 1, "VolumeSizeInGB": 10},
        StoppingCondition={"MaxRuntimeInSeconds": 3600},
        HyperParameters={
            "time_freq":               FREQ,
            "context_length":          str(CONTEXT_LENGTH),
            "prediction_length":       str(PREDICTION_LENGTH),
            "epochs":                  "100",
            "num_cells":               "40",
            "num_layers":              "2",
            "likelihood":              "student-T",
            "mini_batch_size":         "64",
            "learning_rate":           "1E-3",
            "dropout_rate":            "0.1",
            "early_stopping_patience": "10",
        },
    )
    state.update({
        "training_job_name": job_name,
        "training_image":    DEEPAR_IMAGE[REGION],
        "model_artifact":    f"{output_s3}/{job_name}/output/model.tar.gz",
    })
    save_state(state)
    print(f"Submitted {job_name}")
    return state


# deploy - serverless inference endpoint (separate from the hourly one)
def _wait_training(job_name: str) -> str:
    while True:
        r = sm.describe_training_job(TrainingJobName=job_name)
        st = r["TrainingJobStatus"]
        print(f"  [{datetime.now(timezone.utc):%H:%M:%S}] {st} / {r.get('SecondaryStatus','')}")
        if st in ("Completed", "Failed", "Stopped"):
            return st
        time.sleep(30)


def phase_deploy() -> dict:
    step("deploy - wait for training, then deploy serverless endpoint (daily)")
    state = load_state()
    job_name = state.get("training_job_name")
    if not job_name:
        sys.exit("run `train` first")

    print("Waiting for training to finish ...")
    if _wait_training(job_name) != "Completed":
        sys.exit("training did not complete")

    print("Creating SageMaker model ...")
    try:
        sm.delete_model(ModelName=MODEL_NAME)
    except sm.exceptions.ClientError:
        pass
    sm.create_model(
        ModelName=MODEL_NAME,
        PrimaryContainer={
            "Image": state["training_image"],
            "ModelDataUrl": state["model_artifact"],
        },
        ExecutionRoleArn=ROLE_ARN,
    )

    print("Creating serverless endpoint config ...")
    try:
        sm.delete_endpoint_config(EndpointConfigName=EP_CONFIG)
    except sm.exceptions.ClientError:
        pass
    sm.create_endpoint_config(
        EndpointConfigName=EP_CONFIG,
        ProductionVariants=[{
            "VariantName": "AllTraffic",
            "ModelName":   MODEL_NAME,
            "ServerlessConfig": {
                "MemorySizeInMB": SERVERLESS_MEMORY_MB,
                "MaxConcurrency": SERVERLESS_MAX_CONCUR,
            },
        }],
    )

    exists = False
    try:
        sm.describe_endpoint(EndpointName=EP_NAME)
        exists = True
    except sm.exceptions.ClientError:
        pass
    if exists:
        print("Updating existing endpoint ...")
        sm.update_endpoint(EndpointName=EP_NAME, EndpointConfigName=EP_CONFIG)
    else:
        print("Creating endpoint ...")
        sm.create_endpoint(EndpointName=EP_NAME, EndpointConfigName=EP_CONFIG)

    print("Waiting for endpoint InService ...")
    while True:
        r = sm.describe_endpoint(EndpointName=EP_NAME)
        st = r["EndpointStatus"]
        print(f"  [{datetime.now(timezone.utc):%H:%M:%S}] {st}")
        if st in ("InService", "Failed"):
            break
        time.sleep(30)
    if st != "InService":
        sys.exit(f"endpoint ended in {st}")

    state.update({"model_name": MODEL_NAME, "ep_config": EP_CONFIG, "endpoint": EP_NAME})
    save_state(state)
    print(f"\nServerless endpoint ready: {EP_NAME}")
    return state


def phase_status():
    s = load_state()
    print(json.dumps(s, indent=2) if s else "(no daily_state.json yet)")


def phase_all():
    phase_prep()
    phase_train()
    phase_deploy()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("phase", choices=["prep", "train", "deploy", "status", "all"])
    args = p.parse_args()
    {"prep": phase_prep, "train": phase_train, "deploy": phase_deploy,
     "status": phase_status, "all": phase_all}[args.phase]()


if __name__ == "__main__":
    main()
