"""
Train DeepAR and deploy a serverless inference endpoint - RALPH'S ACCOUNT (148557232117).

Runs in Ralph's own account because the team sandbox account has tightly-scoped
permissions (the deployer user can't even read forecast-ready/). Ralph's account
has full SageMaker + can read the team's forecast-ready/ parquets cross-account.

This is the model behind the forecast Lambda. Run it once to bootstrap, and
re-run it to retrain on fresher data (the endpoint is updated in place).

Why serverless inference?  A real-time endpoint bills ~$43/month even idle.
Serverless inference scales to zero - we pay only for the ~1 second per cycle
when the forecast Lambda calls it. Perfect for a 3-hourly forecast.

Phases:
    python train_and_deploy.py prep      # forecast-ready parquets -> DeepAR JSON Lines
    python train_and_deploy.py train     # submit the DeepAR training job
    python train_and_deploy.py deploy    # wait for training, deploy serverless endpoint
    python train_and_deploy.py status    # print state.json
    python train_and_deploy.py all       # prep -> train -> wait -> deploy, in one go

State persists to sagemaker/team_state.json.
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

# Read the team's clean data cross-account; keep our training artefacts in
# Ralph's own bucket.
DATA_BUCKET   = "proj1102-data"                                 # team bucket (read)
READY_PREFIX  = "forecast-ready"
ARTIFACT_BUCKET = f"proj1102-ralph-forecast-{ACCOUNT_ID}"       # my bucket (read/write)
SM_PREFIX     = "deepar"
TICKERS       = ["TSLA", "MSFT", "NVDA"]

ROLE_ARN      = f"arn:aws:iam::{ACCOUNT_ID}:role/ProjectForecastS3Role"

FREQ                = "5min"
CONTEXT_LENGTH      = 120     # 10 h of history
PREDICTION_LENGTH   = 12      # 1 h ahead

TRAINING_INSTANCE   = "ml.c5.xlarge"
DEEPAR_IMAGE = {
    "eu-west-1": "224300973850.dkr.ecr.eu-west-1.amazonaws.com/forecasting-deepar:1",
}

MODEL_NAME    = "proj1102-deepar-model"
EP_CONFIG     = "proj1102-deepar-config"
EP_NAME       = "proj1102-deepar-endpoint"

# Serverless inference sizing - account quota caps memory at 3072 MB;
# DeepAR inference is lightweight so this is plenty.
SERVERLESS_MEMORY_MB   = 3072
SERVERLESS_MAX_CONCUR  = 5

LOCAL_DIR  = Path(__file__).parent
INPUT_DIR  = LOCAL_DIR / "team_input"
STATE_FILE = LOCAL_DIR / "team_state.json"

session = boto3.Session(profile_name=PROFILE, region_name=REGION)
s3  = session.client("s3")
sm  = session.client("sagemaker")


def step(msg): print(f"\n=== {msg} ===")
def load_state(): return json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}
def save_state(s): STATE_FILE.write_text(json.dumps(s, indent=2))


# prep
def phase_prep() -> dict:
    step("prep - forecast-ready parquets -> DeepAR JSON Lines")
    INPUT_DIR.mkdir(exist_ok=True)
    state = load_state()

    train_records, test_records, summary = [], [], []
    for t in TICKERS:
        obj = s3.get_object(Bucket=DATA_BUCKET, Key=f"{READY_PREFIX}/{t}.parquet")
        df = pd.read_parquet(io.BytesIO(obj["Body"].read())).sort_values("timestamp")
        target    = df["target_value"].astype(float).tolist()
        # Sentiment as a dynamic_feat related time series. ffill carries the
        # last known headline forward; 0 (neutral) is the default before any
        # news exists. Same shape as target.
        sentiment = df["sentiment_score"].ffill().fillna(0).astype(float).tolist()
        start     = df["timestamp"].iloc[0].replace("T", " ").replace("Z", "")

        # hold out the last PREDICTION_LENGTH points for the test channel
        train_records.append({
            "start":        start,
            "target":       target[:-PREDICTION_LENGTH],
            "dynamic_feat": [sentiment[:-PREDICTION_LENGTH]],
        })
        test_records.append({
            "start":        start,
            "target":       target,
            "dynamic_feat": [sentiment],
        })
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
        print(f"  {s['ticker']:<10} {s['rows']:>5} rows   {s['from']} -> {s['to']}")

    state.update({
        "train_s3": f"s3://{ARTIFACT_BUCKET}/{train_key}",
        "test_s3":  f"s3://{ARTIFACT_BUCKET}/{test_key}",
        "tickers":  TICKERS,
    })
    save_state(state)
    print(f"\nUploaded train/test to s3://{ARTIFACT_BUCKET}/{SM_PREFIX}/data/")
    return state


# train
def phase_train() -> dict:
    step("train - submit DeepAR training job")
    state = load_state()
    if "train_s3" not in state:
        sys.exit("run `prep` first")

    job_name = f"proj1102-deepar-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
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
            "epochs":                  "50",
            "num_cells":               "40",
            "num_layers":              "2",
            "likelihood":              "gaussian",
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


# deploy - serverless inference endpoint
def _wait_training(job_name: str) -> str:
    while True:
        r = sm.describe_training_job(TrainingJobName=job_name)
        st = r["TrainingJobStatus"]
        print(f"  [{datetime.now(timezone.utc):%H:%M:%S}] {st} / {r.get('SecondaryStatus','')}")
        if st in ("Completed", "Failed", "Stopped"):
            return st
        time.sleep(30)


def phase_deploy() -> dict:
    step("deploy - wait for training, then deploy serverless endpoint")
    state = load_state()
    job_name = state.get("training_job_name")
    if not job_name:
        sys.exit("run `train` first")

    print("Waiting for training to finish ...")
    if _wait_training(job_name) != "Completed":
        sys.exit("training did not complete")

    # SageMaker model
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

    # serverless endpoint config
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

    # endpoint (create or update)
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

    print("Waiting for endpoint InService (serverless ~3-5 min) ...")
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
    print(json.dumps(s, indent=2) if s else "(no team_state.json yet)")


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
