"""
Weekly daily-model retrain Lambda - proj1102 strategic horizon.

Sibling of `proj1102-retrain` but for the *daily* DeepAR model. Reads the
live daily parquets from the team bucket, retrains the daily DeepAR on
the current 5-year window, and updates the serverless endpoint in place.
Runs on a Sunday cron offset 1h after the hourly retrain so they don't
overlap on the same SageMaker quota.

Training ~3-5 min, endpoint update ~2-3 min - fits inside the 15-min
Lambda ceiling. ~$0.02 per run on `ml.c5.xlarge`.

Required env vars:
    ARTIFACT_BUCKET    where to land train.jsonl + model output
    ROLE_ARN           SageMaker execution role (ProjectForecastS3Role)
    APPREGION          eu-west-1
Optional:
    DATA_BUCKET        proj1102-data
    READY_PREFIX       daily
    TICKERS            comma-separated, default TSLA,MSFT,NVDA
"""

from __future__ import annotations

import io
import json
import os
import time
from datetime import datetime, timezone

import boto3
import pandas as pd

REGION              = os.environ.get("APPREGION", "eu-west-1")
DATA_BUCKET         = os.environ.get("DATA_BUCKET",  "proj1102-data")
READY_PREFIX        = os.environ.get("READY_PREFIX", "daily")
ARTIFACT_BUCKET     = os.environ["ARTIFACT_BUCKET"]
TICKERS             = os.environ.get("TICKERS", "TSLA,MSFT,NVDA").split(",")
ROLE_ARN            = os.environ["ROLE_ARN"]

SM_PREFIX           = "deepar-daily"
FREQ                = "D"
CONTEXT_LENGTH      = 60
PREDICTION_LENGTH   = 7
TRAINING_INSTANCE   = "ml.c5.xlarge"
DEEPAR_IMAGE        = "224300973850.dkr.ecr.eu-west-1.amazonaws.com/forecasting-deepar:1"

MODEL_NAME            = "proj1102-deepar-daily-model"
EP_CONFIG             = "proj1102-deepar-daily-config"
EP_NAME               = "proj1102-deepar-daily-endpoint"
SERVERLESS_MEMORY_MB  = 3072
SERVERLESS_MAX_CONCUR = 5

WAIT_TRAINING_MAX_S   = 600

s3 = boto3.client("s3",        region_name=REGION)
sm = boto3.client("sagemaker", region_name=REGION)


def _prep() -> tuple[str, str]:
    train_records, test_records = [], []
    for t in TICKERS:
        obj = s3.get_object(Bucket=DATA_BUCKET, Key=f"{READY_PREFIX}/{t}.parquet")
        df  = pd.read_parquet(io.BytesIO(obj["Body"].read())).sort_values("timestamp")
        target = df["target_value"].astype(float).tolist()
        start  = df["timestamp"].iloc[0].replace("T", " ").replace("Z", "")
        train_records.append({"start": start, "target": target[:-PREDICTION_LENGTH]})
        test_records.append({"start": start, "target": target})
        print(f"  [prep] {t}: {len(df)} daily rows, train_n={len(target) - PREDICTION_LENGTH}")

    train_key = f"{SM_PREFIX}/data/train.jsonl"
    test_key  = f"{SM_PREFIX}/data/test.jsonl"
    s3.put_object(Bucket=ARTIFACT_BUCKET, Key=train_key,
                  Body=("\n".join(json.dumps(r) for r in train_records) + "\n").encode())
    s3.put_object(Bucket=ARTIFACT_BUCKET, Key=test_key,
                  Body=("\n".join(json.dumps(r) for r in test_records) + "\n").encode())
    return f"s3://{ARTIFACT_BUCKET}/{train_key}", f"s3://{ARTIFACT_BUCKET}/{test_key}"


def _train(train_s3: str, test_s3: str) -> tuple[str, str]:
    job = f"proj1102-deepar-daily-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
    output_s3 = f"s3://{ARTIFACT_BUCKET}/{SM_PREFIX}/output"
    sm.create_training_job(
        TrainingJobName=job,
        AlgorithmSpecification={"TrainingImage": DEEPAR_IMAGE, "TrainingInputMode": "File"},
        RoleArn=ROLE_ARN,
        InputDataConfig=[
            {"ChannelName": "train",
             "DataSource": {"S3DataSource": {
                 "S3DataType": "S3Prefix", "S3Uri": train_s3,
                 "S3DataDistributionType": "FullyReplicated"}},
             "ContentType": "json"},
            {"ChannelName": "test",
             "DataSource": {"S3DataSource": {
                 "S3DataType": "S3Prefix", "S3Uri": test_s3,
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
    print(f"  [train] submitted {job}")
    return job, f"{output_s3}/{job}/output/model.tar.gz"


def _wait_training(job: str) -> tuple[str, dict | None]:
    deadline = time.time() + WAIT_TRAINING_MAX_S
    while time.time() < deadline:
        r  = sm.describe_training_job(TrainingJobName=job)
        st = r["TrainingJobStatus"]
        if st in ("Completed", "Failed", "Stopped"):
            print(f"  [wait] {job}: {st}")
            return st, r
        time.sleep(20)
    return "Timeout", None


def _deploy(model_artifact: str) -> None:
    for fn, kw in [
        (sm.delete_model,           dict(ModelName=MODEL_NAME)),
        (sm.delete_endpoint_config, dict(EndpointConfigName=EP_CONFIG)),
    ]:
        try:
            fn(**kw)
        except Exception:
            pass

    sm.create_model(
        ModelName=MODEL_NAME,
        PrimaryContainer={"Image": DEEPAR_IMAGE, "ModelDataUrl": model_artifact},
        ExecutionRoleArn=ROLE_ARN,
    )
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
    try:
        sm.describe_endpoint(EndpointName=EP_NAME)
        sm.update_endpoint(EndpointName=EP_NAME, EndpointConfigName=EP_CONFIG)
        print(f"  [deploy] endpoint update kicked off")
    except sm.exceptions.ClientError:
        sm.create_endpoint(EndpointName=EP_NAME, EndpointConfigName=EP_CONFIG)
        print(f"  [deploy] endpoint create kicked off")


def lambda_handler(event, context):
    print(f"=== weekly DAILY retrain - tickers={TICKERS} ===")

    train_s3, test_s3 = _prep()
    job, artifact     = _train(train_s3, test_s3)
    status, info      = _wait_training(job)

    if status != "Completed":
        reason = (info or {}).get("FailureReason") if status != "Timeout" else "exceeded Lambda wait window"
        msg = f"training ended {status}: {reason}"
        print(f"[ERROR] {msg}")
        return {"statusCode": 500, "status": status, "job": job, "message": msg}

    _deploy(artifact)
    metrics = {m["MetricName"]: m["Value"] for m in info.get("FinalMetricDataList", [])}
    print(f"[OK] retrained {job}  wQL={metrics.get('test:mean_wQuantileLoss')}")

    return {
        "statusCode": 200,
        "status":     "ok",
        "job":        job,
        "artifact":   artifact,
        "metrics":    metrics,
    }
