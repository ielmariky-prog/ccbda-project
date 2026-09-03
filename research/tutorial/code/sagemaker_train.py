"""
Programmatic SageMaker training & inference example.

Goal: same workflow as Canvas (upload data -> train -> predict),
but driven entirely by Python code so it can run inside a Lambda.

This is the snippet behind slide 8 of the research presentation.
"""

import boto3
import sagemaker
from sagemaker.image_uris import retrieve

# Configuration
session = sagemaker.Session()
region = session.boto_region_name              # e.g. "eu-west-1"
role = sagemaker.get_execution_role()           # SageMaker service role
bucket = "proj1102-ralph-forecast-148557232117" # our S3 bucket
prefix = "deepar"                               # folder inside the bucket

# 1) Upload training data to S3 (DeepAR wants JSON Lines, not CSV)
train_data_s3 = session.upload_data(
    path="train.jsonl",
    bucket=bucket,
    key_prefix=f"{prefix}/data",
)

# 2) Get the AWS-maintained DeepAR container image for our region
image_uri = retrieve(framework="forecasting-deepar", region=region)

# 3) Define the training job
estimator = sagemaker.estimator.Estimator(
    image_uri=image_uri,
    role=role,
    instance_count=1,
    instance_type="ml.c5.xlarge",
    output_path=f"s3://{bucket}/{prefix}/output",
    sagemaker_session=session,
)

estimator.set_hyperparameters(
    time_freq="5min",            # data granularity
    context_length=100,          # how much history the model sees
    prediction_length=12,        # how far ahead to forecast (1 hour)
    epochs=50,
    num_cells=40,
    num_layers=2,
)

# 4) Train (~10-20 min for our dataset size)
estimator.fit({"train": train_data_s3})

# 5) Deploy as a real-time endpoint (so a Lambda can call it later)
predictor = estimator.deploy(
    initial_instance_count=1,
    instance_type="ml.t2.medium",
)

# 6) Call the endpoint with a real input
response = predictor.predict(
    {
        "instances": [{
            "start": "2026-04-18T10:00:00",
            "target": [182.04, 181.63, 181.51, ...]   # historical AAPL prices
        }],
        "configuration": {
            "num_samples": 100,
            "output_types": ["quantiles"],
            "quantiles": ["0.1", "0.5", "0.9"],
        }
    }
)

print(response)
# {
#   "predictions": [{
#     "quantiles": {
#       "0.1": [183.05, 182.95, ...],   # P10 - pessimistic
#       "0.5": [183.35, 183.35, ...],   # P50 - most likely
#       "0.9": [183.64, 183.75, ...],   # P90 - optimistic
#     }
#   }]
# }
