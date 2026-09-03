"""
Shared pytest fixtures + import-time setup for proj1102 unit tests.

Our Lambda modules construct boto3 clients at import time, and they read
required configuration from environment variables. To exercise their pure
helper functions without spinning up AWS, we set fake env vars before any
Lambda module is imported. The boto3 clients are still constructed but
remain unused - no AWS calls are made.

Mirrors the lab 5 backend/tests/conftest.py pattern.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Provide every env var the Lambda modules look up at import time, so the
# from-import statements in the test files succeed.
_DEFAULTS = {
    "AWS_DEFAULT_REGION":   "eu-west-1",
    "APPREGION":            "eu-west-1",
    # forecast lambda
    "FORECAST_BUCKET":      "proj1102-data-test",
    "FORECAST_READY_PREFIX": "forecast-ready",
    "FORECAST_OUTPUT_PREFIX": "forecast-output",
    "DDB_TABLE":            "proj1102-forecasts-test",
    "SAGEMAKER_ENDPOINT":   "proj1102-deepar-endpoint-test",
    "TICKERS":              "TSLA,MSFT,NVDA",
    "CONTEXT_LENGTH":       "120",
    "PREDICTION_LENGTH":    "12",
    # forecast-daily lambda
    "DAILY_BUCKET":         "proj1102-data-test",
    "DAILY_READY_PREFIX":   "daily",
    "DAILY_OUTPUT_PREFIX":  "daily-forecast-output",
    "DATA_BUCKET":          "proj1102-data-test",
    "ARTIFACT_BUCKET":      "proj1102-artifacts-test",
    "ROLE_ARN":             "arn:aws:iam::000000000000:role/dummy",
    # ingestion lambda
    "PRICESQUEUE":          "https://sqs.eu-west-1.amazonaws.com/000000000000/prices-test",
    "NEWSQUEUE":            "https://sqs.eu-west-1.amazonaws.com/000000000000/news-test",
    "APIKEY1": "k1", "APIKEY2": "k2", "APIKEY3": "k3",
    "APIKEY4": "k4", "APIKEY5": "k5", "APIKEY6": "k6",
    "APIKEY7": "k7", "APIKEY8": "k8", "APIKEY9": "k9",
}
for k, v in _DEFAULTS.items():
    os.environ.setdefault(k, v)


# Add each Lambda directory to sys.path so `import lambda_function` resolves
# within that Lambda's source tree. We add lazily inside the test fixture
# below to avoid module-name collisions across Lambdas (they all define
# `lambda_function`).
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, file_path: Path):
    """Import a Lambda's `lambda_function.py` under a unique name."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(name, file_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def get_forecast_module():
    return _load_module(
        "forecast_lambda",
        PROJECT_ROOT / "lambda" / "forecast" / "lambda_function.py",
    )


def get_forecast_daily_module():
    return _load_module(
        "forecast_daily_lambda",
        PROJECT_ROOT / "lambda" / "forecast_daily" / "lambda_function.py",
    )


def get_ingestion_module():
    return _load_module(
        "ingestion_lambda",
        PROJECT_ROOT / "lambda" / "data_ingestion" / "lambda_function.py",
    )


def get_glue_module():
    # main() is guarded, so importing only loads the helpers.
    return _load_module(
        "glue_data_preparation",
        PROJECT_ROOT / "glue" / "data_preparation" / "job.py",
    )
