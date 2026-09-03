"""
Creates all AWS resources for Phase 1:
  - S3 bucket (raw data storage)
  - IAM role for Lambda
  - Lambda function (data ingestion -> writes directly to S3)
  - EventBridge rule (trigger every 5 minutes)
"""

import boto3
import os
import zipfile
from dotenv import dotenv_values

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config = dotenv_values(os.path.join(ROOT_DIR, ".env"))

REGION      = config["AWS_DEFAULT_REGION"]
TEAM        = config["TEAM"]
S3_BUCKET   = config["S3_BUCKET_NAME"]
LAMBDA_NAME = f"{TEAM}-data-ingestion"
ROLE_NAME   = f"{TEAM}-lambda-role"
RULE_NAME   = f"{TEAM}-ingest-every-5min"
ALPHA_KEY   = config["ALPHA_VANTAGE_API_KEY"]

s3     = boto3.client("s3",     region_name=REGION)
iam    = boto3.client("iam")
lmb    = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)


def create_s3_bucket():
    print(f"Creating S3 bucket: {S3_BUCKET}")
    try:
        s3.create_bucket(
            Bucket=S3_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
        print(f"  [OK] Bucket {S3_BUCKET} created")
    except Exception as e:
        if any(x in str(e) for x in ["BucketAlreadyOwnedByYou", "BucketAlreadyExists", "AccessDenied"]):
            print(f"  [SKIP] Bucket already exists or created manually - continuing")
        else:
            raise


def get_lab_role():
    print("Fetching existing Lambda role...")
    role_arn = iam.get_role(RoleName="lambda-function-role-python")["Role"]["Arn"]
    print(f"  [OK] Using role: {role_arn}")
    return role_arn


def zip_lambda():
    zip_path = os.path.join(ROOT_DIR, "data_ingestion.zip")
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(
            os.path.join(ROOT_DIR, "lambda", "data_ingestion", "lambda_function.py"),
            "lambda_function.py",
        )
    return zip_path


def create_lambda(role_arn):
    print(f"Deploying Lambda: {LAMBDA_NAME}")
    zip_path = zip_lambda()
    with open(zip_path, "rb") as f:
        code = f.read()

    env_vars = {
        "ALPHA_VANTAGE_API_KEY": ALPHA_KEY,
        "S3_BUCKET_NAME":        S3_BUCKET,
        "AWS_DEFAULT_REGION":    REGION,
        "ASSETS":                '["AAPL"]',
    }

    try:
        response = lmb.create_function(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": code},
            Timeout=30,
            Environment={"Variables": env_vars},
        )
        lambda_arn = response["FunctionArn"]
        print(f"  [OK] Lambda created: {lambda_arn}")
    except lmb.exceptions.ResourceConflictException:
        lmb.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=code)
        lambda_arn = lmb.get_function(FunctionName=LAMBDA_NAME)["Configuration"]["FunctionArn"]
        print(f"  [SKIP] Lambda updated: {lambda_arn}")

    return lambda_arn


def create_eventbridge_rule(lambda_arn):
    print(f"Creating EventBridge rule: {RULE_NAME}")
    rule = events.put_rule(
        Name=RULE_NAME,
        ScheduleExpression="rate(5 minutes)",
        State="ENABLED",
    )
    rule_arn = rule["RuleArn"]

    try:
        lmb.add_permission(
            FunctionName=LAMBDA_NAME,
            StatementId=f"{RULE_NAME}-invoke",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=rule_arn,
        )
    except lmb.exceptions.ResourceConflictException:
        pass

    events.put_targets(
        Rule=RULE_NAME,
        Targets=[{"Id": "1", "Arn": lambda_arn}],
    )
    print(f"  [OK] EventBridge triggers {LAMBDA_NAME} every 5 minutes")


if __name__ == "__main__":
    create_s3_bucket()
    role_arn   = get_lab_role()
    lambda_arn = create_lambda(role_arn)
    create_eventbridge_rule(lambda_arn)
    print("\nPhase 1 setup complete.")
