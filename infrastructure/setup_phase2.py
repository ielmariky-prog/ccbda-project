"""
Phase 2 setup - proj1102 pipeline.

What this script does:
  1. Uploads job.py to s3://proj1102-data/glue-scripts/
  2. Uploads config files to s3://proj1102-data/config/   (for reference / auditing)
  3. Creates or updates the Glue Python Shell job with default arguments
     read directly from config/pipeline.json["glue"]["job_arguments"]
  4. Deploys the glue-trigger Lambda
  5. Creates the EventBridge rule (hourly)

What this script does NOT do:
  - Create SQS queues - proj1102-prices-queue and proj1102-news-queue
    are pre-provisioned; their URLs live in config/pipeline.json
  - Upload processors.zip - the new job.py does its own parsing inline

Re-run at any time to push code changes (job.py, config).
"""

import io
import json
import os
import zipfile

import boto3
from botocore.exceptions import ClientError
from dotenv import dotenv_values

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config   = dotenv_values(os.path.join(ROOT_DIR, ".env"))

REGION = config["AWS_DEFAULT_REGION"]
TEAM   = config["TEAM"]

# Pipeline config is the single source of truth for bucket name and job args
with open(os.path.join(ROOT_DIR, "config", "pipeline.json")) as fh:
    PIPELINE = json.load(fh)

DATA_BUCKET    = PIPELINE["s3"]["bucket"]          # proj1102-data
GLUE_JOB_ARGS  = PIPELINE["glue"]["job_arguments"] # forwarded verbatim to Glue

GLUE_JOB_NAME  = f"{TEAM}-data-preparation"
LAMBDA_TRIGGER = f"{TEAM}-glue-trigger"
RULE_NAME      = f"{TEAM}-prepare-every-hour"
GLUE_ROLE_NAME = f"{TEAM}-glue-role"

s3_client     = boto3.client("s3",     region_name=REGION)
glue_client   = boto3.client("glue",   region_name=REGION)
iam_client    = boto3.client("iam")
lmb_client    = boto3.client("lambda", region_name=REGION)
events_client = boto3.client("events", region_name=REGION)


# Config upload  (reference copy in S3 - not read by job.py at runtime)

def upload_config() -> None:
    print("Uploading config files to S3 …")
    for fname in ("pipeline.json", "assets.json"):
        local = os.path.join(ROOT_DIR, "config", fname)
        key   = f"config/{fname}"
        s3_client.upload_file(local, DATA_BUCKET, key)
        print(f"  [OK] s3://{DATA_BUCKET}/{key}")


# Glue scripts

def upload_glue_scripts() -> str:
    """Upload job.py and return its S3 key. No processors zip needed."""
    print("Uploading Glue scripts …")
    script_key = "glue-scripts/data_preparation/job.py"
    s3_client.upload_file(
        os.path.join(ROOT_DIR, "glue", "data_preparation", "job.py"),
        DATA_BUCKET,
        script_key,
    )
    print(f"  [OK] s3://{DATA_BUCKET}/{script_key}")
    return script_key


# Glue IAM role

def get_glue_role() -> str:
    override = config.get("GLUE_ROLE_ARN", "").strip()
    if override:
        print(f"  Using GLUE_ROLE_ARN from .env: {override}")
        return override

    for candidate in [GLUE_ROLE_NAME, "AWSGlueServiceRole", "LabRole"]:
        try:
            arn = iam_client.get_role(RoleName=candidate)["Role"]["Arn"]
            print(f"  [OK] Using IAM role: {candidate}")
            return arn
        except iam_client.exceptions.NoSuchEntityException:
            pass

    print(f"Creating Glue IAM role: {GLUE_ROLE_NAME}")
    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect":    "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action":    "sts:AssumeRole",
        }],
    }
    role = iam_client.create_role(
        RoleName=GLUE_ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(trust),
    )
    for policy in [
        "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
        "arn:aws:iam::aws:policy/AmazonS3FullAccess",
        "arn:aws:iam::aws:policy/AmazonSQSFullAccess",
    ]:
        iam_client.attach_role_policy(RoleName=GLUE_ROLE_NAME, PolicyArn=policy)
    print(f"  [OK] Created: {role['Role']['Arn']}")
    return role["Role"]["Arn"]


# Glue job

def create_glue_job(role_arn: str, script_key: str) -> None:
    print(f"Creating / updating Glue job: {GLUE_JOB_NAME}")
    job_config = {
        "Role": role_arn,
        "Command": {
            "Name":           "pythonshell",
            "ScriptLocation": f"s3://{DATA_BUCKET}/{script_key}",
            "PythonVersion":  "3.9",
        },
        "DefaultArguments": {
            **GLUE_JOB_ARGS,
            "--TempDir": f"s3://{DATA_BUCKET}/glue-temp/",
        },
        "MaxCapacity": 0.0625,  # minimum Python Shell DPU (1/16)
        "Timeout":     30,
        "GlueVersion": "3.0",
    }

    try:
        glue_client.create_job(Name=GLUE_JOB_NAME, **job_config)
        print("  [OK] Job created")
    except (glue_client.exceptions.AlreadyExistsException, ClientError) as e:
        if isinstance(e, ClientError) and e.response["Error"]["Code"] not in (
            "AlreadyExistsException", "IdempotentParameterMismatchException"
        ):
            raise
        glue_client.update_job(JobName=GLUE_JOB_NAME, JobUpdate=job_config)
        print("  [OK] Job updated")


# Lambda: glue-trigger

def get_lambda_role() -> str:
    override = config.get("LAMBDA_ROLE_ARN", "").strip()
    if override:
        return override
    arn = iam_client.get_role(RoleName="lambda-function-role-python")["Role"]["Arn"]
    print(f"  [OK] Lambda role: {arn}")
    return arn


def grant_lambda_glue_permission(lambda_role_arn: str) -> None:
    role_name   = lambda_role_arn.split("/")[-1]
    account_id  = lambda_role_arn.split(":")[4]
    policy_name = f"{TEAM}-glue-start-job"
    policy_doc  = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect":   "Allow",
            "Action":   "glue:StartJobRun",
            "Resource": f"arn:aws:glue:{REGION}:{account_id}:job/{GLUE_JOB_NAME}",
        }],
    })
    iam_client.put_role_policy(
        RoleName=role_name,
        PolicyName=policy_name,
        PolicyDocument=policy_doc,
    )
    print(f"  [OK] Granted glue:StartJobRun on {GLUE_JOB_NAME} to {role_name}")


def deploy_glue_trigger(role_arn: str) -> str:
    print(f"Deploying Lambda: {LAMBDA_TRIGGER}")
    src_dir = os.path.join(ROOT_DIR, "lambda", "glue_trigger")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in os.listdir(src_dir):
            fpath = os.path.join(src_dir, fname)
            if os.path.isfile(fpath):
                zf.write(fpath, fname)
    code = buf.getvalue()

    env_vars = {"GLUE_JOB_NAME": GLUE_JOB_NAME}

    try:
        lmb_client.update_function_code(FunctionName=LAMBDA_TRIGGER, ZipFile=code)
        lmb_client.get_waiter("function_updated").wait(FunctionName=LAMBDA_TRIGGER)
        lmb_client.update_function_configuration(
            FunctionName=LAMBDA_TRIGGER,
            Environment={"Variables": env_vars},
        )
        arn = lmb_client.get_function(FunctionName=LAMBDA_TRIGGER)["Configuration"]["FunctionArn"]
        print(f"  [OK] Updated: {arn}")
        return arn
    except lmb_client.exceptions.ResourceNotFoundException:
        pass

    resp = lmb_client.create_function(
        FunctionName=LAMBDA_TRIGGER,
        Runtime="python3.12",
        Role=role_arn,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": code},
        Timeout=30,
        Environment={"Variables": env_vars},
        Description="Triggered by EventBridge hourly to start the Glue data-preparation job",
    )
    print(f"  [OK] Created: {resp['FunctionArn']}")
    return resp["FunctionArn"]


# EventBridge

def create_eventbridge_rule(trigger_arn: str) -> None:
    print(f"Creating EventBridge rule: {RULE_NAME}")
    try:
        rule = events_client.put_rule(
            Name=RULE_NAME,
            ScheduleExpression="rate(1 hour)",
            State="ENABLED",
        )
        try:
            lmb_client.add_permission(
                FunctionName=trigger_arn,
                StatementId=f"{RULE_NAME}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=rule["RuleArn"],
            )
        except lmb_client.exceptions.ResourceConflictException:
            pass
        events_client.put_targets(
            Rule=RULE_NAME,
            Targets=[{"Id": "1", "Arn": trigger_arn}],
        )
        print(f"  [OK] {LAMBDA_TRIGGER} triggered every hour")
    except ClientError as e:
        if e.response["Error"]["Code"] != "AccessDeniedException":
            raise
        print("  [SKIP] No EventBridge permission - create it manually:")
        print(f"    Name:     {RULE_NAME}")
        print(f"    Schedule: rate(1 hour)")
        print(f"    Target:   Lambda -> {LAMBDA_TRIGGER}")


# Main

if __name__ == "__main__":
    print("=== Phase 2 Setup ===\n")
    print(f"Data bucket    : s3://{DATA_BUCKET}")
    print(f"Glue job       : {GLUE_JOB_NAME}")
    print(f"Prices queue   : {PIPELINE['sources']['prices']['queue_url']}")
    print(f"Sentiment (S3) : s3://{DATA_BUCKET}/{PIPELINE['s3']['sentiment_prefix']}/<TICKER>.csv\n")

    upload_config()

    print("\nSetting up Glue …")
    glue_role_arn = get_glue_role()
    script_key    = upload_glue_scripts()
    create_glue_job(glue_role_arn, script_key)

    print("\nSetting up Lambda + EventBridge …")
    lambda_role_arn = get_lambda_role()
    grant_lambda_glue_permission(lambda_role_arn)
    trigger_arn     = deploy_glue_trigger(lambda_role_arn)
    create_eventbridge_rule(trigger_arn)

    print("\n=== Phase 2 setup complete ===")
    print("\nTo test the pipeline end-to-end:")
    print(f"  python scripts/send_test_messages.py")
    print(f"  aws lambda invoke --function-name {LAMBDA_TRIGGER} --payload '{{}}' /dev/null")
    print(f"  aws s3 ls s3://{DATA_BUCKET}/{PIPELINE['s3']['processed_prefix']}/ --recursive")
