"""Create the proj1102-forecasts DynamoDB table and attach the cross-account
read policies (team account on the table, Arthur's account on the stream).
Run once with the `ralph` profile."""

import json
import sys
import boto3
from botocore.exceptions import ClientError

PROFILE      = "ralph"
REGION       = "eu-west-1"
TABLE        = "proj1102-forecasts"
TEAM_ACCOUNT      = "834922934600"   # team sandbox (legacy table read)
ARTHUR_ACCOUNT    = "162655485124"   # Arthur's account, runs the anomaly Lambda

session = boto3.Session(profile_name=PROFILE, region_name=REGION)
ddb     = session.client("dynamodb")


def create() -> None:
    try:
        ddb.describe_table(TableName=TABLE)
        print(f"Table {TABLE} already exists - nothing to do.")
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    print(f"Creating table {TABLE} ...")
    ddb.create_table(
        TableName=TABLE,
        KeySchema=[
            {"AttributeName": "ticker",    "KeyType": "HASH"},   # partition key
            {"AttributeName": "timestamp", "KeyType": "RANGE"},  # sort key
        ],
        AttributeDefinitions=[
            {"AttributeName": "ticker",    "AttributeType": "S"},
            {"AttributeName": "timestamp", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
        Tags=[
            {"Key": "project", "Value": "proj1102"},
            {"Key": "owner",   "Value": "ralph"},
        ],
    )
    print("Waiting for table to become ACTIVE ...")
    ddb.get_waiter("table_exists").wait(TableName=TABLE)
    print(f"Table {TABLE} is ACTIVE.")


def add_cross_account_read_policy() -> None:
    # DynamoDB needs separate resource policies for the table and the stream.
    desc       = ddb.describe_table(TableName=TABLE)["Table"]
    table_arn  = desc["TableArn"]
    stream_arn = desc.get("LatestStreamArn")

    # table read for the team account
    table_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "TeamTableRead",
            "Effect": "Allow",
            "Principal": {"AWS": f"arn:aws:iam::{TEAM_ACCOUNT}:root"},
            "Action": [
                "dynamodb:GetItem",
                "dynamodb:BatchGetItem",
                "dynamodb:Query",
                "dynamodb:Scan",
                "dynamodb:DescribeTable",
            ],
            "Resource": [table_arn, f"{table_arn}/index/*"],
        }],
    }
    ddb.put_resource_policy(ResourceArn=table_arn, Policy=json.dumps(table_policy))
    print(f"Attached table read policy for account {TEAM_ACCOUNT}.")

    if not stream_arn:
        print("No stream enabled - skipping stream policy.")
        return

    # stream read for Arthur's account (his anomaly Lambda subscribes to it)
    stream_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "ArthurStreamRead",
            "Effect": "Allow",
            "Principal": {"AWS": f"arn:aws:iam::{ARTHUR_ACCOUNT}:root"},
            "Action": [
                "dynamodb:DescribeStream",
                "dynamodb:GetRecords",
                "dynamodb:GetShardIterator",
            ],
            "Resource": [stream_arn],
        }],
    }
    ddb.put_resource_policy(ResourceArn=stream_arn, Policy=json.dumps(stream_policy))
    print(f"Attached stream read policy for account {ARTHUR_ACCOUNT}.")


if __name__ == "__main__":
    try:
        create()
        add_cross_account_read_policy()
    except ClientError as e:
        sys.exit(f"Failed: {e}")
