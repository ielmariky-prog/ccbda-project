"""
Thin Lambda that starts the Glue data-preparation job.
Triggered by EventBridge on schedule; can also be invoked manually.
"""

import os
import boto3

GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]


def lambda_handler(event: dict, context) -> dict:
    glue     = boto3.client("glue")
    response = glue.start_job_run(JobName=GLUE_JOB_NAME)
    run_id   = response["JobRunId"]
    print(f"Started Glue job '{GLUE_JOB_NAME}', run ID: {run_id}")
    return {"JobRunId": run_id}
