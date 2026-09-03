"""Pipeline health probe. Queries the live AWS state and prints whether every
component (Lambdas, endpoints, tables, alarms, parquet ages) is healthy.
Read-only, safe to run any time.

Usage:
    python3 housekeeping/check_pipeline.py
    PROFILE=ralph REGION=eu-west-1 python3 housekeeping/check_pipeline.py
"""

from __future__ import annotations

import io
import os
import sys
from datetime import datetime, timezone

import boto3

PROFILE = os.environ.get("PROFILE", "ralph")
REGION  = os.environ.get("REGION",  "eu-west-1")
ACCOUNT = "148557232117"
TICKERS = ["TSLA", "MSFT", "NVDA"]

session = boto3.Session(profile_name=PROFILE, region_name=REGION)
lam     = session.client("lambda")
sfn     = session.client("stepfunctions")
sm      = session.client("sagemaker")
ddb     = session.client("dynamodb")
cw      = session.client("cloudwatch")
events  = session.client("events")
s3      = session.client("s3")


GREEN = "\033[92m"
RED   = "\033[91m"
YELLOW = "\033[93m"
DIM   = "\033[2m"
END   = "\033[0m"


def ok(line: str) -> None:
    print(f"  {GREEN}OK{END}    {line}")


def warn(line: str) -> None:
    print(f"  {YELLOW}WARN{END}  {line}")


def bad(line: str) -> None:
    print(f"  {RED}FAIL{END}  {line}")


def section(title: str) -> None:
    print(f"\n{DIM}── {title} ──{END}")



def check_lambdas() -> bool:
    section("Lambda functions")
    expected = [
        "proj1102-sentiment",
        "proj1102-forecast",
        "proj1102-quicksight-refresh",
        "proj1102-retrain",
        "proj1102-freshness",
        "proj1102-daily-ingest",
        "proj1102-daily-forecast",
        "proj1102-daily-retrain",
    ]
    all_ok = True
    for fn in expected:
        try:
            r = lam.get_function_configuration(FunctionName=fn)
            state = r["State"]
            if state == "Active":
                ok(f"{fn} ({r['Runtime']})")
            else:
                warn(f"{fn} state={state}")
                all_ok = False
        except Exception as e:
            bad(f"{fn}: {e.__class__.__name__}")
            all_ok = False
    return all_ok


def check_state_machines() -> bool:
    section("Step Functions state machines")
    expected = ["proj1102-pipeline", "proj1102-daily-pipeline"]
    all_ok = True
    for name in expected:
        arn = f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:{name}"
        try:
            sfn.describe_state_machine(stateMachineArn=arn)
            execs = sfn.list_executions(stateMachineArn=arn, maxResults=1).get("executions", [])
            last = execs[0]["status"] if execs else "no runs yet"
            ok(f"{name}  last execution: {last}")
        except Exception as e:
            bad(f"{name}: {e.__class__.__name__}")
            all_ok = False
    return all_ok


def check_endpoints() -> bool:
    section("SageMaker serverless endpoints")
    all_ok = True
    for ep in ("proj1102-deepar-endpoint", "proj1102-deepar-daily-endpoint"):
        try:
            r = sm.describe_endpoint(EndpointName=ep)
            if r["EndpointStatus"] == "InService":
                ok(f"{ep}")
            else:
                warn(f"{ep} status={r['EndpointStatus']}")
                all_ok = False
        except Exception as e:
            bad(f"{ep}: {e.__class__.__name__}")
            all_ok = False
    return all_ok


def check_tables() -> bool:
    section("DynamoDB tables")
    all_ok = True
    for name in ("proj1102-forecasts", "proj1102-daily-forecasts"):
        try:
            r = ddb.describe_table(TableName=name)
            if r["Table"]["TableStatus"] == "ACTIVE":
                ok(f"{name}")
            else:
                warn(f"{name} status={r['Table']['TableStatus']}")
                all_ok = False
        except Exception as e:
            bad(f"{name}: {e.__class__.__name__}")
            all_ok = False
    return all_ok


def check_alarms() -> bool:
    section("CloudWatch alarms")
    r = cw.describe_alarms(AlarmNamePrefix="proj1102-")
    all_ok = True
    for a in r["MetricAlarms"]:
        state = a["StateValue"]
        if state == "OK":
            ok(f"{a['AlarmName']}")
        elif state == "INSUFFICIENT_DATA":
            warn(f"{a['AlarmName']}  INSUFFICIENT_DATA (likely just hasn't fired yet)")
        else:
            bad(f"{a['AlarmName']}  state={state}")
            all_ok = False
    return all_ok


def check_parquet_ages() -> bool:
    section("Parquet freshness")
    now = datetime.now(timezone.utc)
    import pandas as pd
    all_ok = True
    for prefix, label, soft_h in [("forecast-ready", "hourly", 2),
                                   ("daily",          "daily",  120)]:
        for t in TICKERS:
            try:
                obj = s3.get_object(Bucket="proj1102-data", Key=f"{prefix}/{t}.parquet")
                df  = pd.read_parquet(io.BytesIO(obj["Body"].read()))
                last_str = df["timestamp"].max()
                last = datetime.fromisoformat(last_str.replace("Z", "+00:00"))
                age_h = (now - last).total_seconds() / 3600
                line = f"{prefix}/{t}.parquet  last={last_str}  age={age_h:.1f}h"
                if age_h > soft_h:
                    warn(line + f"  (>{soft_h}h threshold for {label})")
                    all_ok = False
                else:
                    ok(line)
            except Exception as e:
                bad(f"{prefix}/{t}: {e.__class__.__name__}: {e}")
                all_ok = False
    return all_ok


def main() -> int:
    print(f"{DIM}proj1102 health probe - profile={PROFILE} region={REGION}{END}")

    checks = [
        check_lambdas,
        check_state_machines,
        check_endpoints,
        check_tables,
        check_alarms,
        check_parquet_ages,
    ]
    results = [c() for c in checks]

    print()
    if all(results):
        print(f"{GREEN}All checks passed.{END}")
        return 0
    failures = sum(1 for r in results if not r)
    print(f"{RED}{failures} check(s) failed or warned.{END}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
