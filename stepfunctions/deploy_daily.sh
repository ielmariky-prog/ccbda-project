#!/usr/bin/env bash
# Deploy the daily Step Functions orchestration - proj1102 (Ralph's account).
#
# Sister of deploy.sh (which deploys the hourly pipeline). Replaces:
#   proj1102-daily-ingest-schedule    (cron 0 20 ? * MON-FRI *)
#   proj1102-daily-forecast-schedule  (cron 15 20 ? * MON-FRI *)
# with a single state machine triggered once a weekday at 20:00 UTC.
#
# Idempotent - re-run to update the state machine definition + schedule
# + assert the cutover.

set -euo pipefail

PROFILE="ralph"
REGION="eu-west-1"
MY_ACCOUNT="148557232117"

SM_NAME="proj1102-daily-pipeline"
SM_ROLE_NAME="proj1102-daily-pipeline-sfn-role"
SM_ROLE_ARN="arn:aws:iam::${MY_ACCOUNT}:role/${SM_ROLE_NAME}"

EVENT_ROLE_NAME="proj1102-daily-pipeline-events-role"
EVENT_ROLE_ARN="arn:aws:iam::${MY_ACCOUNT}:role/${EVENT_ROLE_NAME}"

SCHEDULE_RULE="proj1102-daily-pipeline-schedule"
SCHEDULE_EXPR="cron(0 20 ? * MON-FRI *)"

INFRA_ALARMS_TOPIC="arn:aws:sns:${REGION}:${MY_ACCOUNT}:proj1102-infra-alarms"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ASL_FILE="${HERE}/proj1102-daily-pipeline.asl.json"

LOG_GROUP="/aws/vendedlogs/states/${SM_NAME}"
LOG_GROUP_ARN="arn:aws:logs:${REGION}:${MY_ACCOUNT}:log-group:${LOG_GROUP}:*"

ensure_sm_role() {
  if aws iam get-role --role-name "$SM_ROLE_NAME" --profile "$PROFILE" >/dev/null 2>&1; then
    echo "[OK] $SM_ROLE_NAME exists"
  else
    echo "[..] creating $SM_ROLE_NAME"
    TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"states.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
    aws iam create-role --role-name "$SM_ROLE_NAME" \
      --assume-role-policy-document "$TRUST" \
      --description "Execution role for the proj1102-daily-pipeline state machine" \
      --profile "$PROFILE" >/dev/null
  fi
  POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": [
        "arn:aws:lambda:${REGION}:${MY_ACCOUNT}:function:proj1102-daily-ingest",
        "arn:aws:lambda:${REGION}:${MY_ACCOUNT}:function:proj1102-daily-forecast"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "sns:Publish",
      "Resource": "${INFRA_ALARMS_TOPIC}"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery",
        "logs:DeleteLogDelivery", "logs:ListLogDeliveries",
        "logs:PutResourcePolicy", "logs:DescribeResourcePolicies", "logs:DescribeLogGroups"
      ],
      "Resource": "*"
    }
  ]
}
EOF
)
  aws iam put-role-policy --role-name "$SM_ROLE_NAME" \
    --policy-name "proj1102-daily-pipeline-sfn-inline" \
    --policy-document "$POLICY" --profile "$PROFILE"
  aws iam tag-role --role-name "$SM_ROLE_NAME" --tags \
    "Key=project,Value=proj1102" "Key=owner,Value=ralph" "Key=horizon,Value=daily" \
    --profile "$PROFILE" >/dev/null 2>&1 || true
  echo "[OK] $SM_ROLE_NAME inline policy attached"
}

ensure_event_role() {
  if aws iam get-role --role-name "$EVENT_ROLE_NAME" --profile "$PROFILE" >/dev/null 2>&1; then
    echo "[OK] $EVENT_ROLE_NAME exists"
  else
    echo "[..] creating $EVENT_ROLE_NAME"
    TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"events.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
    aws iam create-role --role-name "$EVENT_ROLE_NAME" \
      --assume-role-policy-document "$TRUST" \
      --description "EventBridge role to start the proj1102-daily-pipeline state machine" \
      --profile "$PROFILE" >/dev/null
  fi
  SM_ARN="arn:aws:states:${REGION}:${MY_ACCOUNT}:stateMachine:${SM_NAME}"
  POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "states:StartExecution",
    "Resource": "${SM_ARN}"
  }]
}
EOF
)
  aws iam put-role-policy --role-name "$EVENT_ROLE_NAME" \
    --policy-name "proj1102-daily-pipeline-events-inline" \
    --policy-document "$POLICY" --profile "$PROFILE"
  aws iam tag-role --role-name "$EVENT_ROLE_NAME" --tags \
    "Key=project,Value=proj1102" "Key=owner,Value=ralph" "Key=horizon,Value=daily" \
    --profile "$PROFILE" >/dev/null 2>&1 || true
  echo "[OK] $EVENT_ROLE_NAME inline policy attached"
}

ensure_log_group() {
  if aws logs describe-log-groups --log-group-name-prefix "$LOG_GROUP" \
       --profile "$PROFILE" --region "$REGION" \
       --query "logGroups[?logGroupName=='${LOG_GROUP}'].logGroupName" \
       --output text | grep -q "$LOG_GROUP"; then
    echo "[OK] log group $LOG_GROUP exists"
  else
    echo "[..] creating log group $LOG_GROUP"
    aws logs create-log-group --log-group-name "$LOG_GROUP" \
      --profile "$PROFILE" --region "$REGION"
    aws logs put-retention-policy --log-group-name "$LOG_GROUP" \
      --retention-in-days 14 \
      --profile "$PROFILE" --region "$REGION" 2>/dev/null \
      && echo "[OK] log group created (14-day retention)" \
      || echo "[OK] log group created (retention not set)"
  fi
}

upsert_state_machine() {
  SM_ARN="arn:aws:states:${REGION}:${MY_ACCOUNT}:stateMachine:${SM_NAME}"
  LOGGING_CFG=$(cat <<EOF
{
  "level": "ALL",
  "includeExecutionData": true,
  "destinations": [
    {"cloudWatchLogsLogGroup": {"logGroupArn": "${LOG_GROUP_ARN}"}}
  ]
}
EOF
)
  TRACING_CFG='{"enabled": false}'

  if aws stepfunctions describe-state-machine --state-machine-arn "$SM_ARN" \
        --profile "$PROFILE" --region "$REGION" >/dev/null 2>&1; then
    echo "[..] updating state machine $SM_NAME"
    aws stepfunctions update-state-machine \
      --state-machine-arn "$SM_ARN" \
      --definition "file://${ASL_FILE}" \
      --role-arn "$SM_ROLE_ARN" \
      --logging-configuration "$LOGGING_CFG" \
      --tracing-configuration "$TRACING_CFG" \
      --profile "$PROFILE" --region "$REGION" >/dev/null
  else
    echo "[..] creating state machine $SM_NAME"
    aws stepfunctions create-state-machine \
      --name "$SM_NAME" --type STANDARD \
      --definition "file://${ASL_FILE}" \
      --role-arn "$SM_ROLE_ARN" \
      --logging-configuration "$LOGGING_CFG" \
      --tracing-configuration "$TRACING_CFG" \
      --tags "key=project,value=proj1102" "key=owner,value=ralph" "key=horizon,value=daily" \
      --profile "$PROFILE" --region "$REGION" >/dev/null
  fi
  aws stepfunctions tag-resource --resource-arn "$SM_ARN" \
    --tags "key=project,value=proj1102" "key=owner,value=ralph" "key=horizon,value=daily" \
    --profile "$PROFILE" --region "$REGION" >/dev/null 2>&1 || true
  echo "[OK] state machine $SM_NAME ready"
}

ensure_schedule() {
  SM_ARN="arn:aws:states:${REGION}:${MY_ACCOUNT}:stateMachine:${SM_NAME}"
  aws events put-rule --name "$SCHEDULE_RULE" \
    --schedule-expression "$SCHEDULE_EXPR" \
    --description "Daily weekday trigger for the proj1102-daily-pipeline state machine" \
    --profile "$PROFILE" --region "$REGION" >/dev/null
  aws events put-targets --rule "$SCHEDULE_RULE" \
    --targets "Id=1,Arn=${SM_ARN},RoleArn=${EVENT_ROLE_ARN}" \
    --profile "$PROFILE" --region "$REGION" >/dev/null
  echo "[OK] schedule $SCHEDULE_RULE -> state machine"
}

ensure_failure_alarm() {
  SM_ARN="arn:aws:states:${REGION}:${MY_ACCOUNT}:stateMachine:${SM_NAME}"
  aws cloudwatch put-metric-alarm \
    --alarm-name "proj1102-daily-pipeline-execution-failed" \
    --alarm-description "proj1102-daily-pipeline state machine had a failed execution" \
    --namespace "AWS/States" --metric-name "ExecutionsFailed" \
    --dimensions "Name=StateMachineArn,Value=${SM_ARN}" \
    --statistic Sum --period 300 --evaluation-periods 1 \
    --threshold 0 --comparison-operator GreaterThanThreshold \
    --treat-missing-data notBreaching \
    --alarm-actions "$INFRA_ALARMS_TOPIC" \
    --profile "$PROFILE" --region "$REGION"
  echo "[OK] alarm proj1102-daily-pipeline-execution-failed wired"
}

disable_old_schedules() {
  for rule in proj1102-daily-ingest-schedule proj1102-daily-forecast-schedule; do
    if aws events describe-rule --name "$rule" --profile "$PROFILE" --region "$REGION" >/dev/null 2>&1; then
      aws events disable-rule --name "$rule" --profile "$PROFILE" --region "$REGION"
      echo "[OK] disabled $rule"
    fi
  done
}

ensure_sm_role
ensure_event_role
ensure_log_group
upsert_state_machine
ensure_schedule
ensure_failure_alarm

if [[ "${1:-}" == "--with-cutover" ]]; then
  disable_old_schedules
  echo ""
  echo "Cutover done - proj1102-daily-{ingest,forecast}-schedule DISABLED."
fi

echo ""
echo "=== Done ==="
echo "State machine: $SM_NAME  ($SCHEDULE_EXPR)"
echo "Console:       https://${REGION}.console.aws.amazon.com/states/home?region=${REGION}#/statemachines/view/arn:aws:states:${REGION}:${MY_ACCOUNT}:stateMachine:${SM_NAME}"
