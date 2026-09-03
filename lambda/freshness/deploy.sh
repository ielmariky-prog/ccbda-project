#!/usr/bin/env bash
# Deploy the parquet freshness check - proj1102 (Ralph's account 148557232117).
#
# Reuses the forecast Lambda's execution role (already has S3 access). Attaches
# CloudWatchFullAccess for cloudwatch:PutMetricData. Reuses the AWS SDK for
# pandas managed layer for parquet reads.
#
# Schedule: every hour at :35, between Glue (~:29) and the forecast Lambda (:45).
# A CloudWatch alarm on proj1102/oldest_parquet_age_minutes > 120 fires to
# proj1102-infra-alarms.

set -euo pipefail

PROFILE="ralph"
REGION="eu-west-1"
MY_ACCOUNT="148557232117"

FUNCTION_NAME="proj1102-freshness"
ROLE_NAME="proj1102-forecast-exec-role"   # reuse
ROLE_ARN="arn:aws:iam::${MY_ACCOUNT}:role/${ROLE_NAME}"

INFRA_ALARMS_TOPIC="arn:aws:sns:${REGION}:${MY_ACCOUNT}:proj1102-infra-alarms"

PANDAS_LAYER="arn:aws:lambda:${REGION}:336392948345:layer:AWSSDKPandas-Python312:24"

SCHEDULE_RULE="proj1102-freshness-schedule"
SCHEDULE_EXPR="cron(35 * * * ? *)"

ALARM_NAME="proj1102-parquet-stale"
ALARM_THRESHOLD_MIN=120                    # hourly parquet: alarm if data is >2h stale

# Daily parquet alarm - threshold must tolerate the Fri-close -> Mon-close
# weekend gap. yfinance dates daily bars at 00:00 UTC of the trading day
# (not at the 20:00 UTC close), so the natural max age between healthy
# weekday ingests is ~92h normally and ~116h around a Monday US holiday.
DAILY_ALARM_NAME="proj1102-daily-parquet-stale"
DAILY_ALARM_THRESHOLD_MIN=7200             # 120h - covers weekends + Monday holidays

ZIP_PATH="/tmp/${FUNCTION_NAME}.zip"
SRC="lambda_function.py"

build_zip() {
  rm -f "$ZIP_PATH"
  zip -j "$ZIP_PATH" "$SRC" >/dev/null
  echo "[OK] built $ZIP_PATH"
}

ensure_role_perms() {
  # Forecast exec role already exists; just make sure it can PutMetricData.
  aws iam attach-role-policy --role-name "$ROLE_NAME" \
    --policy-arn "arn:aws:iam::aws:policy/CloudWatchFullAccess" \
    --profile "$PROFILE" >/dev/null 2>&1 || true
  echo "[OK] CloudWatchFullAccess attached to $ROLE_NAME (idempotent)"
}

create_fn() {
  echo "[..] creating Lambda $FUNCTION_NAME"
  aws lambda create-function \
    --function-name "$FUNCTION_NAME" \
    --runtime "python3.12" \
    --role "$ROLE_ARN" \
    --handler "lambda_function.lambda_handler" \
    --timeout 60 \
    --memory-size 256 \
    --layers "$PANDAS_LAYER" \
    --zip-file "fileb://$ZIP_PATH" \
    --environment "Variables={DATA_BUCKET=proj1102-data,READY_PREFIX=forecast-ready,TICKERS=TSLA\,MSFT\,NVDA,APPREGION=${REGION}}" \
    --profile "$PROFILE" --region "$REGION" >/dev/null
  echo "[OK] function created"

  echo "[..] creating EventBridge schedule ($SCHEDULE_EXPR)"
  aws events put-rule \
    --name "$SCHEDULE_RULE" \
    --schedule-expression "$SCHEDULE_EXPR" \
    --description "Hourly parquet freshness probe" \
    --profile "$PROFILE" --region "$REGION" >/dev/null

  aws lambda add-permission \
    --function-name "$FUNCTION_NAME" \
    --statement-id "eventbridge-invoke" \
    --action "lambda:InvokeFunction" \
    --principal "events.amazonaws.com" \
    --source-arn "arn:aws:events:${REGION}:${MY_ACCOUNT}:rule/${SCHEDULE_RULE}" \
    --profile "$PROFILE" --region "$REGION" >/dev/null 2>&1 || true

  aws events put-targets \
    --rule "$SCHEDULE_RULE" \
    --targets "Id=1,Arn=arn:aws:lambda:${REGION}:${MY_ACCOUNT}:function:${FUNCTION_NAME}" \
    --profile "$PROFILE" --region "$REGION" >/dev/null
  echo "[OK] EventBridge schedule wired"
}

update_fn() {
  echo "[..] updating function code"
  aws lambda update-function-code \
    --function-name "$FUNCTION_NAME" \
    --zip-file "fileb://$ZIP_PATH" \
    --profile "$PROFILE" --region "$REGION" >/dev/null
  echo "[OK] function code updated"
}

create_alarm() {
  echo "[..] creating CloudWatch alarm '$ALARM_NAME' (>${ALARM_THRESHOLD_MIN} min)"
  aws cloudwatch put-metric-alarm \
    --alarm-name "$ALARM_NAME" \
    --alarm-description "Forecast-ready parquet older than ${ALARM_THRESHOLD_MIN} min - ingestion or Glue stopped" \
    --namespace "proj1102" \
    --metric-name "oldest_parquet_age_minutes" \
    --statistic Maximum \
    --period 3600 \
    --evaluation-periods 1 \
    --threshold "$ALARM_THRESHOLD_MIN" \
    --comparison-operator GreaterThanThreshold \
    --treat-missing-data notBreaching \
    --alarm-actions "$INFRA_ALARMS_TOPIC" \
    --profile "$PROFILE" --region "$REGION"
  echo "[OK] hourly alarm wired to $INFRA_ALARMS_TOPIC"

  echo "[..] creating CloudWatch alarm '$DAILY_ALARM_NAME' (>${DAILY_ALARM_THRESHOLD_MIN} min = 120h)"
  aws cloudwatch put-metric-alarm \
    --alarm-name "$DAILY_ALARM_NAME" \
    --alarm-description "Daily parquet (Yahoo close prices) older than 120h - covers Fri-close->Mon-close weekends and Monday US holidays. Below this threshold (~80h) the alarm tripped every Monday morning because yfinance dates daily bars at 00:00 UTC, not the 20:00 UTC close." \
    --namespace "proj1102" \
    --metric-name "oldest_daily_parquet_age_minutes" \
    --statistic Maximum \
    --period 3600 \
    --evaluation-periods 1 \
    --threshold "$DAILY_ALARM_THRESHOLD_MIN" \
    --comparison-operator GreaterThanThreshold \
    --treat-missing-data notBreaching \
    --alarm-actions "$INFRA_ALARMS_TOPIC" \
    --profile "$PROFILE" --region "$REGION"
  echo "[OK] daily alarm wired to $INFRA_ALARMS_TOPIC"
}

ensure_role_perms
build_zip

if [[ "${1:-}" == "update" ]]; then
  update_fn
else
  if aws lambda get-function --function-name "$FUNCTION_NAME" \
        --profile "$PROFILE" --region "$REGION" >/dev/null 2>&1; then
    echo "[..] function exists - updating code"
    update_fn
  else
    create_fn
  fi
fi

create_alarm

echo ""
echo "=== Done ==="
echo "Function: $FUNCTION_NAME  (hourly: $SCHEDULE_EXPR)"
echo "Alarm:    $ALARM_NAME  (> ${ALARM_THRESHOLD_MIN} min -> $INFRA_ALARMS_TOPIC)"
echo ""
echo "Tail logs:"
echo "  aws logs tail /aws/lambda/${FUNCTION_NAME} --follow --profile ${PROFILE} --region ${REGION}"
