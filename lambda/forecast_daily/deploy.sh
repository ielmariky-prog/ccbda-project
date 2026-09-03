#!/usr/bin/env bash
# Deploy the daily forecast Lambda + DynamoDB table - proj1102 strategic horizon.
#
# Reuses the forecast Lambda's exec role (already has SageMaker/DynamoDB/S3 perms).
# Creates the proj1102-daily-forecasts table (no stream, no anomaly alerts).
# Schedules the Lambda for 20:15 UTC weekdays (after the daily ingest).

set -euo pipefail

PROFILE="ralph"
REGION="eu-west-1"
MY_ACCOUNT="148557232117"

FUNCTION_NAME="proj1102-daily-forecast"
ROLE_NAME="proj1102-forecast-exec-role"
ROLE_ARN="arn:aws:iam::${MY_ACCOUNT}:role/${ROLE_NAME}"

DDB_TABLE="proj1102-daily-forecasts"
SAGEMAKER_ENDPOINT="proj1102-deepar-daily-endpoint"

PANDAS_LAYER="arn:aws:lambda:${REGION}:336392948345:layer:AWSSDKPandas-Python312:24"

SCHEDULE_RULE="proj1102-daily-forecast-schedule"
# 20:15 UTC Mon-Fri (15 min after the daily ingest at 20:00 UTC)
SCHEDULE_EXPR="cron(15 20 ? * MON-FRI *)"

ZIP_PATH="/tmp/${FUNCTION_NAME}.zip"
SRC="lambda_function.py"

# DynamoDB table
ensure_table() {
  if aws dynamodb describe-table --table-name "$DDB_TABLE" \
       --profile "$PROFILE" --region "$REGION" >/dev/null 2>&1; then
    echo "[OK] table $DDB_TABLE exists"
    return
  fi
  echo "[..] creating DynamoDB table $DDB_TABLE"
  aws dynamodb create-table --table-name "$DDB_TABLE" \
    --attribute-definitions AttributeName=ticker,AttributeType=S AttributeName=timestamp,AttributeType=S \
    --key-schema AttributeName=ticker,KeyType=HASH AttributeName=timestamp,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --tags Key=project,Value=proj1102 Key=owner,Value=ralph Key=horizon,Value=daily \
    --profile "$PROFILE" --region "$REGION" >/dev/null
  aws dynamodb wait table-exists --table-name "$DDB_TABLE" --profile "$PROFILE" --region "$REGION"
  echo "[OK] table ACTIVE"
}

# Lambda
build_zip() {
  rm -f "$ZIP_PATH"
  zip -j "$ZIP_PATH" "$SRC" >/dev/null
  echo "[OK] built $ZIP_PATH"
}

create_fn() {
  echo "[..] creating Lambda $FUNCTION_NAME"
  aws lambda create-function \
    --function-name "$FUNCTION_NAME" \
    --runtime "python3.12" --role "$ROLE_ARN" \
    --handler "lambda_function.lambda_handler" \
    --timeout 120 --memory-size 512 \
    --layers "$PANDAS_LAYER" \
    --zip-file "fileb://$ZIP_PATH" \
    --environment "Variables={DAILY_BUCKET=proj1102-data,DAILY_READY_PREFIX=daily,DAILY_OUTPUT_PREFIX=daily-forecast-output,DDB_TABLE=${DDB_TABLE},SAGEMAKER_ENDPOINT=${SAGEMAKER_ENDPOINT},TICKERS=TSLA\,MSFT\,NVDA,CONTEXT_LENGTH=60,PREDICTION_LENGTH=7,APPREGION=${REGION}}" \
    --profile "$PROFILE" --region "$REGION" >/dev/null
  echo "[OK] function created"

  aws events put-rule --name "$SCHEDULE_RULE" \
    --schedule-expression "$SCHEDULE_EXPR" \
    --description "Daily 7-day-ahead forecast (after market close UTC, after ingest)" \
    --profile "$PROFILE" --region "$REGION" >/dev/null
  aws lambda add-permission --function-name "$FUNCTION_NAME" \
    --statement-id "eventbridge-invoke" --action "lambda:InvokeFunction" \
    --principal "events.amazonaws.com" \
    --source-arn "arn:aws:events:${REGION}:${MY_ACCOUNT}:rule/${SCHEDULE_RULE}" \
    --profile "$PROFILE" --region "$REGION" >/dev/null 2>&1 || true
  aws events put-targets --rule "$SCHEDULE_RULE" \
    --targets "Id=1,Arn=arn:aws:lambda:${REGION}:${MY_ACCOUNT}:function:${FUNCTION_NAME}" \
    --profile "$PROFILE" --region "$REGION" >/dev/null
  echo "[OK] EventBridge schedule wired ($SCHEDULE_EXPR)"
}

update_fn() {
  aws lambda update-function-code --function-name "$FUNCTION_NAME" \
    --zip-file "fileb://$ZIP_PATH" --profile "$PROFILE" --region "$REGION" >/dev/null
  echo "[OK] function code updated"
}

ensure_table
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

echo ""
echo "=== Done ==="
echo "Function: $FUNCTION_NAME ($SCHEDULE_EXPR)"
echo "Table:    $DDB_TABLE"
echo "Endpoint: $SAGEMAKER_ENDPOINT"
