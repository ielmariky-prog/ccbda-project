#!/usr/bin/env bash
# Deploy the forecast Lambda to Ralph's account (148557232117).
#
# Why Ralph's account?  The team sandbox account's deployer user has tightly
# scoped permissions (can't even read forecast-ready/). Ralph's account has full
# SageMaker + DynamoDB + cross-account S3 to the team bucket, so the whole
# forecast stack lives here. Outputs land where the team can reach them:
#   • S3 CSV   -> s3://proj1102-data/forecast-output/  (team bucket, cross-account write)
#   • DynamoDB -> proj1102-forecasts in Ralph's account, with a resource-based
#                 policy granting the team account read access.
#
# Prerequisites (all already done):
#   • DynamoDB table proj1102-forecasts created (create_table.py)
#   • DeepAR serverless endpoint deployed   (sagemaker/train_and_deploy.py)
#   • Execution role proj1102-forecast-exec-role - created by this script
#
# Usage:
#   cd lambda/forecast
#   bash deploy.sh                     # first-time deploy + EventBridge schedule
#   bash deploy.sh update              # update code only

set -euo pipefail

# Config
PROFILE="ralph"
REGION="eu-west-1"
MY_ACCOUNT="148557232117"

FUNCTION_NAME="proj1102-forecast"
ROLE_NAME="proj1102-forecast-exec-role"
ROLE_ARN="arn:aws:iam::${MY_ACCOUNT}:role/${ROLE_NAME}"

DDB_TABLE="proj1102-forecasts"
SAGEMAKER_ENDPOINT="proj1102-deepar-endpoint"

# AWS SDK for pandas managed layer (pandas + pyarrow + numpy) for python3.12
# (latest available in eu-west-1 - probe with get-layer-version-by-arn if it 404s)
PANDAS_LAYER="arn:aws:lambda:${REGION}:336392948345:layer:AWSSDKPandas-Python312:24"

# EventBridge schedule - hourly, a bit after Dalibor's (now hourly) Glue cycle.
# Forecast horizon is only 1h, so an hourly run keeps predictions continuous
# and fresh instead of leaving 2h gaps. Confirm the :45 offset clears Glue.
SCHEDULE_RULE="proj1102-forecast-schedule"
SCHEDULE_EXPR="cron(45 * * * ? *)"

ZIP_PATH="/tmp/${FUNCTION_NAME}.zip"
SRC="lambda_function.py"

# Build deploy zip
build_zip() {
  rm -f "$ZIP_PATH"
  zip -j "$ZIP_PATH" "$SRC" >/dev/null
  echo "[OK] built $ZIP_PATH"
}

# Create or reuse IAM execution role
ensure_role() {
  if aws iam get-role --role-name "$ROLE_NAME" --profile "$PROFILE" >/dev/null 2>&1; then
    echo "[OK] role $ROLE_NAME already exists"
    return
  fi
  echo "[..] creating role $ROLE_NAME"
  TRUST=$(cat <<'EOF'
{ "Version": "2012-10-17",
  "Statement": [{ "Effect": "Allow",
    "Principal": { "Service": "lambda.amazonaws.com" },
    "Action": "sts:AssumeRole" }] }
EOF
)
  aws iam create-role --role-name "$ROLE_NAME" \
    --assume-role-policy-document "$TRUST" \
    --description "Execution role for the proj1102 forecast Lambda" \
    --profile "$PROFILE" >/dev/null

  for p in \
    arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole \
    arn:aws:iam::aws:policy/AmazonS3FullAccess \
    arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess \
    arn:aws:iam::aws:policy/AmazonSageMakerFullAccess ; do
    aws iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn "$p" --profile "$PROFILE"
  done
  echo "[OK] role created - waiting 10s for IAM propagation"
  sleep 10
}

# First-time create
create_fn() {
  echo "[..] creating Lambda function $FUNCTION_NAME"
  aws lambda create-function \
    --function-name "$FUNCTION_NAME" \
    --runtime "python3.12" \
    --role "$ROLE_ARN" \
    --handler "lambda_function.lambda_handler" \
    --timeout 300 \
    --memory-size 512 \
    --layers "$PANDAS_LAYER" \
    --zip-file "fileb://$ZIP_PATH" \
    --environment "Variables={FORECAST_BUCKET=proj1102-data,FORECAST_READY_PREFIX=forecast-ready,FORECAST_OUTPUT_PREFIX=forecast-output,DDB_TABLE=${DDB_TABLE},SAGEMAKER_ENDPOINT=${SAGEMAKER_ENDPOINT},TICKERS=TSLA\,MSFT\,NVDA,CONTEXT_LENGTH=120,PREDICTION_LENGTH=12,APPREGION=${REGION}}" \
    --profile "$PROFILE" --region "$REGION" \
    >/dev/null
  echo "[OK] function created"

  echo "[..] creating EventBridge schedule ($SCHEDULE_EXPR)"
  aws events put-rule \
    --name "$SCHEDULE_RULE" \
    --schedule-expression "$SCHEDULE_EXPR" \
    --description "Triggers the proj1102 forecast Lambda every 3h" \
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

# Update code only
update_fn() {
  echo "[..] updating function code"
  aws lambda update-function-code \
    --function-name "$FUNCTION_NAME" \
    --zip-file "fileb://$ZIP_PATH" \
    --profile "$PROFILE" --region "$REGION" >/dev/null
  echo "[OK] function code updated"
}

# Main
ensure_role
build_zip

if [[ "${1:-}" == "update" ]]; then
  update_fn
else
  if aws lambda get-function --function-name "$FUNCTION_NAME" \
        --profile "$PROFILE" --region "$REGION" >/dev/null 2>&1; then
    echo "[..] function already exists - updating code"
    update_fn
  else
    create_fn
  fi
fi

echo ""
echo "=== Done ==="
echo "Function: $FUNCTION_NAME  (in ${MY_ACCOUNT})"
echo "Schedule: $SCHEDULE_EXPR"
echo "Reads:    s3://proj1102-data/forecast-ready/<TICKER>.parquet"
echo "Writes:   s3://proj1102-data/forecast-output/<TICKER>/predictions_<RUN_TS>.csv"
echo "          DynamoDB proj1102-forecasts"
echo "Endpoint: $SAGEMAKER_ENDPOINT"
echo ""
echo "Tail logs:"
echo "  aws logs tail /aws/lambda/${FUNCTION_NAME} --follow --profile ${PROFILE} --region ${REGION}"
