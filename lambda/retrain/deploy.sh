#!/usr/bin/env bash
# Deploy the weekly retrain Lambda - proj1102 (Ralph's account 148557232117).
#
# Reuses the forecast Lambda's execution role (AmazonSageMakerFullAccess +
# AmazonS3FullAccess + AWSLambdaBasicExecutionRole are exactly what we need).
# Reuses the AWS SDK for pandas managed layer for parquet support.
#
# Schedule: every Sunday at 06:00 UTC - quiet time, well outside trading hours.
# ~9 min Lambda runtime, ~$0.02 per training run, billed against academic credits.
#
# Usage:
#   cd lambda/retrain
#   bash deploy.sh            # full create
#   bash deploy.sh update     # update code only

set -euo pipefail

PROFILE="ralph"
REGION="eu-west-1"
MY_ACCOUNT="148557232117"

FUNCTION_NAME="proj1102-retrain"
ROLE_NAME="proj1102-forecast-exec-role"          # reuse the forecast role
ROLE_ARN="arn:aws:iam::${MY_ACCOUNT}:role/${ROLE_NAME}"

# Same as train_and_deploy.py: SageMaker reads the team parquets but writes
# training artifacts and the model into my bucket.
ARTIFACT_BUCKET="proj1102-ralph-forecast-${MY_ACCOUNT}"
SAGEMAKER_ROLE_ARN="arn:aws:iam::${MY_ACCOUNT}:role/ProjectForecastS3Role"

PANDAS_LAYER="arn:aws:lambda:${REGION}:336392948345:layer:AWSSDKPandas-Python312:24"

SCHEDULE_RULE="proj1102-retrain-schedule"
# Sunday 06:00 UTC every week
SCHEDULE_EXPR="cron(0 6 ? * SUN *)"

ZIP_PATH="/tmp/${FUNCTION_NAME}.zip"
SRC="lambda_function.py"

build_zip() {
  rm -f "$ZIP_PATH"
  zip -j "$ZIP_PATH" "$SRC" >/dev/null
  echo "[OK] built $ZIP_PATH"
}

create_fn() {
  echo "[..] creating Lambda function $FUNCTION_NAME"
  aws lambda create-function \
    --function-name "$FUNCTION_NAME" \
    --runtime "python3.12" \
    --role "$ROLE_ARN" \
    --handler "lambda_function.lambda_handler" \
    --timeout 900 \
    --memory-size 512 \
    --layers "$PANDAS_LAYER" \
    --zip-file "fileb://$ZIP_PATH" \
    --environment "Variables={ARTIFACT_BUCKET=${ARTIFACT_BUCKET},ROLE_ARN=${SAGEMAKER_ROLE_ARN},DATA_BUCKET=proj1102-data,READY_PREFIX=forecast-ready,TICKERS=TSLA\,MSFT\,NVDA,APPREGION=${REGION}}" \
    --profile "$PROFILE" --region "$REGION" >/dev/null
  echo "[OK] function created"

  echo "[..] creating EventBridge schedule ($SCHEDULE_EXPR)"
  aws events put-rule \
    --name "$SCHEDULE_RULE" \
    --schedule-expression "$SCHEDULE_EXPR" \
    --description "Triggers the proj1102 retrain Lambda weekly (Sunday 06:00 UTC)" \
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
echo "Schedule: $SCHEDULE_EXPR  ($SCHEDULE_RULE)"
echo "Cost:     ~\$0.02 per run, ~\$1/year total (academic credits)"
echo ""
echo "Tail logs:"
echo "  aws logs tail /aws/lambda/${FUNCTION_NAME} --follow --profile ${PROFILE} --region ${REGION}"
