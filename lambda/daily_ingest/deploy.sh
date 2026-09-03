#!/usr/bin/env bash
# Deploy the daily ingestion Lambda - proj1102 (Ralph's account 148557232117).
#
# Uses the AWS SDK for pandas managed layer for parquet writes. Reuses the
# forecast Lambda's exec role (already has S3 + CloudWatch perms).

set -euo pipefail

PROFILE="ralph"
REGION="eu-west-1"
MY_ACCOUNT="148557232117"

FUNCTION_NAME="proj1102-daily-ingest"
ROLE_NAME="proj1102-forecast-exec-role"        # reuse
ROLE_ARN="arn:aws:iam::${MY_ACCOUNT}:role/${ROLE_NAME}"

PANDAS_LAYER="arn:aws:lambda:${REGION}:336392948345:layer:AWSSDKPandas-Python312:24"

SCHEDULE_RULE="proj1102-daily-ingest-schedule"
# 20:00 UTC Mon-Fri - after US market close, before forecast lambda
SCHEDULE_EXPR="cron(0 20 ? * MON-FRI *)"

ZIP_PATH="/tmp/${FUNCTION_NAME}.zip"
SRC="lambda_function.py"

build_zip() {
  rm -f "$ZIP_PATH"
  zip -j "$ZIP_PATH" "$SRC" >/dev/null
  echo "[OK] built $ZIP_PATH"
}

create_fn() {
  echo "[..] creating Lambda $FUNCTION_NAME"
  aws lambda create-function \
    --function-name "$FUNCTION_NAME" \
    --runtime "python3.12" \
    --role "$ROLE_ARN" \
    --handler "lambda_function.lambda_handler" \
    --timeout 60 --memory-size 256 \
    --layers "$PANDAS_LAYER" \
    --zip-file "fileb://$ZIP_PATH" \
    --environment "Variables={DATA_BUCKET=proj1102-data,TICKERS=TSLA\,MSFT\,NVDA,APPREGION=${REGION}}" \
    --profile "$PROFILE" --region "$REGION" >/dev/null
  echo "[OK] function created"

  aws events put-rule --name "$SCHEDULE_RULE" \
    --schedule-expression "$SCHEDULE_EXPR" \
    --description "Daily ingestion of TSLA/MSFT/NVDA close prices (post market close UTC)" \
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
