#!/usr/bin/env bash
# Deploy the sentiment Lambda to Ralph's account (148557232117).
#
# Why not the team account?  Because the team account (834922934600) is a
# restricted AWS sandbox that doesn't allow Comprehend (or Translate). We
# deploy in Ralph's account where Comprehend is enabled, and consume the
# team SQS news-queue cross-account.
#
# Required cross-account setup (already done):
#   • Team SQS:  resource policy on proj1102-news-queue allows
#                arn:aws:iam::148557232117:root -> ReceiveMessage / DeleteMessage
#   • Team S3:   bucket policy on proj1102-data allows ralph-comprehend to
#                read+write the sentiment/ prefix
#   • My IAM:    AmazonSQSFullAccess attached to ralph-comprehend
#
# Usage:
#   cd lambda/sentiment
#   bash deploy.sh                     # first-time deploy + wire SQS trigger
#   bash deploy.sh update              # update code only (faster on re-deploys)

set -euo pipefail

# Config
PROFILE="ralph"
REGION="eu-west-1"
MY_ACCOUNT="148557232117"
TEAM_ACCOUNT="834922934600"

FUNCTION_NAME="proj1102-sentiment"
ROLE_NAME="proj1102-sentiment-exec-role"
ROLE_ARN="arn:aws:iam::${MY_ACCOUNT}:role/${ROLE_NAME}"
QUEUE_ARN="arn:aws:sqs:${REGION}:${TEAM_ACCOUNT}:proj1102-news-queue"

BUCKET="proj1102-data"
PREFIX="sentiment"

ZIP_PATH="/tmp/${FUNCTION_NAME}.zip"
SRC="lambda_function.py"

# Build deploy zip
build_zip() {
  rm -f "$ZIP_PATH"
  zip -j "$ZIP_PATH" "$SRC" >/dev/null
  echo "[OK] built $ZIP_PATH"
}

# Create or update IAM execution role
ensure_role() {
  if aws iam get-role --role-name "$ROLE_NAME" --profile "$PROFILE" >/dev/null 2>&1; then
    echo "[OK] role $ROLE_NAME already exists"
    return
  fi

  echo "[..] creating role $ROLE_NAME"
  TRUST_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "lambda.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
EOF
)
  aws iam create-role \
    --role-name "$ROLE_NAME" \
    --assume-role-policy-document "$TRUST_POLICY" \
    --description "Execution role for the proj1102 sentiment Lambda" \
    --profile "$PROFILE" >/dev/null

  echo "[..] attaching managed policies"
  for p in AWSLambdaBasicExecutionRole ComprehendReadOnly AmazonSQSFullAccess AmazonS3FullAccess; do
    aws iam attach-role-policy \
      --role-name "$ROLE_NAME" \
      --policy-arn "arn:aws:iam::aws:policy/service-role/${p}" \
      --profile "$PROFILE" 2>/dev/null \
    || aws iam attach-role-policy \
      --role-name "$ROLE_NAME" \
      --policy-arn "arn:aws:iam::aws:policy/${p}" \
      --profile "$PROFILE"
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
    --timeout 60 \
    --memory-size 256 \
    --zip-file "fileb://$ZIP_PATH" \
    --environment "Variables={SENTIMENT_BUCKET=${BUCKET},SENTIMENT_PREFIX=${PREFIX},APPREGION=${REGION}}" \
    --profile "$PROFILE" --region "$REGION" \
    >/dev/null
  echo "[OK] function created"

  echo "[..] capping reserved concurrency at 1 (avoid CSV write races)"
  aws lambda put-function-concurrency \
    --function-name "$FUNCTION_NAME" \
    --reserved-concurrent-executions 1 \
    --profile "$PROFILE" --region "$REGION" \
    >/dev/null
  echo "[OK] concurrency capped"

  echo "[..] attaching cross-account SQS event-source mapping (team news-queue -> Lambda)"
  aws lambda create-event-source-mapping \
    --function-name "$FUNCTION_NAME" \
    --event-source-arn "$QUEUE_ARN" \
    --batch-size 5 \
    --maximum-batching-window-in-seconds 5 \
    --function-response-types "ReportBatchItemFailures" \
    --profile "$PROFILE" --region "$REGION" \
    >/dev/null
  echo "[OK] SQS trigger attached"
}

# Update code only
update_fn() {
  echo "[..] updating function code"
  aws lambda update-function-code \
    --function-name "$FUNCTION_NAME" \
    --zip-file "fileb://$ZIP_PATH" \
    --profile "$PROFILE" --region "$REGION" \
    >/dev/null
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
echo "Region:   $REGION"
echo "Trigger:  $QUEUE_ARN  (cross-account, in ${TEAM_ACCOUNT})"
echo "Output:   s3://${BUCKET}/${PREFIX}/<TICKER>.csv  (in ${TEAM_ACCOUNT})"
echo ""
echo "Tail logs with:"
echo "  aws logs tail /aws/lambda/${FUNCTION_NAME} --follow --profile ${PROFILE} --region ${REGION}"
