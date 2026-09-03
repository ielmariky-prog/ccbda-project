#!/usr/bin/env bash
# Deploy the automatic QuickSight refresh - proj1102 (Ralph's account 148557232117).
#
# QuickSight runs in Ralph's account, so it can only see Athena tables in Ralph's
# account. This sets up a self-refreshing bridge:
#   • Athena EXTERNAL tables proj1102_db.{actuals,forecast}  (registered once)
#   • a Lambda that overwrites the CSVs behind them from live pipeline S3
#   • an EventBridge schedule so it refreshes itself - no manual step
#
# Idempotent - safe to re-run (updates code, re-asserts the tables + schedule).
#
# Usage:
#   cd quicksight
#   bash deploy.sh            # full deploy / update
#   bash deploy.sh update     # update Lambda code only

set -euo pipefail

# Config
PROFILE="ralph"
REGION="eu-west-1"
MY_ACCOUNT="148557232117"

FUNCTION_NAME="proj1102-quicksight-refresh"
ROLE_NAME="proj1102-quicksight-exec-role"
ROLE_ARN="arn:aws:iam::${MY_ACCOUNT}:role/${ROLE_NAME}"

DB="proj1102_db"
ATHENA_RESULTS="s3://proj1102-ralph-forecast-${MY_ACCOUNT}/athena-results/"

# AWS SDK for pandas managed layer (pandas + pyarrow) for python3.12
PANDAS_LAYER="arn:aws:lambda:${REGION}:336392948345:layer:AWSSDKPandas-Python312:24"

# Hourly - runs after Glue and the forecast Lambda (:45) so the dashboard
# data refreshes every hour: Glue -> forecast :45 -> quicksight refresh :55
SCHEDULE_RULE="proj1102-quicksight-schedule"
SCHEDULE_EXPR="cron(55 * * * ? *)"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ZIP_PATH="/tmp/${FUNCTION_NAME}.zip"

# Build deploy zip
build_zip() {
  rm -f "$ZIP_PATH"
  zip -j "$ZIP_PATH" "$HERE/lambda_function.py" >/dev/null
  echo "[OK] built $ZIP_PATH"
}

# IAM execution role
ensure_role() {
  if aws iam get-role --role-name "$ROLE_NAME" --profile "$PROFILE" >/dev/null 2>&1; then
    echo "[OK] role $ROLE_NAME already exists"
    return
  fi
  echo "[..] creating role $ROLE_NAME"
  TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  aws iam create-role --role-name "$ROLE_NAME" \
    --assume-role-policy-document "$TRUST" \
    --description "Execution role for the proj1102 QuickSight refresh Lambda" \
    --profile "$PROFILE" >/dev/null
  for p in \
    arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole \
    arn:aws:iam::aws:policy/AmazonS3FullAccess ; do
    aws iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn "$p" --profile "$PROFILE"
  done
  echo "[OK] role created - waiting 10s for IAM propagation"
  sleep 10
}

# Athena tables (one-time, idempotent)
run_athena() {
  local qid state
  qid=$(aws athena start-query-execution --query-string "$1" \
    --result-configuration "OutputLocation=${ATHENA_RESULTS}" \
    --profile "$PROFILE" --region "$REGION" --query 'QueryExecutionId' --output text)
  while true; do
    state=$(aws athena get-query-execution --query-execution-id "$qid" \
      --profile "$PROFILE" --region "$REGION" \
      --query 'QueryExecution.Status.State' --output text)
    case "$state" in
      SUCCEEDED) return 0 ;;
      FAILED|CANCELLED)
        aws athena get-query-execution --query-execution-id "$qid" \
          --profile "$PROFILE" --region "$REGION" \
          --query 'QueryExecution.Status.StateChangeReason' --output text
        return 1 ;;
      *) sleep 2 ;;
    esac
  done
}

split_sql() {
  python3 - "$HERE/athena_tables.sql" <<'PY'
import sys
sql = open(sys.argv[1]).read()
for chunk in sql.split(";"):
    lines = [l for l in chunk.splitlines() if l.strip() and not l.strip().startswith("--")]
    if lines:
        print(" ".join(lines))
PY
}

register_tables() {
  echo "[..] registering Athena tables in ${DB}"
  while IFS= read -r stmt; do
    label=$(echo "$stmt" | grep -oE 'TABLE [A-Za-z0-9_.]+' | head -1 || echo "statement")
    if run_athena "$stmt"; then echo "[OK] $label"; else echo "[ERR] failed: $label"; exit 1; fi
  done < <(split_sql)
}

# Lambda create / update
create_fn() {
  echo "[..] creating Lambda $FUNCTION_NAME"
  aws lambda create-function \
    --function-name "$FUNCTION_NAME" \
    --runtime "python3.12" \
    --role "$ROLE_ARN" \
    --handler "lambda_function.lambda_handler" \
    --timeout 120 \
    --memory-size 512 \
    --layers "$PANDAS_LAYER" \
    --zip-file "fileb://$ZIP_PATH" \
    --environment "Variables={SRC_BUCKET=proj1102-data,DST_BUCKET=proj1102-ralph-forecast-${MY_ACCOUNT},TICKERS=TSLA\,MSFT\,NVDA,APPREGION=${REGION}}" \
    --profile "$PROFILE" --region "$REGION" >/dev/null
  echo "[OK] function created"

  echo "[..] creating EventBridge schedule ($SCHEDULE_EXPR)"
  aws events put-rule --name "$SCHEDULE_RULE" \
    --schedule-expression "$SCHEDULE_EXPR" \
    --description "Refreshes the proj1102 QuickSight Athena CSVs" \
    --profile "$PROFILE" --region "$REGION" >/dev/null
  aws lambda add-permission --function-name "$FUNCTION_NAME" \
    --statement-id "eventbridge-invoke" --action "lambda:InvokeFunction" \
    --principal "events.amazonaws.com" \
    --source-arn "arn:aws:events:${REGION}:${MY_ACCOUNT}:rule/${SCHEDULE_RULE}" \
    --profile "$PROFILE" --region "$REGION" >/dev/null 2>&1 || true
  aws events put-targets --rule "$SCHEDULE_RULE" \
    --targets "Id=1,Arn=arn:aws:lambda:${REGION}:${MY_ACCOUNT}:function:${FUNCTION_NAME}" \
    --profile "$PROFILE" --region "$REGION" >/dev/null
  echo "[OK] EventBridge schedule wired"
}

update_fn() {
  echo "[..] updating function code"
  aws lambda update-function-code --function-name "$FUNCTION_NAME" \
    --zip-file "fileb://$ZIP_PATH" --profile "$PROFILE" --region "$REGION" >/dev/null
  echo "[OK] function code updated"
}

# Main
ensure_role
build_zip

if [[ "${1:-}" == "update" ]]; then
  update_fn
else
  register_tables
  if aws lambda get-function --function-name "$FUNCTION_NAME" \
        --profile "$PROFILE" --region "$REGION" >/dev/null 2>&1; then
    echo "[..] function exists - updating code"
    update_fn
  else
    create_fn
  fi
fi

echo "[..] initial invoke to populate the tables now"
aws lambda invoke --function-name "$FUNCTION_NAME" \
  --profile "$PROFILE" --region "$REGION" /tmp/${FUNCTION_NAME}_out.json >/dev/null
cat /tmp/${FUNCTION_NAME}_out.json; echo

echo ""
echo "=== Done ==="
echo "Lambda:   $FUNCTION_NAME  (hourly: $SCHEDULE_EXPR)"
echo "Refreshes: s3://proj1102-ralph-forecast-${MY_ACCOUNT}/quicksight/{actuals,forecast}/"
echo "Tables:    ${DB}.actuals, ${DB}.forecast  (Athena, auto-fresh)"
echo ""
echo "Francesco: QuickSight -> New dataset -> Athena -> ${DB} -> actuals + forecast."
echo "The pipeline now refreshes the dashboard data on its own - no manual step."
