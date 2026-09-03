import boto3

sns = boto3.client('sns')
SNS_TOPIC_ARN = "arn:aws:sns:eu-west-1:162655485124:proj1102-alerts"


def lambda_handler(event, context):
    records = event.get("Records", [])
    print(f"Received batch of {len(records)} stream record(s)")

    for record in records:
        # Anomaly is only set on the MODIFY (when Ralph's forecast Lambda
        # backfills actual + is_anomaly on a prior prediction). On the INSERT
        # the row's is_anomaly is {"NULL": True} - reading ["BOOL"] would
        # KeyError and DynamoDB Streams would retry the record forever.
        if record.get("eventName") != "MODIFY":
            continue

        new_image = record.get("dynamodb", {}).get("NewImage", {})
        if not new_image.get("is_anomaly", {}).get("BOOL"):
            continue  # not an anomaly (or is_anomaly still NULL)

        ticker    = new_image["ticker"]["S"]
        timestamp = new_image["timestamp"]["S"]
        p10       = new_image["p10"]["N"]
        p50       = new_image["p50"]["N"]
        p90       = new_image["p90"]["N"]
        actual    = new_image["actual"]["N"]
        delta     = new_image["delta"]["N"]
        severity  = new_image.get("severity", {}).get("S", "unknown")

        subject = f"[ALERT] Anomaly detected for {ticker}"
        body = f"""Ticker: {ticker}
Timestamp: {timestamp}
Predicted: {p50} (range {p10} - {p90})
Actual: {actual}
Delta: {delta}
Severity: {severity}
Full forecast row in DynamoDB: proj1102-forecasts (ticker={ticker}, timestamp={timestamp})
Dashboard:  https://eu-west-1.quicksight.aws.amazon.com/sn/account/ralph-upc-test-2026/start/dashboards"""

        try:
            sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=body)
            print(f"[ALERT] {ticker} @ {timestamp} severity={severity} -> SNS publish OK")
        except Exception as e:
            print(f"[ERROR] {ticker} @ {timestamp} - SNS publish failed: {e}")
