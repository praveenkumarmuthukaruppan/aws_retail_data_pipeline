# ============================================================
# LAMBDA  : kinesis_to_dynamodb           (STEP 2)
# TRIGGER : Kinesis stream — transactions-stream
# REGION  : eu-west-2
#
# DOES:
#   1. Reads records from Kinesis
#   2. Writes 10 core columns to DynamoDB
#   3. If order_value > 950 → sends message to SQS queue
#      SQS → triggers step2b_sqs_to_sns → SNS → email/SMS
#
# ENV VARS:
#   SQS_QUEUE_URL = https://sqs.eu-west-2.amazonaws.com/666127452756/praveen-alerts
# ============================================================

import json
import boto3
import base64
import os
from datetime import datetime

dynamodb   = boto3.resource("dynamodb", region_name="eu-west-2")
sqs        = boto3.client("sqs",       region_name="eu-west-2")

TABLE_NAME    = "praveen_transactions"
SQS_QUEUE_URL = os.environ.get(
    "SQS_QUEUE_URL",
    "https://sqs.eu-west-2.amazonaws.com/666127452756/praveen-alerts"
)
ALERT_THRESHOLD = 990

table = dynamodb.Table(TABLE_NAME)


def lambda_handler(event, context):
    records = event.get("Records", [])
    print(f"Received {len(records)} Kinesis records")

    written = 0
    alerted = 0

    for rec in records:
        try:
            raw    = base64.b64decode(rec["kinesis"]["data"]).decode("utf-8")
            record = json.loads(raw)

            # ── 1. Write to DynamoDB ───────────────────────────
            table.put_item(Item={
                "transaction_id":      str(record.get("transaction_id", "UNKNOWN")),
                "customer_id":         str(record.get("customer_id",    "UNKNOWN")),
                "product_id":          str(record.get("product_id",     "UNKNOWN")),
                "order_date":          str(record.get("order_date",     "UNKNOWN")),
                "order_value":         str(record.get("order_value",    0)),
                "payment_method":      str(record.get("payment_method", "UNKNOWN")),
                "device_type":         str(record.get("device_type",    "UNKNOWN")),
                "discount_applied":    str(record.get("discount_applied", 0)),
                "shipping_delay_days": int(record.get("shipping_delay_days", 0)),
                "high_value_flag":     int(record.get("high_value_flag", 0)),
                "ingested_at":         datetime.utcnow().isoformat(),
            })
            written += 1

            # ── 2. SQS alert if order_value > 990 ─────────────
            if float(record.get("order_value", 0)) > ALERT_THRESHOLD:
                send_to_sqs(record)
                alerted += 1

        except Exception as e:
            print(f"ERROR: {e}")
            raise   # Kinesis retries / DLQ handles it

    print(f"DynamoDB written={written}  SQS alerts={alerted}")
    return {"statusCode": 200, "written": written, "alerted": alerted}


def send_to_sqs(record):
    """
    Send high-value transaction to SQS queue.
    SQS triggers step2b_sqs_to_sns which publishes to SNS.
    """
    message = {
        "transaction_id": record.get("transaction_id"),
        "customer_id":    record.get("customer_id"),
        "order_value":    record.get("order_value"),
        "payment_method": record.get("payment_method"),
        "order_date":     record.get("order_date"),
    }
    sqs.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=json.dumps(message)
    )
    print(f"SQS: {record.get('transaction_id')}  value={record.get('order_value')}")
