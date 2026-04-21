# ============================================================
# LAMBDA  : dynamo_stream_to_s3           (STEP 3)
# TRIGGER : DynamoDB Streams on praveen_transactions
#           Event type: NEW_IMAGE
# REGION  : eu-west-2
#
# DOES:
#   Reads new records from DynamoDB stream
#   Writes them to S3 Bronze as CSV
#   Glue reads this CSV for batch processing
#
# CSV FORMAT:
#   - Header row included
#   - Exactly 10 columns in same order as original transactions_25k.csv
#   - Unix line endings (\n) — required for Glue on Linux
#   - ingested_at excluded (DynamoDB-only field, not in Glue schema)
#
# COLUMNS (matches SCHEMA_TRANSACTIONS in glue_initial_load.py):
#   transaction_id, customer_id, product_id, order_date,
#   order_value, payment_method, device_type,
#   discount_applied, shipping_delay_days, high_value_flag
#
# S3 PATH:
#   bronze/transactions/YYYY/MM/DD/HH/<timestamp>.csv
#
# ENV VARS:
#   S3_BUCKET = praveen-proj
# ============================================================

import json
import boto3
import os
import csv
import io
from datetime import datetime

s3     = boto3.client("s3", region_name="eu-west-2")
BUCKET = os.environ.get("S3_BUCKET", "praveen-proj")

# Exactly 10 columns — same order as transactions_25k.csv header
COLUMNS = [
    "transaction_id",
    "customer_id",
    "product_id",
    "order_date",
    "order_value",
    "payment_method",
    "device_type",
    "discount_applied",
    "shipping_delay_days",
    "high_value_flag",
]


def lambda_handler(event, context):
    records = []

    for stream_record in event.get("Records", []):
        if stream_record.get("eventName") not in ("INSERT", "MODIFY"):
            continue

        new_image = stream_record.get("dynamodb", {}).get("NewImage")
        if not new_image:
            continue

        record = deserialize(new_image)
        records.append(record)

    if not records:
        print("No INSERT/MODIFY records — nothing to write")
        return {"statusCode": 200, "written": 0}

    write_csv_to_s3(records)
    print(f"Written {len(records)} records to S3 Bronze")
    return {"statusCode": 200, "written": len(records)}


def deserialize(dynamo_item):
    """
    Convert DynamoDB NewImage format to plain Python dict.
    DynamoDB stream sends: {"field": {"S": "value"}} or {"N": "123"}
    """
    result = {}
    for key, value in dynamo_item.items():
        if "S"      in value: result[key] = value["S"]
        elif "N"    in value:
            n = value["N"]
            result[key] = int(n) if "." not in n else float(n)
        elif "BOOL" in value: result[key] = value["BOOL"]
        elif "NULL" in value: result[key] = None
        else: result[key] = str(value)
    return result


def write_csv_to_s3(records):
    """
    Write records as CSV to S3 Bronze.

    lineterminator='\\n' — Unix line endings.
    Glue runs on Linux and expects \\n not \\r\\n.
    Windows-style \\r\\n causes Glue to read garbage characters
    at the end of each value.

    extrasaction='ignore' — drops ingested_at and any other
    DynamoDB-only fields. Only COLUMNS are written.
    """
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=COLUMNS,
        extrasaction="ignore",
        lineterminator="\n"
    )
    writer.writeheader()
    writer.writerows(records)

    now = datetime.utcnow()
    key = (
        f"bronze/transactions1/"
        f"{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}/"
        f"{now.strftime('%Y%m%d_%H%M%S_%f')}.csv"
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buf.getvalue(),
        ContentType="text/csv"
    )
    print(f"S3 Bronze: s3://{BUCKET}/{key}  ({len(records)} rows)")
