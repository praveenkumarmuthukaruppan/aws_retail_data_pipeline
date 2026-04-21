# -*- coding: utf-8 -*-
# LAMBDA NAME : praveen_api_producer
# TRIGGER     : API Gateway  POST /transactions  (batch only, max 100)
# TIMEOUT     : 30 seconds
# MEMORY      : 256 MB
# PERMISSIONS : AmazonKinesisFullAccess
#
# SCHEMA — exactly 10 columns:
#   transaction_id, customer_id, product_id, order_date,
#   order_value, payment_method, device_type,
#   discount_applied, shipping_delay_days, high_value_flag
#
# high_value_flag computed here: 1 if order_value > 950, else 0

import json
import boto3
import uuid
from datetime import datetime

kinesis        = boto3.client("kinesis", region_name="eu-west-2")
STREAM_NAME    = "transactions-stream"
VALID_PAYMENTS = ["UPI", "Debit Card", "Credit Card", "PayPal", "Crypto"]


def http(code, body):
    return {
        "statusCode": code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=str)
    }


def validate(tx):
    errors = []
    for field in ["customer_id", "product_id", "order_value", "payment_method"]:
        if tx.get(field) is None or tx.get(field) == "":
            errors.append(f"Missing required field: {field}")
    try:
        val = float(tx.get("order_value", 0))
        if val <= 0:    errors.append("order_value must be greater than 0")
        if val > 10000: errors.append("order_value must be less than 10000")
    except (ValueError, TypeError):
        errors.append("order_value must be a number")
    if tx.get("payment_method") and tx["payment_method"] not in VALID_PAYMENTS:
        errors.append(f"payment_method must be one of: {', '.join(VALID_PAYMENTS)}")
    return errors


def build(tx):
    order_value = round(float(tx.get("order_value", 0)), 2)
    return {
        "transaction_id":      tx.get("transaction_id") or f"TXN{uuid.uuid4().hex[:6].upper()}",
        "customer_id":         str(tx["customer_id"]),
        "product_id":          str(tx["product_id"]),
        "order_date":          tx.get("order_date") or datetime.utcnow().strftime("%Y-%m-%d"),
        "order_value":         order_value,
        "payment_method":      tx["payment_method"],
        "device_type":         tx.get("device_type", "Unknown"),
        "discount_applied":    float(tx.get("discount_applied", 0)),
        "shipping_delay_days": int(tx.get("shipping_delay_days", 0)),
        "high_value_flag":     1 if order_value > 950 else 0,
    }


def push_to_kinesis(records):
    kinesis_records = [
        {
            "Data":         json.dumps(r).encode("utf-8"),
            "PartitionKey": str(r["customer_id"])
        }
        for r in records
    ]
    resp   = kinesis.put_records(StreamName=STREAM_NAME, Records=kinesis_records)
    failed = resp.get("FailedRecordCount", 0)
    sent   = len(kinesis_records) - failed
    return sent, failed


def lambda_handler(event, context):
    method = event.get("httpMethod", "")

    if method != "POST":
        return http(405, {"error": f"Method {method} not allowed. Use POST /transactions"})

    try:
        raw  = event.get("body", "{}")
        body = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError:
        return http(400, {"error": "Invalid JSON in request body"})

    if "transactions" not in body:
        return http(400, {"error": "Request body must contain a 'transactions' list"})

    txs = body["transactions"]

    if not isinstance(txs, list): return http(400, {"error": "transactions must be a list"})
    if len(txs) == 0:             return http(400, {"error": "transactions list is empty"})
    if len(txs) > 100:            return http(400, {"error": "Maximum 100 transactions per request"})

    errors = []
    for i, tx in enumerate(txs):
        errs = validate(tx)
        if errs:
            errors.append({"index": i, "errors": errs})
    if errors:
        return http(400, {"error": "Validation failed", "details": errors})

    built = [build(tx) for tx in txs]
    try:
        sent, failed = push_to_kinesis(built)
        print(f"==> Kinesis: sent={sent} failed={failed}")
        return http(200, {
            "message":      "Transactions sent to Kinesis",
            "sent":         sent,
            "failed":       failed,
            "total":        len(built),
            "transactions": built
        })
    except Exception as e:
        print(f"==> ERROR: {str(e)}")
        return http(500, {"error": f"Failed to push to Kinesis: {str(e)}"})
