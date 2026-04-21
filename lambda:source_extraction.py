import json
import csv
import io
import boto3
import pg8000
 
s3 = boto3.client("s3")
 
def upload_table(conn, query, bucket, key):
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
 
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(column_names)
    writer.writerows(rows)
 
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv"
    )
 
    cursor.close()
 
def lambda_handler(event, context):
    conn = None
 
    try:
        conn = pg8000.connect(
            host="13.42.152.118",
            database="testdb",
            user="admin",
            password="admin123",
            port=5432
        )
 
        bucket = "praveen-proj"
 
        upload_table(
            conn,
            "SELECT * FROM praveen.customers1",
            bucket,
            "bronze/customers/customers.csv"
        )
 
        upload_table(
            conn,
            "SELECT * FROM praveen.behavior_initial",
            bucket,
            "bronze/behavior/behavior.csv"
        )
        upload_table(
            conn,
            "SELECT * FROM praveen.products_initial",
            bucket,
            "bronze/products/products.csv"
        )
        upload_table(
            conn,
            "SELECT * FROM praveen.transactions",
            bucket,
            "bronze/transactions/transactions.csv"
        )
        
 
        return {
            "statusCode": 200,
            "body": json.dumps("Three tables uploaded successfully to S3")
        }
 
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error: {str(e)}")
        }
 
    finally:
        if conn:
            conn.close()
