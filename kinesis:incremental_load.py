# ============================================================
# JOB   : praveen_incremental_load
# FLOW  : PostgreSQL → Silver (append) → Gold → Redshift
# GLUE PARAMETERS: --JOB_NAME
#
# KEY: cast_* functions use FloatType for all decimal columns
#      to match the Silver Parquet schema written by initial load.
# ============================================================

import sys, re, logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import (
    col, sha2, to_date, date_format,
    dayofmonth, month, quarter, year,
    dayofweek, when, lit, current_timestamp,
    max as spark_max
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType
)

# ── Init ──────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Paths ─────────────────────────────────────────────────
BUCKET = "praveen-proj"
SILVER = f"s3://{BUCKET}/silver/"
GOLD   = f"s3://{BUCKET}/gold/"

# ── Redshift ──────────────────────────────────────────────
RS_URL    = "jdbc:redshift://wg2502.666127452756.eu-west-2.redshift-serverless.amazonaws.com:5439/praveen_db"
RS_USER   = "admin"
RS_PWD    = "Passw0rd"
RS_DRIVER = "com.amazon.redshift.jdbc42.Driver"

# ── PostgreSQL ────────────────────────────────────────────
PG_URL   = "jdbc:postgresql://13.42.152.118:5432/testdb"
PG_PROPS = {"user": "admin", "password": "admin123",
            "driver": "org.postgresql.Driver"}

# ── Helpers ───────────────────────────────────────────────
def read_pg(table):
    logger.info(f"==> PostgreSQL: {table}")
    return spark.read.jdbc(url=PG_URL, table=table, properties=PG_PROPS)

def clean(df):
    df = df.dropDuplicates().dropna()
    for c in df.columns:
        df = df.withColumnRenamed(c, re.sub(r'\W+', '_', c.strip().lower()))
    return df

def read_silver(name):
    return spark.read.parquet(SILVER + name + "/")

def save_parquet(df, path, mode="overwrite"):
    df.write.mode(mode).parquet(path)

def execute_sql(sql):
    conn = spark._sc._jvm.java.sql.DriverManager.getConnection(RS_URL, RS_USER, RS_PWD)
    stmt = conn.createStatement()
    stmt.execute(sql)
    stmt.close()
    conn.close()

def load_to_redshift(df, table):
    logger.info(f"==> Loading {table}")
    execute_sql(f"TRUNCATE TABLE {table};")
    df.write \
        .format("jdbc") \
        .option("url",       RS_URL) \
        .option("dbtable",   table) \
        .option("user",      RS_USER) \
        .option("password",  RS_PWD) \
        .option("driver",    RS_DRIVER) \
        .option("batchsize", "10000") \
        .mode("append") \
        .save()
    logger.info(f"==> {table}: done")

def cast_customers(df):
    """
    Cast JDBC result to match Silver schema (FloatType for decimals,
    IntegerType for ints, StringType for IDs and dates).
    """
    return df.select(
        col("customer_id").cast(StringType()),
        col("age").cast(IntegerType()),
        col("gender").cast(StringType()),
        col("country").cast(StringType()),
        col("registration_date").cast(StringType()),
        col("loyalty_score").cast(FloatType()),
        col("lifetime_value").cast(FloatType()),
        col("churn_label").cast(IntegerType()),
    )

def cast_products(df):
    return df.select(
        col("product_id").cast(StringType()),
        col("category").cast(StringType()),
        col("price").cast(FloatType()),
        col("margin_percentage").cast(FloatType()),
        col("popularity_score").cast(FloatType()),
    )

def cast_behavior(df):
    return df.select(
        col("customer_id").cast(StringType()),
        col("avg_session_time").cast(FloatType()),
        col("pages_per_session").cast(IntegerType()),
        col("cart_abandon_rate").cast(FloatType()),
        col("return_rate").cast(FloatType()),
        col("support_tickets").cast(IntegerType()),
        col("review_score").cast(FloatType()),
        col("behavior_churn_signal").cast(IntegerType()),
    )


# ============================================================
# STEP 1 — POSTGRESQL → SILVER (append new rows only)
# ============================================================
logger.info("STEP 1: PostgreSQL → Silver")

# customers2
raw_cust = cast_customers(clean(read_pg("praveen.customers2")))
last_cid = read_silver("customers").select(spark_max("customer_id")).collect()[0][0]
new_cust = raw_cust.filter(col("customer_id") > last_cid)
n_cust   = new_cust.count()
logger.info(f"New customers: {n_cust} (watermark: {last_cid})")
if n_cust > 0:
    save_parquet(new_cust, SILVER + "customers/", mode="append")

# products_incremental
raw_prod = cast_products(clean(read_pg("praveen.products_incremental")))
last_pid = read_silver("products").select(spark_max("product_id")).collect()[0][0]
new_prod = raw_prod.filter(col("product_id") > last_pid)
n_prod   = new_prod.count()
logger.info(f"New products: {n_prod} (watermark: {last_pid})")
if n_prod > 0:
    save_parquet(new_prod, SILVER + "products/", mode="append")

# behavior_incremental
raw_beh  = cast_behavior(clean(read_pg("praveen.behavior_incremental")))
existing = read_silver("behavior").select("customer_id")
new_beh  = raw_beh.join(existing, "customer_id", "left_anti")
n_beh    = new_beh.count()
logger.info(f"New behavior rows: {n_beh}")
if n_beh > 0:
    save_parquet(new_beh, SILVER + "behavior/", mode="append")

logger.info("Step 1 done")


# ============================================================
# STEP 2 — READ FULL SILVER
# ============================================================
logger.info("STEP 2: Full Silver")

customers    = read_silver("customers")
products     = read_silver("products")
behavior     = read_silver("behavior")
transactions = read_silver("transactions")
logger.info("Step 2 done")


# ============================================================
# STEP 3 — DIMENSIONS
# ============================================================
logger.info("STEP 3: Dimensions")

dim_customer = customers \
    .dropDuplicates(["customer_id"]) \
    .withColumn("customer_key", sha2(col("customer_id"), 256)) \
    .withColumn("registration_date", to_date(col("registration_date"))) \
    .select(
        "customer_key", "customer_id", "age", "gender", "country",
        "registration_date", "loyalty_score", "lifetime_value", "churn_label"
    )

dim_product = products \
    .dropDuplicates(["product_id"]) \
    .withColumn("product_key", sha2(col("product_id"), 256)) \
    .select(
        "product_key", "product_id", "category",
        "price", "margin_percentage", "popularity_score"
    )

dim_payment = transactions \
    .select("payment_method").distinct().dropna() \
    .withColumn("payment_method_key", sha2(col("payment_method"), 256)) \
    .select("payment_method_key", "payment_method")

dim_date = transactions \
    .select(to_date(col("order_date")).alias("full_date")) \
    .distinct().dropna() \
    .withColumn("date_key",    date_format("full_date", "yyyyMMdd").cast(IntegerType())) \
    .withColumn("day",         dayofmonth("full_date")) \
    .withColumn("month",       month("full_date")) \
    .withColumn("month_name",  date_format("full_date", "MMMM")) \
    .withColumn("quarter",     quarter("full_date")) \
    .withColumn("year",        year("full_date")) \
    .withColumn("day_of_week", date_format("full_date", "EEEE")) \
    .withColumn("is_weekend",
        when(dayofweek("full_date").isin(1, 7), True).otherwise(False)) \
    .select(
        "date_key", "full_date", "day", "month", "month_name",
        "quarter", "year", "day_of_week", "is_weekend"
    )

logger.info("Step 3 done")


# ============================================================
# STEP 4 — FACTS
# ============================================================
logger.info("STEP 4: Facts")

txn_base = transactions.select(
    col("transaction_id"),
    col("customer_id"),
    col("product_id"),
    col("payment_method"),
    col("order_date"),
    col("order_value"),
    col("discount_applied"),
    col("shipping_delay_days"),
    col("fraud_label"),
    col("device_type")
).withColumn(
    "date_key",
    date_format(to_date(col("order_date")), "yyyyMMdd").cast(IntegerType())
).withColumn(
    "net_value",
    col("order_value") * (1 - col("discount_applied"))
).withColumn("load_type",   lit("INCREMENTAL")) \
 .withColumn("ingested_at", current_timestamp())

fact_txn = txn_base \
    .join(dim_customer.select("customer_key", "customer_id"), "customer_id", "left") \
    .join(dim_product.select("product_key",   "product_id"),  "product_id",  "left") \
    .join(dim_payment.select("payment_method_key", "payment_method"), "payment_method", "left") \
    .select(
        "customer_key",
        "product_key",
        "payment_method_key",
        "date_key",
        "transaction_id",
        "customer_id",
        "product_id",
        "payment_method",
        "order_value",
        "discount_applied",
        "net_value",
        "shipping_delay_days",
        "fraud_label",
        "device_type",
        "load_type",
        "ingested_at"
    )

fact_beh = behavior \
    .join(dim_customer.select("customer_key", "customer_id"), "customer_id", "left") \
    .withColumn("load_type",   lit("INCREMENTAL")) \
    .withColumn("ingested_at", current_timestamp()) \
    .select(
        "customer_key",
        "customer_id",
        "avg_session_time",
        "pages_per_session",
        "cart_abandon_rate",
        "return_rate",
        "support_tickets",
        "review_score",
        "behavior_churn_signal",
        "load_type",
        "ingested_at"
    )

logger.info("Step 4 done")


# ============================================================
# STEP 5 — GOLD S3
# ============================================================
logger.info("STEP 5: Gold")

save_parquet(dim_customer, GOLD + "dim_customer/")
save_parquet(dim_product,  GOLD + "dim_product/")
save_parquet(dim_payment,  GOLD + "dim_payment/")
save_parquet(dim_date,     GOLD + "dim_date/")
save_parquet(fact_txn,     GOLD + "fact_transactions/")
save_parquet(fact_beh,     GOLD + "fact_behavior/")
logger.info("Step 5 done")


# ============================================================
# STEP 6 — REDSHIFT
# ============================================================
logger.info("STEP 6: Redshift")

load_to_redshift(dim_customer, "praveen.dim_customer")
load_to_redshift(dim_product,  "praveen.dim_product")
load_to_redshift(dim_payment,  "praveen.dim_payment")
load_to_redshift(dim_date,     "praveen.dim_date")
load_to_redshift(fact_txn,     "praveen.fact_transactions")
load_to_redshift(fact_beh,     "praveen.fact_behavior")

logger.info("INCREMENTAL LOAD COMPLETE")
job.commit()
