# ============================================================
# JOB   : praveen_initial_load
# FLOW  : Bronze CSV → Silver Parquet → Gold Parquet → Redshift
# GLUE PARAMETERS: --JOB_NAME
#
# TYPES: FloatType used for all decimal columns.
#        Spark inferSchema reads 37.45, 73.2 etc as FloatType.
#        Using FloatType in explicit schema matches this exactly
#        so initial + incremental Silver files are consistent.
# ============================================================

import sys, re, logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import (
    col, sha2, to_date, date_format,
    dayofmonth, month, quarter, year,
    dayofweek, when, lit, current_timestamp
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
BRONZE = f"s3://{BUCKET}/bronze/"
SILVER = f"s3://{BUCKET}/silver/"
GOLD   = f"s3://{BUCKET}/gold/"

# ── Redshift ──────────────────────────────────────────────
RS_URL    = "jdbc:redshift://wg2502.666127452756.eu-west-2.redshift-serverless.amazonaws.com:5439/praveen_db"
RS_USER   = "admin"
RS_PWD    = "Passw0rd"
RS_DRIVER = "com.amazon.redshift.jdbc42.Driver"

# ── Schemas — FloatType for all decimal columns ───────────
# customers1.csv
SCHEMA_CUSTOMERS = StructType([
    StructField("customer_id",       StringType(),  True),
    StructField("age",               IntegerType(), True),
    StructField("gender",            StringType(),  True),
    StructField("country",           StringType(),  True),
    StructField("registration_date", StringType(),  True),
    StructField("loyalty_score",     FloatType(),   True),
    StructField("lifetime_value",    FloatType(),   True),
    StructField("churn_label",       IntegerType(), True),
])

# transactions1.csv
SCHEMA_TRANSACTIONS = StructType([
    StructField("transaction_id",      StringType(),  True),
    StructField("customer_id",         StringType(),  True),
    StructField("product_id",          StringType(),  True),
    StructField("order_date",          StringType(),  True),
    StructField("order_value",         FloatType(),   True),
    StructField("payment_method",      StringType(),  True),
    StructField("device_type",         StringType(),  True),
    StructField("discount_applied",    FloatType(),   True),
    StructField("shipping_delay_days", IntegerType(), True),
    StructField("fraud_label",         IntegerType(), True),
])

# behavior.csv
SCHEMA_BEHAVIOR = StructType([
    StructField("customer_id",           StringType(),  True),
    StructField("avg_session_time",      FloatType(),   True),
    StructField("pages_per_session",     IntegerType(), True),
    StructField("cart_abandon_rate",     FloatType(),   True),
    StructField("return_rate",           FloatType(),   True),
    StructField("support_tickets",       IntegerType(), True),
    StructField("review_score",          FloatType(),   True),
    StructField("behavior_churn_signal", IntegerType(), True),
])

# products.csv
SCHEMA_PRODUCTS = StructType([
    StructField("product_id",        StringType(), True),
    StructField("category",          StringType(), True),
    StructField("price",             FloatType(),  True),
    StructField("margin_percentage", FloatType(),  True),
    StructField("popularity_score",  FloatType(),  True),
])

# ── Helpers ───────────────────────────────────────────────
def read_csv(path, schema):
    return spark.read \
        .option("header", True) \
        .schema(schema) \
        .csv(path)

def clean(df):
    df = df.dropDuplicates().dropna()
    for c in df.columns:
        df = df.withColumnRenamed(c, re.sub(r'\W+', '_', c.strip().lower()))
    return df

def save_parquet(df, path):
    df.write.mode("overwrite").parquet(path)

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


# ============================================================
# STEP 1 — BRONZE → SILVER
# ============================================================
logger.info("STEP 1: Bronze → Silver")

customers    = clean(read_csv(BRONZE + "customers/customers.csv",       SCHEMA_CUSTOMERS))
products     = clean(read_csv(BRONZE + "products/products.csv",         SCHEMA_PRODUCTS))
behavior     = clean(read_csv(BRONZE + "behavior/behavior.csv",         SCHEMA_BEHAVIOR))
transactions = clean(read_csv(BRONZE + "transactions/transactions.csv", SCHEMA_TRANSACTIONS))

save_parquet(customers,    SILVER + "customers/")
save_parquet(products,     SILVER + "products/")
save_parquet(behavior,     SILVER + "behavior/")
save_parquet(transactions, SILVER + "transactions/")
logger.info("Step 1 done")


# ============================================================
# STEP 2 — DIMENSIONS
# ============================================================
logger.info("STEP 2: Dimensions")

# DDL: customer_key, customer_id, age, gender, country,
#      registration_date, loyalty_score, lifetime_value, churn_label
dim_customer = customers \
    .dropDuplicates(["customer_id"]) \
    .withColumn("customer_key", sha2(col("customer_id"), 256)) \
    .withColumn("registration_date", to_date(col("registration_date"))) \
    .select(
        "customer_key", "customer_id", "age", "gender", "country",
        "registration_date", "loyalty_score", "lifetime_value", "churn_label"
    )

# DDL: product_key, product_id, category,
#      price, margin_percentage, popularity_score
dim_product = products \
    .dropDuplicates(["product_id"]) \
    .withColumn("product_key", sha2(col("product_id"), 256)) \
    .select(
        "product_key", "product_id", "category",
        "price", "margin_percentage", "popularity_score"
    )

# DDL: payment_method_key, payment_method
dim_payment = transactions \
    .select("payment_method").distinct().dropna() \
    .withColumn("payment_method_key", sha2(col("payment_method"), 256)) \
    .select("payment_method_key", "payment_method")

# DDL: date_key, full_date, day, month, month_name,
#      quarter, year, day_of_week, is_weekend
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

logger.info("Step 2 done")


# ============================================================
# STEP 3 — FACTS
# ============================================================
logger.info("STEP 3: Facts")

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
).withColumn("load_type",   lit("INITIAL")) \
 .withColumn("ingested_at", current_timestamp())

# DDL: customer_key, product_key, payment_method_key, date_key,
#      transaction_id, customer_id, product_id, payment_method,
#      order_value, discount_applied, net_value,
#      shipping_delay_days, fraud_label, device_type,
#      load_type, ingested_at
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

# DDL: customer_key, customer_id, avg_session_time,
#      pages_per_session, cart_abandon_rate, return_rate,
#      support_tickets, review_score, behavior_churn_signal,
#      load_type, ingested_at
fact_beh = behavior \
    .join(dim_customer.select("customer_key", "customer_id"), "customer_id", "left") \
    .withColumn("load_type",   lit("INITIAL")) \
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

logger.info("Step 3 done")


# ============================================================
# STEP 4 — GOLD S3
# ============================================================
logger.info("STEP 4: Gold")

save_parquet(dim_customer, GOLD + "dim_customer/")
save_parquet(dim_product,  GOLD + "dim_product/")
save_parquet(dim_payment,  GOLD + "dim_payment/")
save_parquet(dim_date,     GOLD + "dim_date/")
save_parquet(fact_txn,     GOLD + "fact_transactions/")
save_parquet(fact_beh,     GOLD + "fact_behavior/")
logger.info("Step 4 done")


# ============================================================
# STEP 5 — REDSHIFT
# ============================================================
logger.info("STEP 5: Redshift")

load_to_redshift(dim_customer, "praveen.dim_customer")
load_to_redshift(dim_product,  "praveen.dim_product")
load_to_redshift(dim_payment,  "praveen.dim_payment")
load_to_redshift(dim_date,     "praveen.dim_date")
load_to_redshift(fact_txn,     "praveen.fact_transactions")
load_to_redshift(fact_beh,     "praveen.fact_behavior")

logger.info("INITIAL LOAD COMPLETE")
job.commit()
