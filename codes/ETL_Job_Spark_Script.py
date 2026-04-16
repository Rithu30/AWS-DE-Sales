import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import (
    col, round as spark_round, when, to_date,
    upper, trim, concat_ws, year, month, dayofweek
)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc   = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job   = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── STEP 1: READ from Athena/Glue catalog ──────────────────────────────────
# Replace 'your_database' and 'sales_analytics' with your actual Glue DB and table name
dyf = glueContext.create_dynamic_frame.from_catalog(
    database   = "my-sales-data-catalog-db",        # ← your Glue database name
    table_name = "sales_analytics"       # ← the table Athena created
)
df = dyf.toDF()

print(">>> Raw row count:", df.count())
df.printSchema()

# ── STEP 2: REMOVE duplicate records ──────────────────────────────────────
df = df.dropDuplicates(["order_id"])
print(">>> After dedup:", df.count())

# ── STEP 3: DROP unwanted / redundant columns ─────────────────────────────
# Drop split name columns — customer_name already has the full name
cols_to_drop = ["first_name", "last_name"]
existing = [c for c in cols_to_drop if c in df.columns]
df = df.drop(*existing)

# ── STEP 4: HANDLE NULL values ────────────────────────────────────────────
df = df.withColumn("score",
        when(col("score").isNull(), 0)
        .otherwise(col("score").cast("integer")))

df = df.withColumn("quantity",
        when(col("quantity").isNull(), 1)
        .otherwise(col("quantity").cast("integer")))

df = df.withColumn("country",
        when(col("country").isNull(), "Unknown")
        .otherwise(col("country")))

# ── STEP 5: FIX data types and floating-point drift ───────────────────────
df = df.withColumn("sales",    spark_round(col("sales").cast("double"),   2))
df = df.withColumn("revenue",  spark_round(col("revenue").cast("double"), 2))
df = df.withColumn("order_date",
        to_date(col("order_date"), "dd/MM/yyyy"))   # adjust format if needed

# ── STEP 6: STANDARDISE text columns ─────────────────────────────────────
df = df.withColumn("product_category", trim(upper(col("product_category"))))
df = df.withColumn("country",          trim(col("country")))
df = df.withColumn("customer_name",    trim(col("customer_name")))

# ── STEP 7: ADD useful derived columns for Power BI ──────────────────────
df = df.withColumn("order_year",      year(col("order_date")))
df = df.withColumn("order_month",     month(col("order_date")))
df = df.withColumn("order_dayofweek", dayofweek(col("order_date")))
df = df.withColumn("revenue_per_unit",
        spark_round(col("revenue") / col("quantity"), 2))

# ── STEP 8: Customer tier based on score ─────────────────────────────────
df = df.withColumn("customer_tier",
        when(col("score") >= 300, "Platinum")
        .when(col("score") >= 100, "Gold")
        .when(col("score") >= 25,  "Silver")
        .otherwise("Bronze"))

print(">>> Final schema:")
df.printSchema()
df.show(5)

# ── STEP 9: WRITE to sales-analytics-processed folder ────────────────────
output_dyf = DynamicFrame.fromDF(df, glueContext, "clean_Processed_data")  #Output data to load into Power BI

glueContext.write_dynamic_frame.from_options(
    frame              = output_dyf,
    connection_type    = "s3",
    connection_options = {
        "path": "s3://de-csv-final-data/sales-analytics-processed/"  # ← your bucket
    },
    format             = "parquet",
    format_options     = {"compression": "snappy"}
)

print(">>> Write complete.")
job.commit()