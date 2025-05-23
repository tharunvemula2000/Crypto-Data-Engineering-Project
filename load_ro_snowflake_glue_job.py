import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, [])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("load_to_snowflake", args)

# Snowflake connector config
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sfOptions = {
    "sfURL": "onwctix-imb85886.snowflakecomputing.com",
    "sfUser" : "TH************",
    "sfPassword" : "Th*********",
    "sfDatabase": "CRYPTO_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "CRYPTO_WH",
    "sfRole": "ACCOUNTADMIN"
      # your Secrets Manager secret
}

# Load Parquet data
dim_df = spark.read.parquet("s3://crypto-dataeng-project/dim_crypto/")
fact_df = spark.read.parquet("s3://crypto-dataeng-project/fact_crypto_prices/")

# Write to Snowflake
dim_df.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "DIM_CRYPTO") \
    .mode("overwrite") \
    .save()

fact_df.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "FACT_CRYPTO_PRICES") \
    .mode("append") \
    .save()

job.commit()