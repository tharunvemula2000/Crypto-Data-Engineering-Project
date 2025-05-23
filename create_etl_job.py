import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, to_timestamp

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read raw JSON from S3
df_raw = spark.read.json("s3://crypto-dataeng-project/raw/")

# Clean and select relevant columns, casting types and flattening ROI (which can be null)
df_clean = df_raw.select(
    "id",
    "symbol",
    "name",
    "image",
    col("current_price").cast("float"),
    col("market_cap").cast("double"),
    col("market_cap_rank").cast("int"),
    col("fully_diluted_valuation").cast("double"),
    col("total_volume").cast("double"),
    col("high_24h").cast("float"),
    col("low_24h").cast("float"),
    col("price_change_24h").cast("float"),
    col("price_change_percentage_24h").cast("float"),
    col("market_cap_change_24h").cast("double"),
    col("market_cap_change_percentage_24h").cast("float"),
    col("circulating_supply").cast("double"),
    col("total_supply").cast("double"),
    col("max_supply").cast("double"),
    col("ath").cast("float"),
    col("ath_change_percentage").cast("float"),
    to_timestamp("ath_date").alias("ath_date"),
    col("atl").cast("float"),
    col("atl_change_percentage").cast("float"),
    to_timestamp("atl_date").alias("atl_date"),
    # roi can be null, so use getField safely
    col("roi.times").alias("roi_times"),
    col("roi.currency").alias("roi_currency"),
    col("roi.percentage").cast("float").alias("roi_percentage"),
    to_timestamp("last_updated").alias("last_updated")
).withColumn("load_time", current_timestamp())

# Save cleaned data as parquet for fast downstream processing
df_clean.write.mode("overwrite").parquet("s3://crypto-dataeng-project/staging/crypto_clean/")

# Build dimension table (unique cryptos)
dim_crypto = df_clean.select("id", "symbol", "name", "image").dropDuplicates()
dim_crypto.write.mode("overwrite").parquet("s3://crypto-dataeng-project/dim_crypto/")

# Build fact table (metrics and timestamps)
fact_crypto = df_clean.select(
    col("id").alias("crypto_id"),
    "current_price",
    "market_cap",
    "market_cap_rank",
    "fully_diluted_valuation",
    "total_volume",
    "high_24h",
    "low_24h",
    "price_change_24h",
    "price_change_percentage_24h",
    "market_cap_change_24h",
    "market_cap_change_percentage_24h",
    "circulating_supply",
    "total_supply",
    "max_supply",
    "ath",
    "ath_change_percentage",
    "ath_date",
    "atl",
    "atl_change_percentage",
    "atl_date",
    "roi_times",
    "roi_currency",
    "roi_percentage",
    "last_updated",
    "load_time"
)
fact_crypto.write.mode("append").parquet("s3://crypto-dataeng-project/fact_crypto_prices/")

job.commit()
