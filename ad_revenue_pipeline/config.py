from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)

IMPRESSIONS_SCHEMA = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("app_id", IntegerType(), nullable=False),
    StructField("country_code", StringType(), nullable=False),
    StructField("advertiser_id", IntegerType(), nullable=False),
])

CLICKS_SCHEMA = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("impression_id", StringType(), nullable=False),
    StructField("revenue", DoubleType(), nullable=False),
])

TOP_N_ADVERTISERS = 5

SPARK_APP_NAME = "ad-revenue-pipeline"
SHUFFLE_PARTITIONS = 200
BROADCAST_THRESHOLD_MB = 10
