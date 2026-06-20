
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("BankingStreaming") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("account_id", StringType()),
    StructField("transaction_type", StringType()),
    StructField("merchant", StringType()),
    StructField("amount", DoubleType()),
    StructField("city", StringType()),
    StructField("risk_rating", StringType()),
    StructField("device_id", StringType()),
    StructField("channel", StringType()),
    StructField("event_timestamp", StringType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "banking_transactions") \
    .load()

# Convert value from binary to string and parse JSON
json_df = df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

output_df = (
    parsed_df
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("year", year(current_timestamp()))
    .withColumn("month", month(current_timestamp()))
    .withColumn("day", dayofmonth(current_timestamp()))
)

# Write to S3
query = output_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "s3a://banking-data-ap-south-1/checkpoints/banking_transactions/") \
    .option("path", "s3a://banking-data-ap-south-1/bronze/transactions/") \
    .partitionBy("year", "month", "day") \
    .outputMode("append") \
    .start()

query.awaitTermination()