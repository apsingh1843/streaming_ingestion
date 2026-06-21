
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("CustUpdateStreaming") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

schema = StructType([
    StructField("customer_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("city", StringType()),
    StructField("risk_rating", StringType()),
    StructField("update_timestamp", StringType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "customer_updates") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
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
    .option("checkpointLocation", "s3a://banking-data-ap-south-1/checkpoints/customer_updates/") \
    .option("path", "s3a://banking-data-ap-south-1/bronze/customers/") \
    .partitionBy("year", "month", "day") \
    .outputMode("append") \
    .start()

query.awaitTermination()