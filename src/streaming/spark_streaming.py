
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaToS3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_events") \
    .load()

# Convert value from binary to string and parse JSON
json_df = df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Write to MinIO (S3)
query = parsed_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "s3a://bronze/checkpoints/") \
    .option("path", "s3a://bronze/user_events/") \
    .outputMode("append") \
    .start()

query.awaitTermination()