from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, current_timestamp, lit)

spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

BUCKET_NAME = "banking-data-ap-south-1"

# Read from S3 Bronze Layer
bronze_df = spark.read.parquet(f"s3a://{BUCKET_NAME}/bronze/transactions/")

#print(f"Total records in Bronze Layer: {bronze_df.count()}")

# Data Quality Checks for Silver Layer
validated_df = (
    bronze_df
    .filter(col("transaction_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("account_id").isNotNull())
    .filter(col("amount") > 0)
)

#print(f"Total valid records count: {validated_df.count()}")

#Deduplicate records based on transaction_id
silver_df = validated_df.dropDuplicates(["transaction_id"])

#print(f"Total records after deduplication: {silver_df.count()}")

# Add metadata columns for Silver Layer
silver_df = (
    silver_df
    .withColumn("processed_timestamp", current_timestamp())
    .withColumn("source_layer", lit("bronze"))
)

# Write to S3 Silver Layer
silver_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(f"s3a://{BUCKET_NAME}/silver/transactions/")

print(f"Data successfully written to Silver Layer at s3a://{BUCKET_NAME}/silver/transactions/")