from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, current_timestamp, lit, max as spark_max)
from src.utils.postgres_utils import get_last_processed_timestamp, update_last_processed_timestamp, insert_run_history
import uuid
from datetime import datetime

spark = SparkSession.builder \
    .appName("TransBronzeToSilver") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

###################################### Variables ####################################

BUCKET_NAME = "banking-data-ap-south-1"

run_id = str(uuid.uuid4())
job_name = "trans_bronze_to_silver"
start_time = datetime.now()

################################## Main Processing Logic ################################

try:
    # Read from S3 Bronze Layer
    bronze_df = spark.read.parquet(f"s3a://{BUCKET_NAME}/bronze/transactions/")

    #print(f"Total records in Bronze Layer: {bronze_df.count()}")

    # Incremental Load Logic using Watermark
    last_processed_timestamp = get_last_processed_timestamp(job_name)

    incremental_df = bronze_df.filter(col("ingestion_time") > lit(last_processed_timestamp)) if last_processed_timestamp else bronze_df

    records_read = incremental_df.count()

    # Data Quality Checks for Silver Layer
    validated_df = (
        incremental_df
        .filter(col("transaction_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("account_id").isNotNull())
        .filter(col("amount") > 0)
    )

    records_rejected = records_read - validated_df.count()
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

    records_written = silver_df.count()

    # Exit if there are no new records to process
    if records_read == 0:
        insert_run_history(
            run_id=run_id,
            job_name=job_name,
            records_read=0,
            records_written=0,
            records_rejected=0,
            start_time=start_time,
            end_time=datetime.now(),
            status="SUCCESS"
        )
        print("No new records to process. Exiting.")
        spark.stop()
        exit(0)

    # Write to S3 Silver Layer
    silver_df.write \
        .mode("append") \
        .partitionBy("year", "month", "day") \
        .parquet(f"s3a://{BUCKET_NAME}/silver/transactions/")

    # Update watermark in PostgreSQL with the maximum ingestion_timestamp from the processed batch
    max_timestamp = silver_df.agg(spark_max("ingestion_time")).collect()[0][0]
    update_last_processed_timestamp(max_timestamp, job_name)

    insert_run_history(
        run_id=run_id,
        job_name=job_name,
        records_read=records_read,
        records_written=records_written,
        records_rejected=records_rejected,
        start_time=start_time,
        end_time=datetime.now(),
        status="SUCCESS"
    )

    print(f"Data successfully written to Silver Layer at s3a://{BUCKET_NAME}/silver/transactions/")

    spark.stop()
    exit(0)

except Exception as e:
    insert_run_history(
        run_id=run_id,
        job_name=job_name,
        records_read=0,
        records_written=0,
        records_rejected=0,
        start_time=start_time,
        end_time=datetime.now(),
        status="FAILED",
        error_message=str(e)
    )

    print(f"Error during processing: {e}")
    spark.stop()
    exit(1)