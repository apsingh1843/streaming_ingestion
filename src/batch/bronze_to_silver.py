from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, current_timestamp, lit, max as spark_max)
import psycopg2
import uuid
from datetime import datetime

spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

###################################### Variables ####################################

BUCKET_NAME = "banking-data-ap-south-1"

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "banking_platform",
    "user": "admin",
    "password": "admin"
}

run_id = str(uuid.uuid4())
job_name = "bronze_to_silver"
start_time = datetime.now()

################################## Utility Functions ################################

# Function to get the last processed timestamp from PostgreSQL
def get_last_processed_timestamp():
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT last_processed_timestamp FROM pipeline_metadata WHERE job_name = 'bronze_to_silver';")
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"Error fetching last processed timestamp: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Function to update the last processed timestamp (watermark) in PostgreSQL
def update_last_processed_timestamp(max_timestamp):
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        cursor.execute("UPDATE pipeline_metadata SET last_processed_timestamp = %s, last_run_status = 'SUCCESS' WHERE job_name = %s;", (max_timestamp, 'bronze_to_silver'))
        conn.commit()
    except Exception as e:
        print(f"Error updating last processed timestamp: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Function to insert current status in run history table
def insert_run_history(run_id, job_name, records_read, records_written, records_rejected, start_time, end_time, status, error_message=None):
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()

        cursor.execute("INSERT INTO pipeline_run_history (run_id, job_name, records_read, records_written, records_rejected, start_time, end_time, status, error_message) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);", (run_id, job_name, records_read, records_written, records_rejected, start_time, end_time, status, error_message))

        conn.commit()
    except Exception as e:
        print(f"Error inserting run history: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


################################## Main Processing Logic ################################

try:
    # Read from S3 Bronze Layer
    bronze_df = spark.read.parquet(f"s3a://{BUCKET_NAME}/bronze/transactions/")

    #print(f"Total records in Bronze Layer: {bronze_df.count()}")

    # Incremental Load Logic using Watermark
    last_processed_timestamp = get_last_processed_timestamp()

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
    update_last_processed_timestamp(max_timestamp)

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