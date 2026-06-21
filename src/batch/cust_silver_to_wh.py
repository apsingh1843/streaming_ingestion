from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, current_timestamp, lit, max as spark_max, row_number, to_timestamp)
from src.utils.postgres_utils import (get_postgres_connection, get_last_processed_timestamp, update_last_processed_timestamp, insert_run_history, run_sql_file)
import uuid
from pyspark.sql.window import Window
from pathlib import Path
from datetime import datetime

spark = SparkSession.builder \
    .appName("CustSilverToWh") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

###################################### Variables ####################################

BUCKET_NAME = "banking-data-ap-south-1"

POSTGRES_URL = "jdbc:postgresql://localhost:5432/banking_platform"

POSTGRES_PROPERTIES = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

SQL_DIR = Path("src/sql/cust_etl")
run_id = str(uuid.uuid4())
job_name = "cust_silver_to_wh"
start_time = datetime.now()


################################## Main Processing Logic ################################

try:
    # Create tables if they don't exist
    conn = get_postgres_connection()
    cursor = conn.cursor()
    run_sql_file(cursor, SQL_DIR / "create_customer_dim_data.sql")
    run_sql_file(cursor, SQL_DIR / "create_customer_silver_stg.sql")
    run_sql_file(cursor, SQL_DIR / "create_customer_changes.sql")
    conn.commit()

    # Read from S3 Silver Layer
    silver_df = spark.read.parquet(f"s3a://{BUCKET_NAME}/silver/customers/")

    # Incremental Load Logic using Watermark
    last_processed_timestamp = get_last_processed_timestamp(job_name)

    incremental_df = silver_df.filter(col("processed_timestamp") > lit(last_processed_timestamp)) if last_processed_timestamp else silver_df

    records_read = incremental_df.count()

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

    incremental_df = (
        incremental_df
        .withColumn(
            "update_timestamp",
            to_timestamp(
                col("update_timestamp")
            )
        )
    )

    window_spec = Window.partitionBy("customer_id").orderBy(col("update_timestamp").desc())

    latest_customer_df = (
        incremental_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    records_written = latest_customer_df.count()

    # Write the latest records to PostgreSQL staging table
    
    (
        latest_customer_df
            .select(
                "customer_id",
                "customer_name",
                "email",
                "phone",
                "city",
                "risk_rating",
                "update_timestamp"
            )
            .write
            .mode("overwrite")
            .option("truncate", "true")
            .jdbc(
                POSTGRES_URL,
                "customer_silver_stg",
                properties=POSTGRES_PROPERTIES
            )
    )

    print(f"Total records written to PostgreSQL staging table: {records_written}")

    # Execute ETL SQLs for SCD Type 2 logic
    run_sql_file(cursor, SQL_DIR / "load_customer_changes.sql")
    run_sql_file(cursor, SQL_DIR / "insert_new_customers.sql")
    run_sql_file(cursor, SQL_DIR / "expire_existing_customers.sql")
    run_sql_file(cursor, SQL_DIR / "insert_new_versions.sql")

    conn.commit()

    print("SCD Type 2 processing completed successfully.")

    # Update watermark in PostgreSQL with the maximum ingestion_timestamp from the processed batch
    max_timestamp = incremental_df.agg(spark_max("processed_timestamp")).collect()[0][0]
    update_last_processed_timestamp(max_timestamp, job_name)

    insert_run_history(
        run_id=run_id,
        job_name=job_name,
        records_read=records_read,
        records_written=records_written,
        records_rejected=0,
        start_time=start_time,
        end_time=datetime.now(),
        status="SUCCESS"
    )

    cursor.close()
    conn.close()
    spark.stop()

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