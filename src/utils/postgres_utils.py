import psycopg2

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "banking_platform",
    "user": "admin",
    "password": "admin"
}

# Function to create and return a new PostgreSQL connection
def get_postgres_connection():
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

# Function to get the last processed timestamp from PostgreSQL
def get_last_processed_timestamp(job_name):
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT last_processed_timestamp FROM pipeline_metadata WHERE job_name = %s;", (job_name,))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"Error fetching last processed timestamp for {job_name}: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Function to update the last processed timestamp (watermark) in PostgreSQL
def update_last_processed_timestamp(max_timestamp, job_name):
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE pipeline_metadata SET last_processed_timestamp = %s, last_run_status = 'SUCCESS' WHERE job_name = %s;", (max_timestamp, job_name))
        conn.commit()
    except Exception as e:
        print(f"Error updating last processed timestamp for {job_name}: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Function to insert current status in run history table
def insert_run_history(run_id, job_name, records_read, records_written, records_rejected, start_time, end_time, status, error_message=None):
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()

        cursor.execute("INSERT INTO pipeline_run_history (run_id, job_name, records_read, records_written, records_rejected, start_time, end_time, status, error_message) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);", (run_id, job_name, records_read, records_written, records_rejected, start_time, end_time, status, error_message))

        conn.commit()
    except Exception as e:
        print(f"Error inserting run history for {job_name}: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Function to run SQL commands from a file
def run_sql_file(cursor, file_path):
    with open(file_path, 'r') as file:
        sql_commands = file.read()
        cursor.execute(sql_commands)