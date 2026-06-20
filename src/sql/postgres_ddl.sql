-- Create Metadata Table

CREATE TABLE pipeline_metadata (
    job_name VARCHAR(100) PRIMARY KEY,
    last_processed_timestamp TIMESTAMP,
    last_run_status VARCHAR(20),
    updated_at TIMESTAMP
);

-- Insert Initial Watermark

INSERT INTO pipeline_metadata
VALUES (
    'bronze_to_silver',
    '1900-01-01',
    'SUCCESS',
    CURRENT_TIMESTAMP
);

-- Create Run History Table

CREATE TABLE pipeline_run_history (
    run_id UUID PRIMARY KEY,
    job_name VARCHAR(100),
    records_read BIGINT,
    records_written BIGINT,
    records_rejected BIGINT,
    records_duplicates BIGINT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20),
    error_message TEXT
);

-- Verify Watermark

SELECT *
FROM pipeline_metadata;

-- Verify Run History

SELECT *
FROM pipeline_run_history
ORDER BY start_time DESC;