CREATE TABLE IF NOT EXISTS customer_silver_stg (
    customer_id VARCHAR(50),
    customer_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(30),
    city VARCHAR(100),
    risk_rating VARCHAR(20),
    update_timestamp TIMESTAMP
);