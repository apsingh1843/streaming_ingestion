INSERT INTO customer_dim_data
(
    customer_id,
    customer_name,
    email,
    phone,
    city,
    risk_rating,
    effective_from,
    effective_to,
    is_current
)
SELECT
    customer_id,
    customer_name,
    email,
    phone,
    city,
    risk_rating,
    CURRENT_TIMESTAMP,
    NULL,
    TRUE
FROM customer_changes;