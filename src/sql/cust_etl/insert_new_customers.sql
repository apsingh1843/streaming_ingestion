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
    s.customer_id,
    s.customer_name,
    s.email,
    s.phone,
    s.city,
    s.risk_rating,
    CURRENT_TIMESTAMP,
    NULL,
    TRUE
FROM customer_silver_stg s
LEFT JOIN customer_dim_data d
ON s.customer_id = d.customer_id
AND d.is_current = TRUE
WHERE d.customer_id IS NULL;