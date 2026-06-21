TRUNCATE TABLE customer_changes;

INSERT INTO customer_changes (
    customer_id,
    customer_name,
    email,
    phone,
    city,
    risk_rating,
    update_timestamp
)
SELECT
    s.customer_id,
    s.customer_name,
    s.email,
    s.phone,
    s.city,
    s.risk_rating,
    s.update_timestamp
FROM customer_silver_stg s
JOIN customer_dim_data d
ON s.customer_id = d.customer_id
WHERE d.is_current = TRUE
AND (
COALESCE(s.city,'') <> COALESCE(d.city,'')
OR COALESCE(s.risk_rating,'') <> COALESCE(d.risk_rating,'')
OR COALESCE(s.phone,'') <> COALESCE(d.phone,'')
OR COALESCE(s.email,'') <> COALESCE(d.email,'')
OR COALESCE(s.customer_name,'') <> COALESCE(d.customer_name,'')
);