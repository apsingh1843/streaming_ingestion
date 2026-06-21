UPDATE customer_dim_data d
SET
effective_to = CURRENT_TIMESTAMP,
is_current = FALSE
FROM customer_changes c
WHERE d.customer_id = c.customer_id
AND d.is_current = TRUE;