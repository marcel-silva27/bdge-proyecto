WITH staging_sales AS (
    SELECT * FROM "supermarket"."public"."stg-sales"
)

SELECT 
    store_id,
    currency,
    COUNT(transaction_id) AS total_transactions,
    SUM(raw_amount) AS total_revenue
FROM staging_sales
GROUP BY 1, 2
ORDER BY total_revenue DESC