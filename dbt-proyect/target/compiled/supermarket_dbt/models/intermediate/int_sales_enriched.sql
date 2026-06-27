WITH base AS (
    SELECT * FROM "supermarket"."public"."stg_sales"
),

cleaned AS (
    SELECT
        transaction_id, store_id, raw_amount, currency, processed_at, batch_id,
        replace(
            replace(
                regexp_replace(
                    status_history::text,
                    'datetime\.datetime\((\d+),\s*(\d+),\s*(\d+),\s*(\d+),\s*(\d+)[^)]*\)',
                    '"\1-\2-\3T\4:\5:00Z"',
                    'g'
                ),
                E'}\n {', '}, {'
            ),
            '''', '"'
        )::jsonb AS status_history_json
    FROM base
),

created_events AS (
    SELECT transaction_id, MIN((event->>'date')::timestamp) AS created_at
    FROM cleaned, jsonb_array_elements(status_history_json) AS event
    WHERE event->>'status' = 'CREATED'
    GROUP BY transaction_id
),

completed_events AS (
    SELECT transaction_id, MIN((event->>'date')::timestamp) AS completed_at
    FROM cleaned, jsonb_array_elements(status_history_json) AS event
    WHERE event->>'status' = 'COMPLETED'
    GROUP BY transaction_id
),

refunded_events AS (
    SELECT DISTINCT transaction_id, TRUE AS is_refunded
    FROM cleaned, jsonb_array_elements(status_history_json) AS event
    WHERE event->>'status' = 'REFUNDED'
)

SELECT
    b.transaction_id, b.store_id, b.raw_amount, b.currency,
    b.processed_at, b.batch_id,
    cr.created_at, co.completed_at,
    ROUND(EXTRACT(EPOCH FROM (co.completed_at - cr.created_at)) / 3600.0, 2) AS conversion_time_hours,
    COALESCE(re.is_refunded, FALSE) AS is_refunded
FROM cleaned b
LEFT JOIN created_events   cr ON b.transaction_id = cr.transaction_id
LEFT JOIN completed_events co ON b.transaction_id = co.transaction_id
LEFT JOIN refunded_events  re ON b.transaction_id = re.transaction_id