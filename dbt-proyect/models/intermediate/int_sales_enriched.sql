/*
    int_sales_enriched.sql — Capa Intermedia
    ──────────────────────────────────────────
    Responsabilidad: calcular métricas de ciclo de vida por transacción
    a partir del status_history (JSONB).

    Extrae las marcas de tiempo de los eventos CREATED y COMPLETED para
    calcular el tiempo de conversión, y detecta si la transacción fue
    devuelta (REFUNDED).

    Inputs:  stg_sales
    Outputs: una fila por transaction_id con métricas de tiempo
*/

WITH base AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

-- Extraemos el timestamp del primer estado CREATED
created_events AS (
    SELECT
        transaction_id,
        MIN((event->>'date')::timestamp) AS created_at
    FROM base,
         jsonb_array_elements(status_history) AS event
    WHERE event->>'status' = 'CREATED'
    GROUP BY transaction_id
),

-- Extraemos el timestamp del primer estado COMPLETED
completed_events AS (
    SELECT
        transaction_id,
        MIN((event->>'date')::timestamp) AS completed_at
    FROM base,
         jsonb_array_elements(status_history) AS event
    WHERE event->>'status' = 'COMPLETED'
    GROUP BY transaction_id
),

-- Detectamos si la transacción tiene algún estado REFUNDED
refunded_events AS (
    SELECT DISTINCT
        transaction_id,
        TRUE AS is_refunded
    FROM base,
         jsonb_array_elements(status_history) AS event
    WHERE event->>'status' = 'REFUNDED'
)

SELECT
    b.transaction_id,
    b.store_id,
    b.raw_amount,
    b.currency,
    b.processed_at,
    b.batch_id,

    -- Tiempos de ciclo de vida
    cr.created_at,
    co.completed_at,

    -- Tiempo de conversión en horas (NULL si no hay COMPLETED)
    ROUND(
        EXTRACT(EPOCH FROM (co.completed_at - cr.created_at)) / 3600.0,
        2
    )                                               AS conversion_time_hours,

    -- Flag de devolución
    COALESCE(re.is_refunded, FALSE)                 AS is_refunded

FROM base b
LEFT JOIN created_events   cr ON b.transaction_id = cr.transaction_id
LEFT JOIN completed_events co ON b.transaction_id = co.transaction_id
LEFT JOIN refunded_events  re ON b.transaction_id = re.transaction_id
