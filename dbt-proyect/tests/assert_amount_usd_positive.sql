/*
    Test de umbral personalizado: assert_amount_usd_positive
    ──────────────────────────────────────────────────────────
    Valida que ninguna fila en int_sales_enriched tenga un amount_usd
    calculado <= 0 después de la conversión.

    dbt falla el test si esta query retorna alguna fila (filas = errores).
*/

WITH sales_usd AS (
    SELECT
        e.transaction_id,
        e.raw_amount,
        e.currency,
        e.raw_amount / er.rate_to_usd AS amount_usd
    FROM {{ ref('int_sales_enriched') }} e
    LEFT JOIN {{ ref('ref_exchange_rates') }} er
        ON e.currency = er.currency
)

SELECT *
FROM sales_usd
WHERE amount_usd IS NULL
   OR amount_usd <= 0
