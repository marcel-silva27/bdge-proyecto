
  
    

  create  table "supermarket"."public"."fct_sales__dbt_tmp"
  
  
    as
  
  (
    /*
    fct_sales.sql — Capa Gold (Fact Table)
    ───────────────────────────────────────
    Responsabilidad: modelo final de reportabilidad.

    Aplica la conversión monetaria a USD usando ref_exchange_rates,
    y calcula los cuatro KPIs financieros requeridos:
      · total_sales_usd       — ingresos totales en USD por tienda y día
      · avg_conversion_time   — tiempo promedio CREATED→COMPLETED (horas)
      · refund_rate           — % de transacciones devueltas
      · ticket_promedio_usd   — valor medio de venta por tienda

    Inputs:  int_sales_enriched, ref_exchange_rates (seed)
    Output:  tabla física en PostgreSQL (Gold)
*/

WITH enriched AS (
    SELECT * FROM "supermarket"."public"."int_sales_enriched"
),

exchange_rates AS (
    SELECT
        currency,
        rate_to_usd
    FROM "supermarket"."public"."ref_exchange_rates"
),

-- Unimos con tasas de cambio y calculamos amount_usd
sales_usd AS (
    SELECT
        e.transaction_id,
        e.store_id,
        e.currency,
        e.raw_amount,
        e.raw_amount / er.rate_to_usd              AS amount_usd,
        e.created_at::date                          AS sale_date,
        e.conversion_time_hours,
        e.is_refunded
    FROM enriched e
    LEFT JOIN exchange_rates er ON e.currency = er.currency
)

-- KPIs agregados por tienda y día
SELECT
    store_id,
    sale_date,
    currency,

    -- Volumen
    COUNT(transaction_id)                           AS total_transactions,

    -- KPI 1: Ingresos totales normalizados a USD
    ROUND(SUM(amount_usd)::numeric, 2)              AS total_sales_usd,

    -- KPI 2: Tiempo promedio de conversión CREATED → COMPLETED (horas)
    ROUND(AVG(conversion_time_hours)::numeric, 2)   AS avg_conversion_time,

    -- KPI 3: Tasa de devolución (% sobre total de ventas)
    ROUND(
        (SUM(CASE WHEN is_refunded THEN 1 ELSE 0 END)::numeric
         / NULLIF(COUNT(transaction_id), 0)) * 100,
        2
    )                                               AS refund_rate,

    -- KPI 4: Ticket promedio en USD
    ROUND(AVG(amount_usd)::numeric, 2)              AS ticket_promedio_usd

FROM sales_usd
GROUP BY store_id, sale_date, currency
ORDER BY sale_date DESC, total_sales_usd DESC
  );
  