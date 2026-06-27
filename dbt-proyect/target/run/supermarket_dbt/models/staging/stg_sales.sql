
  create view "supermarket"."public"."stg_sales__dbt_tmp"
    
    
  as (
    /*
    stg_sales.sql — Capa Staging
    ─────────────────────────────
    Responsabilidad: leer raw_sales (cargada por Beam) y desanidar
    los campos JSON en columnas tipadas planas.
 
    No aplica lógica de negocio — solo renombra, castea y desanida.
*/
 
WITH source_data AS (
    SELECT * FROM raw_sales
)
 
SELECT
    -- Identificadores
    id                                              AS transaction_id,
    store                                           AS store_id,
 
    -- Financials (desanidado desde JSONB)
    (financials::jsonb->>'raw_amount')::numeric     AS raw_amount,
    financials::jsonb->>'currency'                  AS currency,
 
    -- Metadata (desanidado desde JSONB)
    (metadata::jsonb->>'processed_at')::timestamp   AS processed_at,
    metadata::jsonb->>'batch_id'                    AS batch_id,
 
    -- Status history se mantiene como JSONB para procesarlo en la capa intermedia
    status_history::jsonb                           AS status_history
 
FROM source_data
  );