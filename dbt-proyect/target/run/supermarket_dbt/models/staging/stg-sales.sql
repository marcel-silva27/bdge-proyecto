
  create view "supermarket"."public"."stg-sales__dbt_tmp"
    
    
  as (
    WITH source_data AS (
    -- dbt leerá directamente de la tabla que inyectó tu pipeline de Beam
    SELECT * FROM raw_sales
)

SELECT 
    id AS transaction_id,
    store AS store_id,
    
    -- Desanidamos el JSON de 'financials' usando sintaxis de Postgres
    (financials::jsonb->>'raw_amount')::numeric AS raw_amount,
    financials::jsonb->>'currency' AS currency,
    
    -- Desanidamos los metadatos
    (metadata::jsonb->>'processed_at')::timestamp AS processed_at,
    metadata::jsonb->>'batch_id' AS batch_id,
    
    -- Mantenemos el historial de estados como JSONB para procesarlo en la capa Gold
    status_history::jsonb AS status_history

FROM source_data
  );