{{ config(
    materialized='table'
) }}
WITH BASE
    AS (SELECT fva.accounting_date,
               COALESCE(fva.rate_value, 0)     AS rate_value,
               COALESCE(TRIM(UPPER(fva.department_code)),' ') AS department_code
        FROM {{ source('bronze', 'fact_conso_exchange_rate')}} fva)
SELECT * FROM BASE
WHERE NOT (
    rate_value = 0
    OR department_code=' '
    OR department_code='0'
)
