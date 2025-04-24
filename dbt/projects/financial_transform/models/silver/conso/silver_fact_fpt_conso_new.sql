{{ config(
    materialized='table'
) }}
WITH BASE
    AS (SELECT fva.date,
               COALESCE(fva.value, 0)     AS value,
               COALESCE(TRIM(UPPER(fva.expense_code)),' ') AS expense_code,
               COALESCE(TRIM(UPPER(fva.department_code)),' ') AS department_code
        FROM {{ source('bronze', 'fact_fpt_conso_new')}} fva)
SELECT * FROM BASE
WHERE NOT (
    value= 0
    OR department_code=' '
    OR department_code='0'
    OR expense_code='0'
    OR expense_code=' '
)
