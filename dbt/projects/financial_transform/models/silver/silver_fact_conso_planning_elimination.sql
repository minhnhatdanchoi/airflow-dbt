{{ config(
    materialized='table'
) }}
WITH BASE
    AS (SELECT fva.accounting_date,
               COALESCE(fva.management_report_value, 0)     AS management_report_value,
               COALESCE(TRIM(UPPER(fva.expense_code)),' ') AS expense_code,
               COALESCE(TRIM(UPPER(fva.department_code)),' ') AS department_code,
               COALESCE(TRIM(fva.type), ' ') AS type
        FROM {{ source('bronze', 'fact_conso_planning_elimination')}} fva)
SELECT * FROM BASE
WHERE NOT (
    management_report_value= 0
    OR management_report_value IS NULL
    OR department_code=' '
    OR department_code='0'
    OR expense_code='0'
    OR expense_code=' '
)
