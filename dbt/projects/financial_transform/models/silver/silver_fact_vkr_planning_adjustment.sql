{{ config(
    materialized='table'
) }}
WITH BASE
    AS (SELECT fva.accounting_date,
               COALESCE(fva.financial_report_value, 0)  AS financial_report_value,
               COALESCE(fva.management_report_value, 0) AS management_report_value,
               COALESCE(TRIM(UPPER(fva.expense_code)),' ') AS expense_code,
               COALESCE(TRIM(UPPER(fva.department_code)),' ') AS department_code
        FROM {{ source('bronze', 'fact_vkr_planning_adjustment')}} fva)
SELECT * FROM BASE
WHERE NOT (
    (financial_report_value = 0
    AND management_report_value= 0)
    OR department_code=' '
    OR department_code='0'
    OR expense_code='0'
    OR expense_code=' '
)
