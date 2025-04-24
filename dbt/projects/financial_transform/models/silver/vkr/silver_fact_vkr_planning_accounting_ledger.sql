{{ config(
    materialized='table'
) }}
WITH BASE
    AS (SELECT fval.accounting_date,
               COALESCE(fval.financial_report_value, 0)  AS financial_report_value,
               COALESCE(fval.management_report_value, 0) AS management_report_value,
               COALESCE(TRIM(UPPER(fval.expense_code)),' ') AS expense_code,
               COALESCE(TRIM(UPPER(fval.department_code)),' ') AS department_code
        FROM {{ source('bronze', 'fact_vkr_planning_accounting_ledger')}} fval)
SELECT * FROM BASE
WHERE NOT (
    (financial_report_value = 0
    AND management_report_value= 0)
    OR department_code=' '
    OR department_code='0'
    OR expense_code='0'
    OR expense_code='90C'
    OR expense_code=' '
)
