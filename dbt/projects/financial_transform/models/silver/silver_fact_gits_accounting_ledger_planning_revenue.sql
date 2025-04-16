{{ config(
    materialized='table'
) }}
WITH BASE AS (SELECT fgal.accounting_date,
                     COALESCE(fgal.financial_report_value, 0)      AS financial_report_value,
                     COALESCE(fgal.management_report_value, 0)     AS management_report_value,
                     COALESCE(TRIM(UPPER(fgal.expense_code)),' ') AS expense_code,
                     COALESCE(TRIM(UPPER(fgal.department_code)),' ') AS department_code
              FROM {{ source('bronze', 'fact_gits_accounting_ledger_planning_revenue')}} fgal)
SELECT * FROM base
WHERE NOT (
    (financial_report_value = 0
    AND management_report_value= 0)
    OR department_code=' '
    OR department_code='0'
    OR expense_code='0'
    OR expense_code=' '
)
