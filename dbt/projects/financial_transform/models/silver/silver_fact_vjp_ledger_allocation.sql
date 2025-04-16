{{ config(
    materialized='table'
) }}
WITH BASE AS (SELECT fvl.accounting_date,
                     COALESCE(fvl.financial_report_value, 0)  AS financial_report_value,
                     COALESCE(TRIM(UPPER(fvl.expense_code)),' ') AS expense_code,
                     COALESCE(TRIM(UPPER(fvl.department_code)),' ') AS department_code
              FROM {{ source('bronze', 'fact_vjp_ledger_allocation')}} fvl)
SELECT * FROM BASE
WHERE NOT (
    financial_report_value = 0
    OR department_code=' '
    OR department_code='0'
    OR expense_code='0'
    OR expense_code=' '
)
