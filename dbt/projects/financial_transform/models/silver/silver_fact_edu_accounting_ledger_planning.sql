{{ config(
    materialized='table'
) }}
WITH base AS(
SELECT
    feal.accounting_date,
    COALESCE(feal.financial_report_value,0) AS financial_report_value,
    COALESCE(feal.management_report_value,0) AS management_report_value,
    COALESCE(TRIM(UPPER(feal.expense_code)),' ') AS expense_code,
    COALESCE(TRIM(UPPER(feal.department_code)),' ') AS department_code
FROM {{ source('bronze', 'fact_edu_accounting_ledger_planning') }} feal
)
SELECT * FROM base
WHERE NOT (
    (financial_report_value = 0
    AND management_report_value= 0)
    OR department_code=' '
    OR department_code='0'
    OR expense_code=' '
    OR expense_code='0'
    OR expense_code='90C'
)

