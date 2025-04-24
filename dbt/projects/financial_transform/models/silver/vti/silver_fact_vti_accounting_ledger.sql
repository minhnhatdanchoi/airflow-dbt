{{ config(
    materialized='table'
) }}
WITH BASE
    AS (SELECT fal.accounting_date,
               COALESCE(fal.financial_report_value, 0)     AS financial_report_value,
               COALESCE(fal.management_report_value, 0)    AS management_report_value,
               COALESCE(TRIM(UPPER(fal.expense_code)),' ') AS expense_code,
               COALESCE(TRIM(UPPER(fal.department_code)),' ') AS department_code,
               COALESCE(TRIM(fal.general_ledger_code), '') as general_ledger_code
        FROM {{ source('bronze', 'fact_vti_accounting_ledger')}} fal)
SELECT * FROM BASE
WHERE NOT (
    (financial_report_value = 0
    AND management_report_value= 0)
    OR department_code='0'
    OR department_code=' '
    OR expense_code='0'
    OR expense_code=' '
)