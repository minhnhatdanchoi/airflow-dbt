{{ config(
    materialized='table'
) }}
WITH BASE AS (SELECT ftal.accounting_date,
                     COALESCE(ftal.financial_report_value, 0)      AS financial_report_value,
                     COALESCE(ftal.management_report_value, 0)     AS management_report_value,
                     COALESCE(TRIM(UPPER(ftal.expense_code)),' ') AS expense_code,
                     COALESCE(TRIM(UPPER(ftal.department_code)),' ') AS department_code
              FROM {{ source('bronze', 'fact_tvn_accounting_ledger_planning')}} as ftal)
SELECT * FROM BASE
WHERE NOT (
    (financial_report_value = 0
    AND management_report_value= 0)
    OR department_code=' '
    OR department_code='0'
    OR expense_code='0'
    OR expense_code=' '
)
