{{ config(
    materialized='table'
) }}
WITH BASE
    AS (SELECT fva.accounting_date,
               COALESCE(fva.financial_report_value, 0)      AS financial_report_value,
               COALESCE(fva.management_report_value, 0)     AS management_report_value,
               COALESCE(TRIM(UPPER(fva.expense_code)),' ') AS expense_code,
               COALESCE(TRIM(UPPER(fva.department_code)),' ') AS department_code,
               COALESCE(TRIM(fva.general_ledger_code), ' ') AS general_ledger_code
        FROM bronze.fact_vti_adjustment_planning fva)
SELECT * FROM BASE
WHERE NOT (
    (financial_report_value = 0
    AND management_report_value= 0)
    OR department_code=' '
    OR department_code='0'
    OR expense_code='0'
    OR expense_code=' '
)