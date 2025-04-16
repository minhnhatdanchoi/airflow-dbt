{{ config(
    materialized='table'
) }}
WITH BASE
    AS (SELECT fkr.accounting_date,
               COALESCE(fkr.financial_report_value, 0)  AS financial_report_value,
               COALESCE(fkr.management_report_value, 0) AS management_report_value,
               COALESCE(TRIM(UPPER(fkr.expense_code)),' ') AS expense_code,
               COALESCE(TRIM(UPPER(fkr.department_code)),' ') AS department_code
        FROM {{ source('bronze', 'fact_vti_snp_planning_detail')}} fkr)
SELECT * FROM BASE
WHERE NOT (
    (financial_report_value = 0
    AND management_report_value= 0)
    OR department_code=' '
    OR department_code='0'
    OR expense_code='0'
    OR expense_code=' '
)
