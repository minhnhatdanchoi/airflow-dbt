{{ config(
    materialized='table'
)}}

SELECT
    COALESCE(TRIM(UPPER(det.expense_code)),' ') as expense_code,
    expense_type_1_id,
    expense_type_5_id FROM {{ source('bronze', 'dim_expense_type')}} det




