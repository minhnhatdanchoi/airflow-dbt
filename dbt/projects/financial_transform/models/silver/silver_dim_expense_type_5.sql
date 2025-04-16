{{ config(
    materialized='table'
) }}

SELECT
    det5.expense_type_id,
    COALESCE(det5.expense_type_name, ' ') as expense_type_name
FROM {{ source('bronze', 'dim_expense_type_5')}} det5
