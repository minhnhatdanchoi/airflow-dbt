{{ config(
    materialized='table'
) }}

SELECT
    COALESCE(det3.expense_type_name, ' ') AS expense_type_name,
    det3.expense_type_id
FROM {{ source('bronze', 'dim_expense_type_3')}} det3
