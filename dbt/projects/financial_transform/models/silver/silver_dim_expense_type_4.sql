{{ config(
    materialized='table'
) }}

SELECT
    det4.expense_type_id,
    COALESCE(det4.expense_type_name, ' ') AS expense_type_name
FROM {{ source('bronze', 'dim_expense_type_4')}} det4
