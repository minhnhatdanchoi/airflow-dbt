{{ config(
    materialized='table'
) }}

SELECT
    det2.expense_type_id,
    COALESCE(det2.expense_type_name, ' ') AS expense_type_name,
    COALESCE(det2.expense_type_name_2, ' ') AS expense_type_name_2
FROM {{ source('bronze', 'dim_expense_type_2')}} det2
