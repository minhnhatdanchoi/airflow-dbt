{{ config(
    materialized='table'
) }}

SELECT
    dr.revenue_id,
    COALESCE(dr.revenue_name, ' ') as revenue_name
FROM {{ source('bronze', 'dim_revenue')}} dr
