{{ config(
    materialized='table'
) }}

SELECT
    COALESCE(TRIM(UPPER(dd.department_code)),' ') as department_code,
    deparment_id,
    ob_id,
    unit_id,
    business_unit_1_id,
    COALESCE(TRIM(dd.business_unit_1_name), ' ') AS business_unit_1_name,
    business_unit_2_id,
    COALESCE(TRIM(dd.business_unit_2_name), ' ') AS business_unit_2_name,
    business_unit_3_id,
    COALESCE(TRIM(dd.business_unit_3_name), ' ') AS business_unit_3_name
FROM {{ source('bronze','dim_department')}} dd
