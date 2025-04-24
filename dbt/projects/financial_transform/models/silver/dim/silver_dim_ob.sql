{{ config(
    materialized='table'
)}}

SELECT
dob.ob_id,
dob.ob_code,
dob.ob_type_1_code,
dob.ob_type_2_code,
dob.ob_type_3_code FROM {{ source('bronze', 'dim_ob')}} AS dob

