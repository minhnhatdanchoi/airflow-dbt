{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_vti_group_new2_value_type ON {{ this }}(value_type);",
        "CREATE INDEX IF NOT EXISTS idx_vti_group_new2_expense_type_5_id ON {{ this }}(expense_type_5_id);"
    ]
) }}
WITH fact_vn AS (
  SELECT
    f.date,
    f.value,
    f.department_code,
    f.expense_code,
    f.source
  FROM
    {{ ref('vti_vn_plan_actual_new')}} f
  WHERE f.value_type IN ('e', 'r')
),
fact_vvn AS (
  SELECT
    f.date,
    f.value,
    f.department_code,
    f.expense_code,
    f.source
  FROM
    {{ ref('vti_vvn_new')}} f
  WHERE f.value_type IN ('e', 'r')
),
fact_apac AS (
  SELECT
    f.date,
    f.value,
    f.department_code,
    f.expense_code,
    f.source
  FROM
    {{ ref('vti_apac_new')}} f
  WHERE f.value_type IN ('e', 'r')
),
fact_snp AS (
  SELECT
    f.date,
    f.value,
    f.department_code,
    f.expense_code,
    f.source
  FROM
    {{ ref('vti_snp_plan_actual_new')}} f
  WHERE f.value_type IN ('e', 'r')
),
fact_group AS (
  SELECT
    f.date,
    f.value,
    f.department_code,
    f.expense_code,
    f.source
  FROM
    {{ ref('vti_group_new')}} f
  WHERE f.value_type IN ('e', 'r')
),
fact_gits AS (
  SELECT
    f.date,
    f.value,
    f.department_code,
    f.expense_code,
    f.source
  FROM
    {{ ref('vti_gits_plan_actual_new')}} f
  WHERE f.value_type IN ('e', 'r')
),
fact_edu AS (
  SELECT
    f.date,
    f.value,
    f.department_code,
    f.expense_code,
    f.source
  FROM
    {{ ref('vti_edu_plan_actual_new')}} f
  WHERE f.value_type IN ('e', 'r')
),
fact_tvn AS (
  SELECT
    f.date,
    f.value,
    f.department_code,
    f.expense_code,
    f.source
  FROM
    {{ ref('vti_tvn_plan_actual_new')}} f
  WHERE f.value_type IN ('e', 'r')
),
fact_cloud AS (
  SELECT
    f.accounting_date date,
    f.management_report_value as value,
    f.department_code,
    f.expense_code,
    'actual' source
  FROM
    {{ ref('silver_fact_conso_cloud_ledger')}} f
  UNION ALL
  SELECT
    f.accounting_date date,
    f.management_report_value as value,
    f.department_code,
    f.expense_code,
    'planning' source
  FROM
    {{ ref('silver_fact_conso_planning_cloud_ledger')}} f
),
fact_willer AS (
  SELECT
    f.accounting_date date,
    f.management_report_value as value,
    f.department_code,
    f.expense_code,
    'actual' source
  FROM
    {{ ref('silver_fact_conso_willer_ledger')}} f
  UNION ALL
  SELECT
    f.accounting_date date,
    f.management_report_value as value,
    f.department_code,
    f.expense_code,
    'planning' source
  FROM
    {{ ref('silver_fact_conso_planning_willer_ledger')}} f
),
fact_elimination AS (
  SELECT
    ce.accounting_date date,
    ce.management_report_value as value,
    ce.department_code,
    ce.expense_code,
    ce.type,
    'actual' source
  FROM
    {{ ref('silver_fact_conso_elimination')}} ce
  UNION ALL
  SELECT
    cpe.accounting_date date,
    cpe.management_report_value as value,
    cpe.department_code,
    cpe.expense_code,
    cpe.type,
    'planning' source
  FROM
    {{ ref('silver_fact_conso_planning_elimination')}} cpe
),
fact_exchange_rate AS (
  SELECT
    cer.accounting_date,
    cer.rate_value,
    cer.department_code,
    'actual' source
  FROM
    {{ ref('silver_fact_conso_exchange_rate')}} cer
  UNION ALL
  SELECT
    cper.accounting_date,
    cper.rate_value,
    cper.department_code,
    'planning' source
  FROM
    {{ ref('silver_fact_conso_planning_exchange_rate')}} cper
),
fact_vjp AS (
  SELECT
    f.date,
    CASE
	    WHEN cer.rate_value IS NULL THEN f.value
    	ELSE f.value * cer.rate_value
    END as value,
    f.department_code,
    f.expense_code,
    f.source
  FROM
    {{ ref('vti_vjp_plan_actual_new')}} f
  LEFT JOIN fact_exchange_rate cer ON DATE_TRUNC('month', f.date) = DATE_TRUNC('month', cer.accounting_date)
  	AND f.source = cer.source
  	AND cer.department_code = 'VTI.JAPAN'
  WHERE f.value_type IN ('e', 'r')
),
fact_vkr AS (
  SELECT
    f.date,
    CASE
	    WHEN cer.rate_value IS NULL THEN f.value
    	ELSE f.value * cer.rate_value
    END as value,
    f.department_code,
    f.expense_code,
    f.source
  FROM
    {{ ref('vti_vkr_plan_actual_new')}} f
  LEFT JOIN fact_exchange_rate cer ON DATE_TRUNC('month', f.date) = DATE_TRUNC('month', cer.accounting_date)
  	AND f.source = cer.source
  	AND cer.department_code = 'VTI.VKR'
  WHERE f.value_type IN ('e', 'r')
),
fact_vti AS (
--1 Mang IT
--1.1 Outsourcing
  SELECT
    f.date,
    f.value / 10^6 as value,
    f.expense_code,
    f.department_code,
    f.source
  FROM fact_vn f
  UNION ALL
  SELECT
    f.date,
    f.value / 10^6 as value,
    f.expense_code,
    f.department_code,
    f.source
  FROM fact_vvn f
  UNION ALL
  SELECT
    f.date,
    f.value / 10^6 as value,
    f.expense_code,
    f.department_code,
    f.source
  FROM fact_vjp f
  UNION ALL
  SELECT
    f.date,
    f.value / 10^6 as value,
    f.expense_code,
    f.department_code,
    f.source
  FROM fact_vkr f
  UNION ALL
  SELECT
    f.date,
    f.value / 10^6 as value,
    f.expense_code,
    f.department_code,
    f.source
  FROM fact_apac f
  UNION ALL
  SELECT
    f.date,
    f.value / 10^6 as value,
    f.expense_code,
    f.department_code,
    f.source
  FROM fact_gits f
  UNION ALL
  SELECT
    f.date,
    f.value,
    f.expense_code,
    f.department_code,
    f.source
  FROM fact_cloud f
  UNION ALL
  SELECT
    f.date,
    f.value,
    f.expense_code,
    f.department_code,
    f.source
  FROM fact_willer f
  UNION ALL
  SELECT
    f.date,
    f.value,
    f.expense_code,
    f.department_code,
    f.source
  FROM fact_elimination f
  WHERE f.type = 'BT loại trừ outsourcing'
--1.2  IT
  UNION ALL
  SELECT
    f.date,
    f.value / 10^6 as value,
    f.expense_code,
    f.department_code,
    f.source
  FROM fact_snp f
  UNION ALL
  SELECT
    f.date,
    f.value,
    f.expense_code ,
    f.department_code,
    f.source
  FROM fact_elimination f
  WHERE f.type = 'BT loại trừ IT'
--2  Edu Group
  UNION ALL
  SELECT
    f.date,
    f.value / 10^6 as value,
    f.expense_code,
    f.department_code,
    f.source
  FROM fact_edu f
  UNION ALL
  SELECT
    f.date,
    f.value / 10^6 as value,
    f.expense_code,
    f.department_code,
    f.source
  FROM fact_tvn f
--3  VTI.Group
  UNION ALL
  SELECT
    f.date,
    f.value / 10^6 as value,
    f.expense_code,
    f.department_code,
    f.source
  FROM fact_group f
  UNION ALL
  SELECT
    f.date,
    f.value,
    f.expense_code,
    f.department_code,
    f.source
  FROM fact_elimination f
  WHERE f.type = 'BT loại trừ VTI.Group'
),
fact AS(
  SELECT
    f.date,
    f.value,
    f.expense_code,
    f.department_code,
    f.source,
    'VTI' company
  FROM
    fact_vti f
  UNION ALL
  SELECT
    f.date,
    f.value,
    f.expense_code,
    f.department_code,
    'actual' source,
    'FPT' company
  FROM {{ ref('silver_fact_fpt_conso_new')}} f
),
--dim expense
dim_expense AS (
  SELECT
    det.expense_code,
    det.expense_type_5_id,
    det5.expense_type_name AS expense_type_5_name
  FROM
    {{ ref('silver_dim_expense_type')}} det
  LEFT JOIN {{ ref('silver_dim_expense_type_5')}} det5 ON det.expense_type_5_id = det5.expense_type_id
),
--dim department
dim_department AS (
  SELECT
    dd.department_code,
    dd.business_unit_1_id,
    dd.business_unit_1_name,
    dd.business_unit_2_id,
    dd.business_unit_2_name,
    dob.ob_code,
    dob.ob_type_1_code,
    dob.ob_type_2_code,
    dob.ob_type_3_code
  FROM
    {{ ref('silver_dim_department')}} dd
  LEFT JOIN {{ ref('silver_dim_ob')}} dob ON dd.ob_id = dob.ob_id
),
--er
er AS (
  SELECT
    fact.source,
    fact.date,
    fact.value,
    CASE WHEN exp.expense_type_5_id IN (18, 19) THEN 'r' ELSE 'e' END value_type,
    exp.expense_code,
    exp.expense_type_5_id,
    exp.expense_type_5_name,
    dpt.department_code,
    dpt.business_unit_1_id,
    dpt.business_unit_1_name,
    dpt.business_unit_2_id,
    dpt.business_unit_2_name,
    dpt.ob_code,
    fact.company
  FROM
    fact
  LEFT JOIN dim_expense exp ON UPPER(fact.expense_code) = UPPER(exp.expense_code)
  LEFT JOIN dim_department dpt ON UPPER(fact.department_code) = UPPER(dpt.department_code)
),
-- Lợi nhuận ròng
profit AS (
  SELECT
  	er.source,
  	er.company,
    date_trunc('month', date) AS date,
    SUM(value) as value
  FROM
    er
  GROUP BY 1, 2, 3
),
subtotal_profit AS (
  SELECT
    p.*,
    p2.value previous_value,
    'p' value_type
  FROM
    profit p
  LEFT JOIN profit p2 on p.date = (p2.date + INTERVAL '1 month') and p.source = p2.source and p.company = p2.company
),
-- Tổng doanh thu theo tháng
revenue AS (
  SELECT
    er.source,
    er.company,
    date_trunc('month', date) AS date,
    SUM(value) AS value
  FROM
    er
  WHERE
    expense_type_5_id = 18
  GROUP BY 1, 2, 3
),
yearly_revenue AS (
  SELECT
    er.source,
    er.company,
    date_trunc('year', date) AS date,
    SUM(value) AS value
  FROM
    revenue er
  GROUP BY 1, 2, 3
),
-- Tổng doanh thu
subtotal_revenue AS (
  SELECT
    rv.*,
    rv2.value previous_value,
    'str' value_type
  FROM
    revenue rv
  LEFT JOIN revenue rv2 on rv.date = (rv2.date + INTERVAL '1 month') and rv.source = rv2.source and rv.company = rv2.company
),
erc AS (
-- er
  SELECT
    er.date,
    er.value,
    er.value_type,
    er.expense_code,
    er.expense_type_5_id,
    er.expense_type_5_name,
    er.department_code,
    er.business_unit_1_id,
    er.business_unit_1_name,
    er.business_unit_2_id,
    er.business_unit_2_name,
    CASE WHEN er.business_unit_2_id != 0 THEN er.business_unit_1_name || '-' || er.business_unit_2_name ELSE er.business_unit_1_name END business_unit_3_name,
    er.ob_code,
    er.company,
    er.source,
    NULL previous_value
  FROM er
  UNION ALL
-- profit
  SELECT
    stp.date,
    stp.value,
    stp.value_type,
    NULL expense_code,
    NULL expense_type_5_id,
    '' expense_type_5_name,
    NULL department_code,
    NULL business_unit_1_id,
    '' business_unit_1_name,
    NULL business_unit_2_id,
    '' business_unit_2_name,
    '' business_unit_3_name,
    '' ob_code,
    stp.company,
    stp.source,
    stp.previous_value
  FROM subtotal_profit stp
  UNION ALL
-- revenue
  SELECT
    str.date,
    str.value,
    str.value_type,
    NULL expense_code,
    NULL expense_type_5_id,
    '' expense_type_5_name,
    NULL department_code,
    NULL business_unit_1_id,
    '' business_unit_1_name,
    NULL business_unit_2_id,
    '' business_unit_2_name,
    '' business_unit_3_name,
    '' ob_code,
    str.company,
    str.source,
    str.previous_value
  FROM subtotal_revenue str
)
SELECT
    erc.*,
    str.value str_value
FROM
    erc
LEFT JOIN yearly_revenue str on date_trunc('year', erc.date) = str.date and erc.source = str.source and erc.company = str.company
