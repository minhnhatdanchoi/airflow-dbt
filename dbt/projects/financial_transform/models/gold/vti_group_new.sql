{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_vti_group_new_value_type ON {{ this }}(value_type);",
        "CREATE INDEX IF NOT EXISTS idx_vti_group_new_expense_type_5_id ON {{ this }}(expense_type_5_id);"
    ]
) }}
with fact_actual as(
  select 
    val.accounting_date, 
    val.financial_report_value, 
    val.management_report_value, 
    val.department_code, 
    val.expense_code 
  from 
    {{ ref('silver_fact_vti_accounting_ledger')}} val
  union all
  select
    va.accounting_date, 
    va.financial_report_value,
    va.management_report_value,
    va.department_code, 
    va.expense_code 
  from 
    {{ ref('silver_fact_vti_adjustment')}} va
),
fact_planning as(
  select 
    val.accounting_date, 
    val.financial_report_value, 
    val.management_report_value, 
    val.department_code, 
    val.expense_code 
  from 
    {{ ref('silver_fact_vti_accounting_ledger_planning')}} val
  union all
  select 
    va.accounting_date, 
    va.financial_report_value,
    va.management_report_value,
    va.department_code, 
    va.expense_code 
  FROM 
    {{ ref('silver_fact_vti_adjustment_planning')}} va
),
fact AS (
  SELECT
    fa.*,
    'actual' source
  FROM
    fact_actual fa
  UNION ALL
  SELECT
    fp.*,
    'planning' source
  FROM
    fact_planning fp
),
dim_expense AS ( 
  SELECT
    det.expense_code,
    det.expense_type_5_id,
    det5.expense_type_name AS expense_type_5_name
  FROM
    {{ ref('silver_dim_expense_type')}} det
  LEFT JOIN {{ ref('silver_dim_expense_type_5')}} det5 ON det.expense_type_5_id = det5.expense_type_id
),
dim_department AS (
  SELECT
    dd.department_code,
    dd.business_unit_1_id,
    dd.business_unit_1_name,
    dd.business_unit_2_id,
    dd.business_unit_2_name,
    dd.business_unit_3_id,
    dd.business_unit_3_name,
    dob.ob_code,
    dob.ob_type_1_code,
    dob.ob_type_2_code,
    dob.ob_type_3_code
  FROM
    {{ ref('silver_dim_department')}} dd
  LEFT JOIN {{ ref('silver_dim_ob')}} dob ON dd.ob_id = dob.ob_id
),
er AS (
  SELECT
    fact.source,
    fact.accounting_date AS date,
    fact.financial_report_value AS value,
    exp.expense_code,
    exp.expense_type_5_id,
    exp.expense_type_5_name,
    CASE
      WHEN exp.expense_type_5_id IN (18, 19) THEN 'r'
      ELSE 'e'
    END AS value_type,
    dpt.department_code,
    dpt.business_unit_1_id,
    dpt.business_unit_1_name,
    dpt.business_unit_2_id,
    dpt.business_unit_2_name,
    dpt.business_unit_3_id,
    dpt.business_unit_3_name,
    dpt.ob_code
  FROM
    fact
  LEFT JOIN dim_expense exp ON LOWER(fact.expense_code) = LOWER(exp.expense_code)
  LEFT JOIN dim_department dpt ON LOWER(fact.department_code) = LOWER(dpt.department_code)
  WHERE dpt.ob_code = 'GROUP'
),
-- Doanh Thu
revenue as(
	SELECT
    er.source,
    er.department_code,
    er.ob_code,
    date_trunc('month', er.date) AS date,
    SUM(er.value) AS value
  FROM
    er
  WHERE
    er.expense_type_5_id = 18
  GROUP BY 1, 2, date_trunc('month', er.date), er.ob_code
),
subtotal_revenue AS (
  SELECT
    rv.*,
    rv2.value previous_value,
    'str' value_type
  FROM
    revenue rv
  LEFT JOIN revenue rv2 on rv.date = (rv2.date + INTERVAL '1 years') and rv.source = rv2.source and rv.department_code = rv2.department_code
),
-- Lợi Nhuận
profit AS(
  SELECT
  	er.source,
  	er.department_code,
  	er.ob_code,
    date_trunc('year', date) AS date,
    SUM(CASE WHEN value_type = 'e' THEN -value ELSE value END) value
  FROM er
  WHERE expense_type_5_id <> 19
  GROUP BY 1, 2, date_trunc('year', date), er.ob_code
),
subtotal_profit AS (
  SELECT
    pr.*,
    pr2.value previous_value,
    'p' value_type
  FROM
    profit pr
  LEFT JOIN profit pr2 on pr.date = (pr2.date + INTERVAL '1 years') and pr.source = pr2.source and pr.department_code = pr2.department_code
),
net_profit as(
  select
    er.source,
    er.department_code,
    er.ob_code,
    date_trunc('year', date) AS date,
    SUM(CASE WHEN value_type = 'e' THEN -value ELSE value END) value
  from
    er
  group by 1, 2, date_trunc('year', date), er.ob_code
),
subtotal_net_profit AS (
  SELECT
    np.*,
    np2.value previous_value,
    'np' value_type
  FROM
    net_profit np
  LEFT JOIN net_profit np2 on np.date = (np2.date + INTERVAL '1 years') and np.source = np2.source and np.department_code = np2.department_code
),
erc as (
-- conso
  select
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
    er.business_unit_3_id,
    CASE WHEN (er.business_unit_2_id != 0 and er.business_unit_2_id != '2') THEN er.business_unit_1_name || '-' || er.business_unit_2_name ELSE er.business_unit_1_name END business_unit_3_name,
    er.ob_code,
    er.source,
    NULL previous_value
  FROM er
  UNION ALL
-- profit
  SELECT
    stp.date,
    stp.value,
    stp.value_type,
    '' expense_code,
    NULL expense_type_5_id,
    '' expense_type_5_name,
    stp.department_code,
    NULL business_unit_1_id, 
    '' business_unit_1_name,
    NULL business_unit_2_id,
    '' business_unit_2_name,
    NULL business_unit_3_id,
    ''business_unit_3_name,
    stp.ob_code,
    stp.source,
    stp.previous_value
  FROM subtotal_profit stp
  UNION ALL
-- net profit
  SELECT
    stnp.date,
    stnp.value,
    stnp.value_type,
    '' expense_code,
    NULL expense_type_5_id,
    '' expense_type_5_name,
    stnp.department_code,
    NULL business_unit_1_id, 
    '' business_unit_1_name,
    NULL business_unit_2_id,
    '' business_unit_2_name,
    NULL business_unit_3_id,
    ''business_unit_3_name,
    stnp.ob_code,
    stnp.source,
    stnp.previous_value
  FROM subtotal_net_profit stnp
  UNION ALL
-- revenue
  SELECT
    str.date,
    str.value,
    str.value_type,
    '' expense_code,
    NULL expense_type_5_id,
    '' expense_type_5_name,
    str.department_code,
    NULL business_unit_1_id, 
    '' business_unit_1_name,
    NULL business_unit_2_id,
    '' business_unit_2_name,
    NULL business_unit_3_id,
    ''business_unit_3_name,
    str.ob_code,
    str.source,
    str.previous_value
  FROM subtotal_revenue str
),
out AS (
	SELECT
	    erc.*,
	    str.value str_value
	FROM
	    erc
	LEFT JOIN subtotal_revenue str on date_trunc('year', erc.date) = str.date and erc.source = str.source and erc.department_code = str.department_code
)
select * from out