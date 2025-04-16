{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_vkr_value_type ON {{ this }}(value_type);",
        "CREATE INDEX IF NOT EXISTS idx_vkr_expense_type_5_id ON {{ this }}(expense_type_5_id);"
    ]
) }}
WITH fact_actual AS (
  SELECT 
    val.accounting_date, 
    val.financial_report_value, 
    val.management_report_value, 
    val.department_code, 
    val.expense_code 
  FROM 
    {{ ref('silver_fact_vkr_accounting_ledger')}} val
  UNION ALL
  SELECT 
    va.accounting_date, 
    va.financial_report_value, 
    va.management_report_value, 
    va.department_code, 
    va.expense_code 
  FROM 
    {{ ref('silver_fact_vkr_adjustment')}} va
), 
fact_planning AS (
  SELECT 
    vpal.accounting_date, 
    vpal.financial_report_value, 
    vpal.management_report_value, 
    vpal.department_code, 
    vpal.expense_code 
  FROM 
    {{ ref('silver_fact_vkr_planning_accounting_ledger')}} vpal 
  UNION ALL
  SELECT 
    vpa.accounting_date, 
    vpa.financial_report_value, 
    vpa.management_report_value, 
    vpa.department_code, 
    vpa.expense_code 
  FROM 
    {{ ref('silver_fact_vkr_planning_adjustment')}} vpa
  UNION ALL
  SELECT 
    vpr.accounting_date, 
    vpr.financial_report_value, 
    vpr.management_report_value, 
    vpr.department_code, 
    vpr.expense_code 
  FROM 
    {{ ref('silver_fact_vkr_planning_revenue')}} vpr
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
    fact.accounting_date date, 
    CASE 
	    WHEN exp.expense_type_5_id NOT IN (18, 19) AND fact.source = 'planning' THEN -fact.management_report_value 
	    ELSE fact.management_report_value 
	END as value,
    CASE 
	    WHEN exp.expense_type_5_id IN (18, 19) THEN 'r' 
	    ELSE 'e' 
	END value_type,
    exp.expense_code, 
    exp.expense_type_5_id, 
    exp.expense_type_5_name, 
    dpt.department_code, 
    dpt.business_unit_1_id, 
    dpt.business_unit_1_name, 
    dpt.business_unit_2_id, 
    dpt.business_unit_2_name, 
    dpt.ob_code
  FROM 
    fact 
  LEFT JOIN dim_expense exp ON UPPER(fact.expense_code) = UPPER(exp.expense_code) 
  LEFT JOIN dim_department dpt ON UPPER(fact.department_code) = UPPER(dpt.department_code) 
), 
-- Lợi nhuận
profit AS (
  SELECT 
  	er.source,
    date_trunc('month', date) AS date, 
    SUM(value) AS value
  FROM 
    er 
  WHERE 
    expense_type_5_id <> 19 
  GROUP BY 1, 2
), 
subtotal_profit AS (
  SELECT 
    pr.*, 
    pr2.value previous_value,
    'p' value_type
  FROM 
    profit pr 
  LEFT JOIN profit pr2 on pr.date = (pr2.date + INTERVAL '1 months') and pr.source = pr2.source
),
-- Lợi nhuận thuần
net_profit AS (
  SELECT 
  	er.source, 
    date_trunc('month', date) AS date, 
    SUM(value) AS value
  FROM 
    er 
  GROUP BY 1, 2
),
subtotal_net_profit AS (
  SELECT 
    np.*, 
    np2.value previous_value,
    'np' value_type
  FROM 
    net_profit np 
  LEFT JOIN net_profit np2 on np.date = (np2.date + INTERVAL '1 months') and np.source = np2.source
),
-- Tổng doanh thu theo tháng
revenue AS (
  SELECT 
    er.source, 
    date_trunc('month', date) AS date, 
    SUM(value) AS value 
  FROM 
    er 
  WHERE 
    expense_type_5_id = 18 
  GROUP BY 1, 2
),  
-- Tổng doanh thu
subtotal_revenue AS (
  SELECT 
    rv.*, 
    rv2.value previous_value, 
    'str' value_type
  FROM 
    revenue rv 
  LEFT JOIN revenue rv2 on rv.date = (rv2.date + INTERVAL '1 months') and rv.source = rv2.source
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
    stp.source, 
    stp.previous_value
  FROM subtotal_profit stp
  UNION ALL
-- net profit
  SELECT
    stnp.date, 
    stnp.value, 
    stnp.value_type,  
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
    stnp.source, 
    stnp.previous_value
  FROM subtotal_net_profit stnp
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
    str.source, 
    str.previous_value
  FROM subtotal_revenue str
)
SELECT 
    erc.*,
    str.value str_value
FROM 
    erc
LEFT JOIN subtotal_revenue str on date_trunc('month', erc.date) = str.date and erc.source = str.source