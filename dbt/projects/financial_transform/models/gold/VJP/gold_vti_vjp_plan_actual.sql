{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_er_vjp_new_value_type ON {{ this }}(value_type);",
        "CREATE INDEX IF NOT EXISTS idx_er_vjp_new_expense_type_5_id ON {{ this }}(expense_type_5_id);"
    ]
) }}
WITH fact_actual AS (
  SELECT 
    al.accounting_date, 
    al.financial_report_value, 
    al.department_code, 
    al.expense_code 
  FROM 
    {{ ref('silver_fact_vjp_accounting_ledger')}} al
  UNION ALL  
  SELECT 
    fval.accounting_date, 
    fval.financial_report_value / COALESCE(rate.rate_value, 1) AS financial_report_value, 
    fval.department_code, 
    fval.expense_code 
  FROM 
    {{ ref('silver_fact_vti_accounting_ledger')}} fval
  LEFT JOIN {{ ref('silver_fact_conso_exchange_rate')}} rate 
    ON DATE_TRUNC('month', fval.accounting_date) = DATE_TRUNC('month', rate.accounting_date)
    AND rate.department_code = 'VTI.JAPAN'
  WHERE 
    fval.department_code = 'VTI.JAPAN'
    AND EXTRACT(YEAR FROM fval.accounting_date) = 2024
  UNION ALL 
  SELECT 
    vla.accounting_date, 
    vla.financial_report_value, 
    vla.department_code, 
    vla.expense_code 
  FROM 
	{{ ref('silver_fact_vjp_ledger_allocation')}} vla 
  WHERE 
	vla.department_code = 'VJP.GS'
  UNION ALL 
  SELECT 
    vaa.accounting_date, 
    vaa.financial_report_value, 
    vaa.department_code, 
    vaa.expense_code 
  FROM 
    {{ ref('silver_fact_vjp_adjustment')}} vaa
),  
fact_planning AS (
  SELECT 
    alp.accounting_date, 
    alp.financial_report_value, 
    alp.department_code, 
    alp.expense_code 
  FROM 
    {{ ref('silver_fact_vjp_accounting_ledger_planning')}} alp
  UNION ALL 
  SELECT 
    ap.accounting_date, 
    ap.financial_report_value, 
    ap.department_code, 
    ap.expense_code 
  FROM 
    {{ ref('silver_fact_vjp_adjustment_planning')}} ap
  UNION ALL 
  SELECT 
    vrp.accounting_date, 
    vrp.financial_report_value, 
    vrp.department_code, 
    vrp.expense_code 
  FROM 
    {{ ref('silver_fact_vjp_revenue_planning')}} vrp
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
    fact.financial_report_value as value,
    CASE WHEN exp.expense_type_5_id IN (18, 19) THEN 'r' ELSE 'e' END value_type, 
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