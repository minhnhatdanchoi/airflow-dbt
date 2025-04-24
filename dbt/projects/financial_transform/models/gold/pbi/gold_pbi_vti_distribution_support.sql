with dim_expense as (
  select
    det.code expense_code,
    det.fee_type_1,
    det.fee_type_2,
    det.fee_type_3,
    det.fee_type_4,
    det.type_id
  from
    bronze.dim_feecode det
),
dim_department as (
  select
    dd.department_code,
    dd.business_unit_1_id,
    dd.business_unit_1_name,
    dd.business_unit_2_id,
    dd.business_unit_2_name,
    dd.unit_id,
    dd.ob_id,
    dd.deparment_id d_id,
    dob.ob_code,
    dob.ob_type_1_code,
    dob.ob_type_2_code,
    dob.ob_type_3_code
  from
    silver.silver_dim_department dd
    left join silver.silver_dim_ob dob on dd.ob_id = dob.ob_id
),
fact_ledger as (
  select
    f.accounting_date date,
    f.bctc / 10 ^ 6 financial_report_value,
    f.bcqt / 10 ^ 6 management_report_value,
    f.fee_code expense_code,
    f.department_id department_code,
    f.gl3_code
  from
    bronze.fact_vti_accounting_ledger_pbi f
  union all
  select
    f.accounting_date date,
    f.bctc / 10 ^ 6 financial_report_value,
    f.bcqt / 10 ^ 6 management_report_value,
    f.fee_code expense_code,
    f.department_id department_code,
    f.gl3_code
  from
    bronze.fact_vti_adjustment_pbi f
),
temp_fact_ledger as (
  select
    f.date,
    f.financial_report_value,
    f.management_report_value,
    f.expense_code,
    de.fee_type_1,
    de.fee_type_2,
    de.fee_type_3,
    de.fee_type_4,
    de.type_id,
    f.department_code,
    dd.business_unit_1_id,
    dd.business_unit_1_name,
    dd.business_unit_2_id,
    dd.business_unit_2_name,
    dd.unit_id,
    dd.ob_id,
    dd.ob_code,
    dd.d_id,
    f.gl3_code
  from
    fact_ledger f
    left join dim_expense de on UPPER(de.expense_code) = UPPER(f.expense_code)
    left join dim_department dd on UPPER(dd.department_code) = UPPER(f.department_code)
),
temp_fact_man_month as (
  select
    f.accounting_date date,
    f.value,
    f.type_id,
    f.department_id department_code,
    dd.business_unit_1_id,
    dd.business_unit_1_name,
    dd.business_unit_2_id,
    dd.business_unit_2_name,
    dd.unit_id,
    dd.ob_id,
    dd.ob_code,
    dd.d_id
  from
    bronze.fact_pbi_vti_man_month f
    left join dim_department dd on UPPER(dd.department_code) = UPPER(f.department_id)
),
temp_expense as (
  select
    t.date,
    t.ob_code,
    t.unit_id,
    t.department_code,
    sum(t.management_report_value) value
  from
    temp_fact_ledger t
  where
    t.fee_type_3 = 'Chi phí'
  group by
    1,
    2,
    3,
    4
),
temp_man_month as (
  select
    t.date,
    t.type_id,
    t.ob_code,
    t.unit_id,
    t.department_code,
    sum(t.value) value
  from
    temp_fact_man_month t
  group by
    1,
    2,
    3,
    4,
    5
),
temp_expense_support as (
  select
    *
  from
    temp_expense
  where
    unit_id = 5
),
temp_man_month_support as (
  select
    *,
    SUM(value) over (partition by t.date) calendar_mm_total
  from
    temp_man_month t
  where
    type_id = 'MM001'
    and ob_code in ('VN')
    and unit_id in (1, 13, 14)
),
temp_distribution_support as (
  select
    tmm.date,
    tmm.unit_id,
    te.department_code department_distribute_code,
    tmm.department_code,
    te.value expense,
    tmm.value calendar_mm,
    tmm.calendar_mm_total,
    tmm.value * te.value / tmm.calendar_mm_total distribution
  from
    temp_man_month_support tmm
    full join temp_expense_support te on tmm.date = te.date
)
select
  *
from
  temp_distribution_support
order by
  date asc,
  unit_id asc,
  department_code asc
