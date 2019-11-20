### Monthly training table
bns_migration_eda_step0_sql = """
SELECT
	b.imc_key_no,
	b.mo_yr_key_no,
	b.calendar_year,
	b.imc_months_after_signup - LAG(b.imc_months_after_signup, {monthLag}) OVER (PARTITION BY b.imc_key_no ORDER BY b.mo_yr_key_no) as n_months_included,
	b.imc_months_after_signup,
	MAX(CASE WHEN b.I_IMC_EARN_BNS_FLG = 'Y' THEN b.bns_pct ELSE 0 END) OVER (PARTITION BY b.imc_key_no ORDER BY b.mo_yr_key_no ROWS {monthLag} PRECEDING) as max_bns_{monthPara}
FROM behaviors_combined b
WHERE b.cntry_key_no IN ({cntry_list}) AND b.globl_bus_stat_cd = 'ACTIVE' and b.globl_imc_type_cd = 'I'
"""

bns_migration_eda_step1_sql = """
SELECT
	a.imc_key_no,
	a.mo_yr_key_no,
	a.calendar_year,
	a.imc_months_after_signup,
	IFNULL(a.max_bns_{monthPara},0) as max_bns_{monthPara},
	IFNULL(b.max_bns_{monthPara},0) as future_{monthPara}_max_bns,
	CASE WHEN IFNULL(b.max_bns_{monthPara},0) > IFNULL(a.max_bns_{monthPara},0) THEN 'UP' 
		WHEN IFNULL(b.max_bns_{monthPara},0) = IFNULL(a.max_bns_{monthPara},0) THEN 'SAME'
		ELSE 'DOWN' END as bns_migration_label_{monthPara},
	a.n_months_included,
	b.n_months_included as future_n_months_included

FROM bns_migration_eda_step_0 a
LEFT JOIN bns_migration_eda_step_0 b
ON a.imc_key_no = b.imc_key_no and a.mo_yr_key_no = b.mo_yr_key_no_{monthPara}_pre"""

bns_migration_outcomes_sql = """
SELECT 
	a.imc_key_no,
	a.mo_yr_key_no,
	a.imc_months_after_signup,
	a.max_bns_{monthPara},
	a.future_{monthPara}_max_bns,
	a.bns_migration_label_{monthPara}
FROM bns_migration_eda_step_1 a
where a.n_months_included = {monthLag} and a.future_n_months_included = {monthLag}"""

bns_migration_v1_final_sql = """
SELECT /*+  BROADCASTJOIN(small) */
    b.*,
    a.max_bns_{monthPara},
    a.future_{monthPara}_max_bns,
    a.bns_migration_label_{monthPara}
from abo_dna_full_partial b
    left join  bns_migration_monthly_outcomes_partial a on a.imc_key_no = b.imc_key_no and a.mo_yr_key_no = b.mo_yr_key_no
"""
