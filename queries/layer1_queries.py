cntry_promo_imc_order_sql = """
select ord_imc_key_no,
concat(left(ord_mo_yr_key_no,4) ,'-' , right(ord_mo_yr_key_no,2) ,'-','01') as mo_yr_key_no,
count(promo_sku) as n_promo_sku_purchased
from cntry_promo_imc_order_data 
group by 1,2
limit 10 
"""


performance_year_query = """
SELECT 
DISTINCT  
b.mo_yr_key_no,  
CASE    WHEN b.mo_yr_key_no >= '2012-09-01' and b.mo_yr_key_no <= '2013-08-01' THEN 2013    
    WHEN b.mo_yr_key_no >= '2013-09-01' and b.mo_yr_key_no <= '2014-08-01' THEN 2014    
    WHEN b.mo_yr_key_no >= '2014-09-01' and b.mo_yr_key_no <= '2015-08-01' THEN 2015    
    WHEN b.mo_yr_key_no >= '2015-09-01' and b.mo_yr_key_no <= '2016-08-01' THEN 2016   
    WHEN b.mo_yr_key_no >= '2016-09-01' and b.mo_yr_key_no <= '2017-08-01' THEN 2017   
    WHEN b.mo_yr_key_no >= '2017-09-01' and b.mo_yr_key_no <= '2018-08-01' THEN 2018   
    WHEN b.mo_yr_key_no >= '2018-09-01' and b.mo_yr_key_no <= '2019-08-01' THEN 2019   
    WHEN b.mo_yr_key_no >= '2019-09-01' and b.mo_yr_key_no <= '2020-08-01' THEN 2020   
    WHEN b.mo_yr_key_no >= '2020-09-01' and b.mo_yr_key_no <= '2021-08-01' THEN 2021   
    WHEN b.mo_yr_key_no >= '2021-09-01' and b.mo_yr_key_no <= '2022-08-01' THEN 2022   
    WHEN b.mo_yr_key_no >= '2022-09-01' and b.mo_yr_key_no <= '2023-08-01' THEN 2023   
    WHEN b.mo_yr_key_no >= '2023-09-01' and b.mo_yr_key_no <= '2024-08-01' THEN 2024   
    WHEN b.mo_yr_key_no >= '2024-09-01' and b.mo_yr_key_no <= '2025-08-01' THEN 2025   
    WHEN b.mo_yr_key_no >= '2025-09-01' and b.mo_yr_key_no <= '2026-08-01' THEN 2026   
    WHEN b.mo_yr_key_no >= '2026-09-01' and b.mo_yr_key_no <= '2027-08-01' THEN 2027   
    WHEN b.mo_yr_key_no >= '2027-09-01' and b.mo_yr_key_no <= '2028-08-01' THEN 2028   
    WHEN b.mo_yr_key_no >= '2028-09-01' and b.mo_yr_key_no <= '2029-08-01' THEN 2029   
    WHEN b.mo_yr_key_no >= '2029-09-01' and b.mo_yr_key_no <= '2030-08-01' THEN 2030   
    ELSE 2012  END as perf_yr from behaviors_combined b
"""


promo_vw1_query = """
SELECT   
b.imc_key_no,  
b.mo_yr_key_no,  
b.REV_LC_AMT,  
b.bl_beauty_rev_lc_amt,  
b.bl_durables_rev_lc_amt,  
b.bl_homecare_rev_lc_amt,  
b.bl_nutrition_rev_lc_amt,  
b.bl_other_rev_lc_amt,  
b.bl_perscare_rev_lc_amt,  
p.unit_price,  
p.l1 from  behaviors_combined b 
left join support_promotion_campaigns p 
on b.mo_yr_key_no = p.promo_date
"""


support_promotions_query = """
SELECT  
v.imc_key_no,  
v.mo_yr_key_no,  
SUM(case when v.l1 = 'BEAUTY' and v.bl_beauty_rev_lc_amt >= v.unit_price THEN 1 ELSE 0 END) as n_beauty_promo,  
AVG(case when v.l1 = 'BEAUTY' and v.bl_beauty_rev_lc_amt >= v.unit_price THEN v.bl_beauty_rev_lc_amt / v.unit_price ELSE 0 END) as beauty_multiple,    
SUM(case when v.l1 = 'DURABLES' and v.bl_durables_rev_lc_amt >= v.unit_price THEN 1 ELSE 0 END) as n_durables_promo,  
AVG(case when v.l1 = 'DURABLES' and v.bl_durables_rev_lc_amt >= v.unit_price THEN v.bl_durables_rev_lc_amt / v.unit_price ELSE 0 END) as durables_multiple,    
SUM(case when v.l1 = 'HOMECARE' and v.bl_durables_rev_lc_amt >= v.unit_price THEN 1 ELSE 0 END) as n_homecare_promo,  
AVG(case when v.l1 = 'HOMECARE' and v.bl_durables_rev_lc_amt >= v.unit_price THEN v.bl_durables_rev_lc_amt / v.unit_price ELSE 0 END) as homecare_multiple,    
SUM(case when v.l1 = 'PERSCARE' and v.bl_durables_rev_lc_amt >= v.unit_price THEN 1 ELSE 0 END) as n_perscare_promo,  
AVG(case when v.l1 = 'PERSCARE' and v.bl_durables_rev_lc_amt >= v.unit_price THEN v.bl_durables_rev_lc_amt / v.unit_price ELSE 0 END) as perscare_multiple,    
SUM(case when v.l1 = 'NUTRITION' and v.bl_nutrition_rev_lc_amt >= v.unit_price THEN 1 ELSE 0 END) as n_nutrition_promo,  
AVG(case when v.l1 = 'NUTRITION' and v.bl_nutrition_rev_lc_amt >= v.unit_price THEN v.bl_nutrition_rev_lc_amt / v.unit_price ELSE 0 END) as nutrition_multiple,    
SUM(case when v.l1 = 'OTHERS' and v.bl_durables_rev_lc_amt >= v.unit_price THEN 1 ELSE 0 END) as n_others_promo,  
AVG(case when v.l1 = 'OTHERS' and v.bl_durables_rev_lc_amt >= v.unit_price THEN v.bl_durables_rev_lc_amt / v.unit_price ELSE 0 END) as others_multiple,    
SUM(case when v.l1 = 'ALL' and v.REV_LC_AMT >= v.unit_price THEN 1 ELSE 0 END) as n_all_promo,  AVG(case when v.l1 = 'ALL' and v.rev_lc_amt >= v.unit_price THEN v.rev_lc_amt / v.unit_price ELSE 0 END) as all_multiple   
from  promo_vw1 v 
group by v.imc_key_no, v.mo_yr_key_no
"""


first_indicators_query = """
SELECT 
COALESCE(ord.imc_key_no, sil.imc_key_no, gld.imc_key_no, dd.imc_key_no, p3.imc_key_no, p6.imc_key_no, p9.imc_key_no, p12.imc_key_no, p15.imc_key_no, p18.imc_key_no, p21.imc_key_no, sr.imc_key_no, aa.imc_key_no) as imc_key_no, 
COALESCE(ord.mo_yr_key_no, sil.mo_yr_key_no, gld.mo_yr_key_no, dd.mo_yr_key_no,  p3.mo_yr_key_no, p6.mo_yr_key_no, p9.mo_yr_key_no, p12.mo_yr_key_no, p15.mo_yr_key_no, p18.mo_yr_key_no, p21.mo_yr_key_no, sr.mo_yr_key_no, aa.mo_yr_key_no) as mo_yr_key_no, 
nvl(ord.i_first_order, 0) as n_first_order, 
nvl(sil.i_first_silver, 0) as n_first_silver, 
nvl(gld.i_first_gold, 0) as n_first_gold, 
nvl(dd.i_first_dd, 0) as n_first_dd, 
nvl(p3.i_first_3pctbns, 0) as n_first_3pctbns, 
nvl(p6.i_first_6pctbns, 0) as n_first_6pctbns, 
nvl(p9.i_first_9pctbns, 0) as n_first_9pctbns, 
nvl(p12.i_first_12pctbns, 0) as n_first_12pctbns, 
nvl(p15.i_first_15pctbns, 0) as n_first_15pctbns, 
nvl(p18.i_first_18pctbns, 0) as n_first_18pctbns, 
nvl(p21.i_first_21pctbns, 0) as n_first_21pctbns, 
nvl(sr.i_first_SR, 0) as n_first_SR, 
nvl(aa.i_first_AA, 0) as n_first_AA 
from  (select 
    b.imc_key_no, 
    min(b.mo_yr_key_no) as mo_yr_key_no, 
    1 as i_first_order 
    from behaviors_combined b 
    where b.dmd_ord_cnt > 0 group by b.imc_key_no) ord 
full join (select 
    b.imc_key_no, 
    min(b.mo_yr_key_no) as mo_yr_key_no, 
    1 as i_first_silver from behaviors_combined b 
    where b.cur_awd_awd_rnk_no = '310' 
    group by b.imc_key_no) sil 
on sil.imc_key_no = ord.imc_key_no and sil.mo_yr_key_no = ord.mo_yr_key_no 
full join (select 
    b.imc_key_no, 
    min(b.mo_yr_key_no) as mo_yr_key_no, 
    1 as i_first_gold 
    from behaviors_combined b 
    where b.cur_awd_awd_rnk_no = '320' 
    group by b.imc_key_no) gld 
on gld.imc_key_no = ord.imc_key_no and gld.mo_yr_key_no = ord.mo_yr_key_no 
full join (select 
    b.imc_key_no, 
    min(b.mo_yr_key_no) as mo_yr_key_no, 
    1 as i_first_dd 
    from behaviors_combined b 
    where b.cur_awd_awd_rnk_no = '330' 
    group by b.imc_key_no) dd 
on dd.imc_key_no = ord.imc_key_no and dd.mo_yr_key_no = ord.mo_yr_key_no 
full join (select 
    b.imc_key_no, 
    min(b.mo_yr_key_no) as mo_yr_key_no, 
    1 as i_first_3pctbns 
    from behaviors_combined b 
    where b.imc_group_pv >= 100 and b.imc_group_pv <= 299 
    group by b.imc_key_no) p3 
on p3.imc_key_no = ord.imc_key_no and p3.mo_yr_key_no = ord.mo_yr_key_no 
full join (select 
    b.imc_key_no, 
    min(b.mo_yr_key_no) as mo_yr_key_no, 
    1 as i_first_6pctbns 
    from behaviors_combined b 
    where b.imc_group_pv >= 300 and b.imc_group_pv <= 599 
    group by b.imc_key_no) p6 
on p6.imc_key_no = ord.imc_key_no and p6.mo_yr_key_no = ord.mo_yr_key_no 
full join (select 
    b.imc_key_no, 
    min(b.mo_yr_key_no) as mo_yr_key_no, 
    1 as i_first_9pctbns 
    from behaviors_combined b 
    where b.imc_group_pv >= 600 and b.imc_group_pv <= 999 
    group by b.imc_key_no) p9 
on p9.imc_key_no = ord.imc_key_no and p9.mo_yr_key_no = ord.mo_yr_key_no 
full join (select 
    b.imc_key_no, 
    min(b.mo_yr_key_no) as mo_yr_key_no, 
    1 as i_first_12pctbns 
    from behaviors_combined b 
    where b.imc_group_pv >= 1000 and b.imc_group_pv <= 1499 
    group by b.imc_key_no) p12 
on p12.imc_key_no = ord.imc_key_no and p12.mo_yr_key_no = ord.mo_yr_key_no 
full join (select 
    b.imc_key_no, 
    min(b.mo_yr_key_no) as mo_yr_key_no, 
    1 as i_first_15pctbns 
    from behaviors_combined b 
    where b.imc_group_pv >= 1500 and b.imc_group_pv <= 2499 
    group by b.imc_key_no) p15 
on p15.imc_key_no = ord.imc_key_no and p15.mo_yr_key_no = ord.mo_yr_key_no 
full join (select 
    b.imc_key_no, 
    min(b.mo_yr_key_no) as mo_yr_key_no, 
    1 as i_first_18pctbns 
    from behaviors_combined b 
    where b.imc_group_pv >= 2500 and b.imc_group_pv <= 3999 
    group by b.imc_key_no) p18 
on p18.imc_key_no = ord.imc_key_no and p18.mo_yr_key_no = ord.mo_yr_key_no 
full join (select 
    b.imc_key_no, 
    min(b.mo_yr_key_no) as mo_yr_key_no, 
    1 as i_first_21pctbns 
    from behaviors_combined b 
    where b.imc_group_pv >= 4000 and b.imc_group_pv <= 5999 
    group by b.imc_key_no) p21 
on p21.imc_key_no = ord.imc_key_no and p21.mo_yr_key_no = ord.mo_yr_key_no 
full join (select 
    b.imc_key_no, min(b.mo_yr_key_no) as mo_yr_key_no, 
    1 as i_first_23pctbns from behaviors_combined b 
    where b.imc_group_pv >= 6000 and b.imc_group_pv <= 7499 
    group by b.imc_key_no) p23 
on p23.imc_key_no = ord.imc_key_no and p23.mo_yr_key_no = ord.mo_yr_key_no 
full join (select 
    b.imc_key_no, 
    min(b.mo_yr_key_no) as mo_yr_key_no, 
    1 as i_first_SR 
    from behaviors_combined b 
    where b.local_imc_type_desc = 'Sales Representative(SR)' 
    group by b.imc_key_no) sr 
on sr.imc_key_no = ord.imc_key_no and sr.mo_yr_key_no = ord.mo_yr_key_no 
full join (select 
    b.imc_key_no, 
    min(b.mo_yr_key_no) as mo_yr_key_no, 
    1 as i_first_AA from behaviors_combined b 
    where b.local_imc_type_desc = 'Special AA(SAA)' or b.local_imc_type_desc = 'Services Sites(SS)' or b.local_imc_type_desc = 'Probationary AA(PAA)' 
    group by b.imc_key_no) aa 
on aa.imc_key_no = ord.imc_key_no and aa.mo_yr_key_no = ord.mo_yr_key_no
"""


repeat_customers_view_query = """
SELECT 
inner_q.*, 
(CASE WHEN past3_count >= 2 THEN 1 ELSE 0 END) as i_repeat_customer 
from (  SELECT   
    b.imc_key_no,  
    b.mo_yr_key_no,  
    SUM(CASE WHEN b.dmd_ord_cnt > 0 and b.globl_bus_stat_cd = 'ACTIVE' THEN 1 ELSE 0 END) OVER (PARTITION BY imc_key_no ORDER BY mo_yr_key_no ROWS 2 PRECEDING) as past3_count 
    FROM behaviors_combined b WHERE b.globl_imc_type_cd = 'C') as inner_q
"""


los_plat_view_query = """
SELECT  
b.mo_yr_key_no, 
b.cur_plat_imc_no, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' THEN 1 ELSE 0 END) as n_LOS_plat_ABOs, 
SUM(CASE WHEN b.local_imc_type_desc = 'Special AA(SAA)' THEN 1 WHEN b.local_imc_type_desc = 'Services Sites(SS)' THEN 1 WHEN b.local_imc_type_desc = 'Probationary AA(PAA)' THEN 1 ELSE 0 END) as n_LOS_plat_ABOs_AA, 
SUM(CASE WHEN b.local_imc_type_desc = 'Sales Representative(SR)' THEN 1 ELSE 0 END) as n_LOS_plat_ABOs_SR, 
SUM(CASE WHEN b.engage_flg = 'Y' THEN 1 ELSE 0 END) as n_LOS_plat_ABOs_engaged, 
SUM(CASE WHEN b.globl_imc_type_cd = 'C' THEN 1 ELSE 0 END) as n_LOS_plat_customers, 
SUM(s.i_repeat_customer) as n_LOS_plat_customers_repeat, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_plat_q_mth, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_QV_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_plat_qv_mth, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q1_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_plat_q1_mth, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q2_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_plat_q2_mth, 
SUM(CASE WHEN b.frontln_distb_cnt = 0 THEN 1 ELSE 0 END) as n_LOS_plat_leaves, 
nvl(SUM(b.imc_prsnl_pv) / COUNT(b.globl_imc_type_cd) ,0) as n_LOS_plat_pv_per_member, 
SUM(CASE WHEN b.spon_imc_key_no = c.spon_imc_key_no and b.globl_imc_type_cd = 'I' THEN 1 ELSE 0 END) as n_LOS_plat_head_frontln_ABOs, 
SUM(CASE WHEN b.spon_imc_key_no = c.spon_imc_key_no and b.globl_imc_type_cd = 'C' THEN 1 ELSE 0 END) as n_LOS_plat_head_frontln_customers, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_plat_head_frontln_q_mth, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_QV_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_plat_head_frontln_qv_mth, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q1_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_plat_head_frontln_q1_mth, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q2_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_plat_head_frontln_q2_mth 
FROM behaviors_combined b 
LEFT JOIN support_repeat_customers_vw s 
ON s.imc_key_no = b.imc_key_no and s.mo_yr_key_no = b.mo_yr_key_no 
LEFT JOIN (select 
    distinct 
    imc_key_no as spon_imc_key_no, 
    imc_no as spon_imc_no 
    from behaviors_combined 
    where imc_no = cur_plat_imc_no and cntry_key_no IN ({cntry_list})) c 
ON c.spon_imc_no = b.cur_plat_imc_no 
WHERE b.mo_yr_key_no >= '2013-01-01' and b.globl_bus_stat_cd = 'ACTIVE' and b.cntry_key_no IN ({cntry_list}) 
GROUP BY b.cur_plat_imc_no, b.mo_yr_key_no
"""


los_plat_table_query = """
SELECT 
a.mo_yr_key_no, 
a.cur_plat_imc_no, 
CASE WHEN a.cur_plat_imc_no = '-1' THEN 0 WHEN a.cur_plat_imc_no = '1' THEN 0 ELSE 1 END as n_LOS_plat_head_exists, 
a.n_LOS_plat_ABOs, 
a.n_LOS_plat_ABOs_AA, 
a.n_LOS_plat_ABOs_SR, 
a.n_LOS_plat_ABOs_engaged, 
a.n_LOS_plat_customers, 
AVG(a.n_LOS_plat_customers) OVER (PARTITION BY cur_plat_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_plat_customers_avg_3m, 
AVG(a.n_LOS_plat_customers) OVER (PARTITION BY cur_plat_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_plat_customers_avg_6m, 
AVG(a.n_LOS_plat_customers) OVER (PARTITION BY cur_plat_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_plat_customers_avg_9m, 
AVG(a.n_LOS_plat_customers) OVER (PARTITION BY cur_plat_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_plat_customers_avg_12m, 
a.n_LOS_plat_customers_repeat, 
AVG(a.n_LOS_plat_customers_repeat) OVER (PARTITION BY cur_plat_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_plat_customers_repeat_avg_3m, 
AVG(a.n_LOS_plat_customers_repeat) OVER (PARTITION BY cur_plat_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_plat_customers_repeat_avg_6m, 
AVG(a.n_LOS_plat_customers_repeat) OVER (PARTITION BY cur_plat_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_plat_customers_repeat_avg_9m, 
AVG(a.n_LOS_plat_customers_repeat) OVER (PARTITION BY cur_plat_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_plat_customers_repeat_avg_12m, 
a.n_LOS_plat_q_mth, 
AVG(a.n_LOS_plat_q_mth) OVER (PARTITION BY cur_plat_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_plat_q_mth_avg_3m, 
AVG(a.n_LOS_plat_q_mth) OVER (PARTITION BY cur_plat_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_plat_q_mth_avg_6m, 
AVG(a.n_LOS_plat_q_mth) OVER (PARTITION BY cur_plat_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_plat_q_mth_avg_9m, 
AVG(a.n_LOS_plat_q_mth) OVER (PARTITION BY cur_plat_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_plat_q_mth_avg_12m, 
a.n_LOS_plat_qv_mth, 
a.n_LOS_plat_q1_mth, 
a.n_LOS_plat_q2_mth, 
a.n_LOS_plat_leaves, 
a.n_LOS_plat_pv_per_member, 
a.n_los_plat_head_frontln_ABOs, 
a.n_los_plat_head_frontln_customers, 
a.n_los_plat_head_frontln_q_mth, 
AVG(a.n_los_plat_head_frontln_q_mth) OVER (PARTITION BY cur_plat_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_plat_head_frontln_q_mth_avg_3m, 
AVG(a.n_los_plat_head_frontln_q_mth) OVER (PARTITION BY cur_plat_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_plat_head_frontln_q_mth_avg_6m, 
AVG(a.n_los_plat_head_frontln_q_mth) OVER (PARTITION BY cur_plat_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_plat_head_frontln_q_mth_avg_9m, 
AVG(a.n_los_plat_head_frontln_q_mth) OVER (PARTITION BY cur_plat_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_plat_head_frontln_q_mth_avg_12m, 
a.n_los_plat_head_frontln_qv_mth, 
a.n_los_plat_head_frontln_q1_mth, 
a.n_los_plat_head_frontln_q2_mth 
FROM support_los_plat_vw a
"""


los_emerald_view_query = """
SELECT  
b.mo_yr_key_no, 
b.cur_emd_imc_no, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' THEN 1 ELSE 0 END) as n_LOS_emd_ABOs, 
SUM(CASE WHEN b.local_imc_type_desc = 'Special AA(SAA)' THEN 1 WHEN b.local_imc_type_desc = 'Services Sites(SS)' THEN 1 WHEN b.local_imc_type_desc = 'Probationary AA(PAA)' THEN 1 ELSE 0 END) as n_LOS_emd_ABOs_AA, 
SUM(CASE WHEN b.local_imc_type_desc = 'Sales Representative(SR)' THEN 1 ELSE 0 END) as n_LOS_emd_ABOs_SR, 
SUM(CASE WHEN b.engage_flg = 'Y' THEN 1 ELSE 0 END) as n_LOS_emd_ABOs_engaged, 
SUM(CASE WHEN b.globl_imc_type_cd = 'C' THEN 1 ELSE 0 END) as n_LOS_emd_customers, 
SUM(s.i_repeat_customer) as n_LOS_emd_customers_repeat, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_emd_q_mth, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_QV_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_emd_qv_mth, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q1_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_emd_q1_mth, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q2_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_emd_q2_mth, 
SUM(CASE WHEN b.frontln_distb_cnt = 0 THEN 1 ELSE 0 END) as n_LOS_emd_leaves, 
nvl(SUM(b.imc_prsnl_pv) / COUNT(b.globl_imc_type_cd) ,0) as n_LOS_emd_pv_per_member, 
SUM(CASE WHEN b.spon_imc_key_no = c.spon_imc_key_no and b.globl_imc_type_cd = 'I' THEN 1 ELSE 0 END) as n_LOS_emd_head_frontln_ABOs, 
SUM(CASE WHEN b.spon_imc_key_no = c.spon_imc_key_no and b.globl_imc_type_cd = 'C' THEN 1 ELSE 0 END) as n_LOS_emd_head_frontln_customers, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_emd_head_frontln_q_mth, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_QV_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_emd_head_frontln_qv_mth, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q1_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_emd_head_frontln_q1_mth, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q2_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_emd_head_frontln_q2_mth 
FROM behaviors_combined b 
LEFT JOIN support_repeat_customers_vw s 
ON s.imc_key_no = b.imc_key_no and s.mo_yr_key_no = b.mo_yr_key_no 
LEFT JOIN (select 
    distinct 
    imc_key_no as spon_imc_key_no, 
    imc_no as spon_imc_no 
    from behaviors_combined 
    where imc_no = cur_emd_imc_no and cntry_key_no IN ({cntry_list})) c 
ON c.spon_imc_no = b.cur_emd_imc_no 
WHERE b.mo_yr_key_no >= '2013-01-01' and b.globl_bus_stat_cd = 'ACTIVE' and b.cntry_key_no IN ({cntry_list})
GROUP BY b.cur_emd_imc_no, b.mo_yr_key_no
"""


los_emerald_table_query = """
SELECT 
a.mo_yr_key_no, 
a.cur_emd_imc_no, 
CASE WHEN a.cur_emd_imc_no = '-1' THEN 0 WHEN a.cur_emd_imc_no = '1' THEN 0 ELSE 1 END as n_LOS_emd_head_exists, 
a.n_LOS_emd_ABOs, 
a.n_LOS_emd_ABOs_AA, 
a.n_LOS_emd_ABOs_SR, 
a.n_LOS_emd_ABOs_engaged, 
a.n_LOS_emd_customers, 
AVG(a.n_LOS_emd_customers) OVER (PARTITION BY cur_emd_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_emd_customers_avg_3m, 
AVG(a.n_LOS_emd_customers) OVER (PARTITION BY cur_emd_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_emd_customers_avg_6m, 
AVG(a.n_LOS_emd_customers) OVER (PARTITION BY cur_emd_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_emd_customers_avg_9m, 
AVG(a.n_LOS_emd_customers) OVER (PARTITION BY cur_emd_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_emd_customers_avg_12m, 
a.n_LOS_emd_customers_repeat, 
AVG(a.n_LOS_emd_customers_repeat) OVER (PARTITION BY cur_emd_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_emd_customers_repeat_avg_3m, 
AVG(a.n_LOS_emd_customers_repeat) OVER (PARTITION BY cur_emd_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_emd_customers_repeat_avg_6m, 
AVG(a.n_LOS_emd_customers_repeat) OVER (PARTITION BY cur_emd_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_emd_customers_repeat_avg_9m, 
AVG(a.n_LOS_emd_customers_repeat) OVER (PARTITION BY cur_emd_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_emd_customers_repeat_avg_12m, 
a.n_LOS_emd_q_mth, 
AVG(a.n_LOS_emd_q_mth) OVER (PARTITION BY cur_emd_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_emd_q_mth_avg_3m, 
AVG(a.n_LOS_emd_q_mth) OVER (PARTITION BY cur_emd_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_emd_q_mth_avg_6m, 
AVG(a.n_LOS_emd_q_mth) OVER (PARTITION BY cur_emd_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_emd_q_mth_avg_9m, 
AVG(a.n_LOS_emd_q_mth) OVER (PARTITION BY cur_emd_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_emd_q_mth_avg_12m, 
a.n_LOS_emd_qv_mth, 
a.n_LOS_emd_q1_mth, 
a.n_LOS_emd_q2_mth, 
a.n_LOS_emd_leaves, 
a.n_LOS_emd_pv_per_member, 
a.n_los_emd_head_frontln_ABOs, 
a.n_los_emd_head_frontln_customers, 
a.n_los_emd_head_frontln_q_mth, 
AVG(a.n_los_emd_head_frontln_q_mth) OVER (PARTITION BY cur_emd_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_emd_head_frontln_q_mth_avg_3m, 
AVG(a.n_los_emd_head_frontln_q_mth) OVER (PARTITION BY cur_emd_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_emd_head_frontln_q_mth_avg_6m, 
AVG(a.n_los_emd_head_frontln_q_mth) OVER (PARTITION BY cur_emd_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_emd_head_frontln_q_mth_avg_9m, 
AVG(a.n_los_emd_head_frontln_q_mth) OVER (PARTITION BY cur_emd_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_emd_head_frontln_q_mth_avg_12m, 
a.n_los_emd_head_frontln_qv_mth, 
a.n_los_emd_head_frontln_q1_mth, 
a.n_los_emd_head_frontln_q2_mth 
FROM support_los_emerald_vw a
"""


los_silver_view_query = """
SELECT  
b.mo_yr_key_no, 
b.cur_sil_imc_no, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' THEN 1 ELSE 0 END) as n_LOS_sil_ABOs, 
SUM(CASE WHEN b.local_imc_type_desc = 'Special AA(SAA)' THEN 1 WHEN b.local_imc_type_desc = 'Services Sites(SS)' THEN 1 WHEN b.local_imc_type_desc = 'Probationary AA(PAA)' THEN 1 ELSE 0 END) as n_LOS_sil_ABOs_AA, 
SUM(CASE WHEN b.local_imc_type_desc = 'Sales Representative(SR)' THEN 1 ELSE 0 END) as n_LOS_sil_ABOs_SR, 
SUM(CASE WHEN b.engage_flg = 'Y' THEN 1 ELSE 0 END) as n_LOS_sil_ABOs_engaged, 
SUM(CASE WHEN b.globl_imc_type_cd = 'C' THEN 1 ELSE 0 END) as n_LOS_sil_customers, 
SUM(s.i_repeat_customer) as n_LOS_sil_customers_repeat, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_sil_q_mth, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_QV_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_sil_qv_mth, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q1_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_sil_q1_mth, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q2_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_sil_q2_mth, 
SUM(CASE WHEN b.frontln_distb_cnt = 0 THEN 1 ELSE 0 END) as n_LOS_sil_leaves, 
nvl(SUM(b.imc_prsnl_pv) / COUNT(b.globl_imc_type_cd) ,0) as n_LOS_sil_pv_per_member, 
SUM(CASE WHEN b.spon_imc_key_no = c.spon_imc_key_no and b.globl_imc_type_cd = 'I' THEN 1 ELSE 0 END) as n_LOS_sil_head_frontln_ABOs, 
SUM(CASE WHEN b.spon_imc_key_no = c.spon_imc_key_no and b.globl_imc_type_cd = 'C' THEN 1 ELSE 0 END) as n_LOS_sil_head_frontln_customers, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_sil_head_frontln_q_mth, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_QV_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_sil_head_frontln_qv_mth, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q1_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_sil_head_frontln_q1_mth, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q2_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_sil_head_frontln_q2_mth 
FROM behaviors_combined b 
LEFT JOIN support_repeat_customers_vw s 
ON s.imc_key_no = b.imc_key_no and s.mo_yr_key_no = b.mo_yr_key_no 
LEFT JOIN (select 
    distinct 
    imc_key_no as spon_imc_key_no, 
    imc_no as spon_imc_no 
    from behaviors_combined 
    where imc_no = cur_sil_imc_no and cntry_key_no IN ({cntry_list})) c 
ON c.spon_imc_no = b.cur_sil_imc_no 
WHERE b.mo_yr_key_no >= '2013-01-01' and b.globl_bus_stat_cd = 'ACTIVE' and b.cntry_key_no IN ({cntry_list}) 
GROUP BY b.cur_sil_imc_no, b.mo_yr_key_no
"""


los_silver_table_query = """
SELECT 
a.mo_yr_key_no, 
a.cur_sil_imc_no, 
CASE WHEN a.cur_sil_imc_no = '-1' THEN 0 WHEN a.cur_sil_imc_no = '1' THEN 0 ELSE 1 END as n_LOS_sil_head_exists, 
a.n_LOS_sil_ABOs, 
a.n_LOS_sil_ABOs_AA, 
a.n_LOS_sil_ABOs_SR, 
a.n_LOS_sil_ABOs_engaged, 
a.n_LOS_sil_customers, 
AVG(a.n_LOS_sil_customers) OVER (PARTITION BY cur_sil_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_sil_customers_avg_3m, 
AVG(a.n_LOS_sil_customers) OVER (PARTITION BY cur_sil_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_sil_customers_avg_6m, 
AVG(a.n_LOS_sil_customers) OVER (PARTITION BY cur_sil_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_sil_customers_avg_9m, 
AVG(a.n_LOS_sil_customers) OVER (PARTITION BY cur_sil_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_sil_customers_avg_12m, 
a.n_LOS_sil_customers_repeat, 
AVG(a.n_LOS_sil_customers_repeat) OVER (PARTITION BY cur_sil_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_sil_customers_repeat_avg_3m, 
AVG(a.n_LOS_sil_customers_repeat) OVER (PARTITION BY cur_sil_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_sil_customers_repeat_avg_6m, 
AVG(a.n_LOS_sil_customers_repeat) OVER (PARTITION BY cur_sil_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_sil_customers_repeat_avg_9m, 
AVG(a.n_LOS_sil_customers_repeat) OVER (PARTITION BY cur_sil_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_sil_customers_repeat_avg_12m, 
a.n_LOS_sil_q_mth, 
AVG(a.n_LOS_sil_q_mth) OVER (PARTITION BY cur_sil_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_sil_q_mth_avg_3m, 
AVG(a.n_LOS_sil_q_mth) OVER (PARTITION BY cur_sil_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_sil_q_mth_avg_6m, 
AVG(a.n_LOS_sil_q_mth) OVER (PARTITION BY cur_sil_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_sil_q_mth_avg_9m, 
AVG(a.n_LOS_sil_q_mth) OVER (PARTITION BY cur_sil_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_sil_q_mth_avg_12m, 
a.n_LOS_sil_qv_mth, 
a.n_LOS_sil_q1_mth, 
a.n_LOS_sil_q2_mth, 
a.n_LOS_sil_leaves, 
a.n_LOS_sil_pv_per_member, 
a.n_los_sil_head_frontln_ABOs, 
a.n_los_sil_head_frontln_customers, 
a.n_los_sil_head_frontln_q_mth, 
AVG(a.n_los_sil_head_frontln_q_mth) OVER (PARTITION BY cur_sil_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_sil_head_frontln_q_mth_avg_3m, 
AVG(a.n_los_sil_head_frontln_q_mth) OVER (PARTITION BY cur_sil_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_sil_head_frontln_q_mth_avg_6m, 
AVG(a.n_los_sil_head_frontln_q_mth) OVER (PARTITION BY cur_sil_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_sil_head_frontln_q_mth_avg_9m, 
AVG(a.n_los_sil_head_frontln_q_mth) OVER (PARTITION BY cur_sil_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_sil_head_frontln_q_mth_avg_12m, 
a.n_los_sil_head_frontln_qv_mth, 
a.n_los_sil_head_frontln_q1_mth, 
a.n_los_sil_head_frontln_q2_mth 
FROM support_los_silver_vw a
"""


los_gold_view_query = """
SELECT  
b.mo_yr_key_no, 
b.cur_gld_imc_no, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' THEN 1 ELSE 0 END) as n_LOS_gld_ABOs, SUM(CASE WHEN b.local_imc_type_desc = 'Special AA(SAA)' THEN 1 WHEN b.local_imc_type_desc = 'Services Sites(SS)' THEN 1 WHEN b.local_imc_type_desc = 'Probationary AA(PAA)' THEN 1 ELSE 0 END) as n_LOS_gld_ABOs_AA, 
SUM(CASE WHEN b.local_imc_type_desc = 'Sales Representative(SR)' THEN 1 ELSE 0 END) as n_LOS_gld_ABOs_SR, 
SUM(CASE WHEN b.engage_flg = 'Y' THEN 1 ELSE 0 END) as n_LOS_gld_ABOs_engaged, 
SUM(CASE WHEN b.globl_imc_type_cd = 'C' THEN 1 ELSE 0 END) as n_LOS_gld_customers, 
SUM(s.i_repeat_customer) as n_LOS_gld_customers_repeat, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_gld_q_mth, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_QV_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_gld_qv_mth, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q1_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_gld_q1_mth, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q2_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_gld_q2_mth, 
SUM(CASE WHEN b.frontln_distb_cnt = 0 THEN 1 ELSE 0 END) as n_LOS_gld_leaves, 
nvl(SUM(b.imc_prsnl_pv) / COUNT(b.globl_imc_type_cd) ,0) as n_LOS_gld_pv_per_member, 
SUM(CASE WHEN b.spon_imc_key_no = c.spon_imc_key_no and b.globl_imc_type_cd = 'I' THEN 1 ELSE 0 END) as n_LOS_gld_head_frontln_ABOs, 
SUM(CASE WHEN b.spon_imc_key_no = c.spon_imc_key_no and b.globl_imc_type_cd = 'C' THEN 1 ELSE 0 END) as n_LOS_gld_head_frontln_customers, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_gld_head_frontln_q_mth, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_QV_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_gld_head_frontln_qv_mth, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q1_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_gld_head_frontln_q1_mth, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q2_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_gld_head_frontln_q2_mth 
FROM behaviors_combined b 
LEFT JOIN support_repeat_customers_vw s 
ON s.imc_key_no = b.imc_key_no and s.mo_yr_key_no = b.mo_yr_key_no 
LEFT JOIN (select 
    distinct 
    imc_key_no as spon_imc_key_no, 
    imc_no as spon_imc_no 
    from behaviors_combined 
    where imc_no = cur_gld_imc_no and cntry_key_no IN ({cntry_list})) c 
ON c.spon_imc_no = b.cur_gld_imc_no 
WHERE b.mo_yr_key_no >= '2013-01-01' and b.globl_bus_stat_cd = 'ACTIVE' and b.cntry_key_no IN ({cntry_list}) 
GROUP BY b.cur_gld_imc_no, b.mo_yr_key_no
"""


los_gold_table_query = """
SELECT 
a.mo_yr_key_no, 
a.cur_gld_imc_no, 
CASE WHEN a.cur_gld_imc_no = '-1' THEN 0 WHEN a.cur_gld_imc_no = '1' THEN 0 ELSE 1 END as n_LOS_gld_head_exists, 
a.n_LOS_gld_ABOs, 
a.n_LOS_gld_ABOs_AA, 
a.n_LOS_gld_ABOs_SR, 
a.n_LOS_gld_ABOs_engaged, 
a.n_LOS_gld_customers, 
AVG(a.n_LOS_gld_customers) OVER (PARTITION BY cur_gld_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_gld_customers_avg_3m, 
AVG(a.n_LOS_gld_customers) OVER (PARTITION BY cur_gld_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_gld_customers_avg_6m, 
AVG(a.n_LOS_gld_customers) OVER (PARTITION BY cur_gld_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_gld_customers_avg_9m, 
AVG(a.n_LOS_gld_customers) OVER (PARTITION BY cur_gld_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_gld_customers_avg_12m, 
a.n_LOS_gld_customers_repeat, AVG(a.n_LOS_gld_customers_repeat) OVER (PARTITION BY cur_gld_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_gld_customers_repeat_avg_3m, 
AVG(a.n_LOS_gld_customers_repeat) OVER (PARTITION BY cur_gld_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_gld_customers_repeat_avg_6m, 
AVG(a.n_LOS_gld_customers_repeat) OVER (PARTITION BY cur_gld_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_gld_customers_repeat_avg_9m, 
AVG(a.n_LOS_gld_customers_repeat) OVER (PARTITION BY cur_gld_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_gld_customers_repeat_avg_12m, 
a.n_LOS_gld_q_mth, 
AVG(a.n_LOS_gld_q_mth) OVER (PARTITION BY cur_gld_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_gld_q_mth_avg_3m, 
AVG(a.n_LOS_gld_q_mth) OVER (PARTITION BY cur_gld_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_gld_q_mth_avg_6m, 
AVG(a.n_LOS_gld_q_mth) OVER (PARTITION BY cur_gld_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_gld_q_mth_avg_9m, 
AVG(a.n_LOS_gld_q_mth) OVER (PARTITION BY cur_gld_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_gld_q_mth_avg_12m, 
a.n_LOS_gld_qv_mth, 
a.n_LOS_gld_q1_mth, 
a.n_LOS_gld_q2_mth, 
a.n_LOS_gld_leaves, 
a.n_LOS_gld_pv_per_member, 
a.n_los_gld_head_frontln_ABOs, 
a.n_los_gld_head_frontln_customers, 
a.n_los_gld_head_frontln_q_mth, 
AVG(a.n_los_gld_head_frontln_q_mth) OVER (PARTITION BY cur_gld_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_gld_head_frontln_q_mth_avg_3m, 
AVG(a.n_los_gld_head_frontln_q_mth) OVER (PARTITION BY cur_gld_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_gld_head_frontln_q_mth_avg_6m, 
AVG(a.n_los_gld_head_frontln_q_mth) OVER (PARTITION BY cur_gld_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_gld_head_frontln_q_mth_avg_9m, 
AVG(a.n_los_gld_head_frontln_q_mth) OVER (PARTITION BY cur_gld_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_gld_head_frontln_q_mth_avg_12m, 
a.n_los_gld_head_frontln_qv_mth, 
a.n_los_gld_head_frontln_q1_mth, 
a.n_los_gld_head_frontln_q2_mth 
FROM support_los_gold_vw a
"""


los_dia_view_query = """
SELECT  
b.mo_yr_key_no, 
b.cur_dia_imc_no, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' THEN 1 ELSE 0 END) as n_LOS_dia_ABOs, 
SUM(CASE WHEN b.local_imc_type_desc = 'Special AA(SAA)' THEN 1 WHEN b.local_imc_type_desc = 'Services Sites(SS)' THEN 1 WHEN b.local_imc_type_desc = 'Probationary AA(PAA)' THEN 1 ELSE 0 END) as n_LOS_dia_ABOs_AA, 
SUM(CASE WHEN b.local_imc_type_desc = 'Sales Representative(SR)' THEN 1 ELSE 0 END) as n_LOS_dia_ABOs_SR, 
SUM(CASE WHEN b.engage_flg = 'Y' THEN 1 ELSE 0 END) as n_LOS_dia_ABOs_engaged, 
SUM(CASE WHEN b.globl_imc_type_cd = 'C' THEN 1 ELSE 0 END) as n_LOS_dia_customers, 
SUM(s.i_repeat_customer) as n_LOS_dia_customers_repeat, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_dia_q_mth, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_QV_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_dia_qv_mth, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q1_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_dia_q1_mth, 
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q2_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_dia_q2_mth, 
SUM(CASE WHEN b.frontln_distb_cnt = 0 THEN 1 ELSE 0 END) as n_LOS_dia_leaves, 
nvl(SUM(b.imc_prsnl_pv) / COUNT(b.globl_imc_type_cd) ,0) as n_LOS_dia_pv_per_member, 
SUM(CASE WHEN b.spon_imc_key_no = c.spon_imc_key_no and b.globl_imc_type_cd = 'I' THEN 1 ELSE 0 END) as n_LOS_dia_head_frontln_ABOs, 
SUM(CASE WHEN b.spon_imc_key_no = c.spon_imc_key_no and b.globl_imc_type_cd = 'C' THEN 1 ELSE 0 END) as n_LOS_dia_head_frontln_customers, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_dia_head_frontln_q_mth, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_QV_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_dia_head_frontln_qv_mth, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q1_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_dia_head_frontln_q1_mth, 
SUM(CASE WHEN (b.spon_imc_key_no = c.spon_imc_key_no) and b.globl_imc_type_cd = 'I' and b.IMC_SLVR_PROD_Q2_MO_FLG = 'Y' THEN 1 ELSE 0 END) as n_LOS_dia_head_frontln_q2_mth 
FROM behaviors_combined b 
LEFT JOIN support_repeat_customers_vw s 
ON s.imc_key_no = b.imc_key_no and s.mo_yr_key_no = b.mo_yr_key_no 
LEFT JOIN (select 
    distinct 
    imc_key_no as spon_imc_key_no, 
    imc_no as spon_imc_no 
    from behaviors_combined 
    where imc_no = cur_dia_imc_no and cntry_key_no IN ({cntry_list})) c 
ON c.spon_imc_no = b.cur_dia_imc_no 
WHERE b.mo_yr_key_no >= '2013-01-01' and b.globl_bus_stat_cd = 'ACTIVE' and b.cntry_key_no IN ({cntry_list}) 
GROUP BY b.cur_dia_imc_no, b.mo_yr_key_no
"""


los_dia_table_query = """
SELECT 
a.mo_yr_key_no, 
a.cur_dia_imc_no, 
CASE WHEN a.cur_dia_imc_no = '-1' THEN 0 WHEN a.cur_dia_imc_no = '1' THEN 0 ELSE 1 END as n_LOS_dia_head_exists, 
a.n_LOS_dia_ABOs, 
a.n_LOS_dia_ABOs_AA, 
a.n_LOS_dia_ABOs_SR, 
a.n_LOS_dia_ABOs_engaged, 
a.n_LOS_dia_customers, 
AVG(a.n_LOS_dia_customers) OVER (PARTITION BY cur_dia_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_dia_customers_avg_3m, 
AVG(a.n_LOS_dia_customers) OVER (PARTITION BY cur_dia_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_dia_customers_avg_6m, 
AVG(a.n_LOS_dia_customers) OVER (PARTITION BY cur_dia_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_dia_customers_avg_9m, 
AVG(a.n_LOS_dia_customers) OVER (PARTITION BY cur_dia_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_dia_customers_avg_12m, 
a.n_LOS_dia_customers_repeat, 
AVG(a.n_LOS_dia_customers_repeat) OVER (PARTITION BY cur_dia_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_dia_customers_repeat_avg_3m, 
AVG(a.n_LOS_dia_customers_repeat) OVER (PARTITION BY cur_dia_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_dia_customers_repeat_avg_6m, 
AVG(a.n_LOS_dia_customers_repeat) OVER (PARTITION BY cur_dia_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_dia_customers_repeat_avg_9m, 
AVG(a.n_LOS_dia_customers_repeat) OVER (PARTITION BY cur_dia_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_dia_customers_repeat_avg_12m, 
a.n_LOS_dia_q_mth, 
AVG(a.n_LOS_dia_q_mth) OVER (PARTITION BY cur_dia_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_dia_q_mth_avg_3m, 
AVG(a.n_LOS_dia_q_mth) OVER (PARTITION BY cur_dia_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_dia_q_mth_avg_6m, 
AVG(a.n_LOS_dia_q_mth) OVER (PARTITION BY cur_dia_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_dia_q_mth_avg_9m, 
AVG(a.n_LOS_dia_q_mth) OVER (PARTITION BY cur_dia_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_dia_q_mth_avg_12m, 
a.n_LOS_dia_qv_mth, 
a.n_LOS_dia_q1_mth, 
a.n_LOS_dia_q2_mth, 
a.n_LOS_dia_leaves, 
a.n_LOS_dia_pv_per_member, 
a.n_los_dia_head_frontln_ABOs, 
a.n_los_dia_head_frontln_customers, 
a.n_los_dia_head_frontln_q_mth, 
AVG(a.n_los_dia_head_frontln_q_mth) OVER (PARTITION BY cur_dia_imc_no ORDER BY a.mo_yr_key_no ROWS 2 PRECEDING) as n_los_dia_head_frontln_q_mth_avg_3m, 
AVG(a.n_los_dia_head_frontln_q_mth) OVER (PARTITION BY cur_dia_imc_no ORDER BY a.mo_yr_key_no ROWS 5 PRECEDING) as n_los_dia_head_frontln_q_mth_avg_6m, 
AVG(a.n_los_dia_head_frontln_q_mth) OVER (PARTITION BY cur_dia_imc_no ORDER BY a.mo_yr_key_no ROWS 8 PRECEDING) as n_los_dia_head_frontln_q_mth_avg_9m, 
AVG(a.n_los_dia_head_frontln_q_mth) OVER (PARTITION BY cur_dia_imc_no ORDER BY a.mo_yr_key_no ROWS 11 PRECEDING) as n_los_dia_head_frontln_q_mth_avg_12m, 
a.n_los_dia_head_frontln_qv_mth, 
a.n_los_dia_head_frontln_q1_mth, 
a.n_los_dia_head_frontln_q2_mth 
FROM support_los_diamond_vw a
"""


abo_dna_downline_indiv_view_query = """
SELECT b.spon_imc_key_no as imc_key_no,    
b.mo_yr_key_no,     
COUNT(b.imc_key_no) as n_downline_all,  
SUM(CASE WHEN b.globl_imc_type_cd = 'I' THEN 1 ELSE 0 END) as n_downline_ABOs,  
SUM(CASE WHEN b.globl_imc_type_cd = 'C' THEN 1 ELSE 0 END) as n_downline_customers,     
SUM(s.i_repeat_customer) as n_downline_customers_repeat,    
SUM(b.imc_prsnl_pv) as n_downline_prsnl_pv_sum,     
SUM(CASE WHEN b.globl_imc_type_cd = 'C' THEN b.imc_prsnl_pv ELSE 0 END) as n_downline_customer_prsnl_pv_sum,    
SUM(CASE WHEN b.globl_imc_type_cd = 'I' THEN b.imc_prsnl_pv ELSE 0 END) as n_downline_ABO_prsnl_pv_sum,     
SUM(b.imc_group_pv) as n_downline_group_pv_sum,     
SUM(b.dmd_ord_cnt) as n_downline_dmd_ord_sum,   
SUM(CASE WHEN b.dmd_ord_cnt > 0 THEN 1 ELSE 0 END) as n_downline_dmd_ord_nonzero,   
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.imc_spon_flg = 'Y' THEN 1 ELSE 0 END) as n_downline_sponsor,  
SUM(CASE WHEN b.cur_awd_awd_rnk_no = '*NA' THEN 0 WHEN b.cur_awd_awd_rnk_no = '-1' THEN 0 ELSE 1 END) as n_downline_awd_achieved,   
SUM(CASE WHEN b.cur_awd_awd_rnk_no = '310' THEN 1 ELSE 0 END) as n_downline_310_awd,    
SUM(CASE WHEN b.cur_awd_awd_rnk_no = '320' THEN 1 ELSE 0 END) as n_downline_320_awd,    
MAX(replace(b.cur_awd_awd_rnk_no, '*NA', '0')) as i_downline_awd_max,   
SUM(b.frontln_distb_cnt) as n_downline_frontln_ABO_sum,     
SUM(b.frontln_cust_cnt) as n_downline_frontln_customer_sum,     
SUM(CASE WHEN b.bns_pct >= 9 THEN 1 ELSE 0 END) as n_downline_9plus_bns_pct_mth,    
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.imc_slvr_prod_mo_flg = 'Y' THEN 1 ELSE 0 END) as n_downline_q_mth,    
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.imc_slvr_prod_QV_mo_flg = 'Y' THEN 1 ELSE 0 END) as n_downline_qv_mth,    
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.imc_slvr_prod_Q1_mo_flg = 'Y' THEN 1 ELSE 0 END) as n_downline_q1_mth,    
SUM(CASE WHEN b.globl_imc_type_cd = 'I' and b.imc_slvr_prod_Q2_mo_flg = 'Y' THEN 1 ELSE 0 END) as n_downline_q2_mth      
FROM behaviors_combined b 
LEFT JOIN support_repeat_customers_vw s 
ON s.imc_key_no = b.imc_key_no and s.mo_yr_key_no = b.mo_yr_key_no 
WHERE b.cntry_key_no IN ({cntry_list}) and b.globl_bus_stat_cd = 'ACTIVE' 
group by b.spon_imc_key_no, b.mo_yr_key_no
"""


percentile_rank_query = """
SELECT 
b.imc_key_no, 
b.mo_yr_key_no,  
percent_rank() over (partition by b.mo_yr_key_no order by b.bns_grs_pln_comm_curr_amt) as n_percentile_overall_grs_bns, 
percent_rank() over (partition by b.mo_yr_key_no order by b.imc_prsnl_pv) as n_percentile_overall_prsnl_pv, 
percent_rank() over (partition by b.mo_yr_key_no order by b.imc_prsnl_bv) as n_percentile_overall_prsnl_bv, 
percent_rank() over (partition by b.mo_yr_key_no order by b.imc_group_pv) as n_percentile_overall_group_pv, 
percent_rank() over (partition by b.mo_yr_key_no order by b.imc_group_bv) as n_percentile_overall_group_bv,  
percent_rank() over (partition by b.mo_yr_key_no, b.reg_shop_prvnc_nm order by b.bns_grs_pln_comm_curr_amt) as n_percentile_region_grs_bns, 
percent_rank() over (partition by b.mo_yr_key_no, b.reg_shop_prvnc_nm order by b.imc_prsnl_pv) as n_percentile_region_prsnl_pv, 
percent_rank() over (partition by b.mo_yr_key_no, b.reg_shop_prvnc_nm order by b.imc_prsnl_bv) as n_percentile_region_prsnl_bv, 
percent_rank() over (partition by b.mo_yr_key_no, b.reg_shop_prvnc_nm order by b.imc_group_pv) as n_percentile_region_group_pv, 
percent_rank() over (partition by b.mo_yr_key_no, b.reg_shop_prvnc_nm order by b.imc_group_bv) as n_percentile_region_group_bv,   
percent_rank() over (partition by b.mo_yr_key_no, b.cur_awd_awd_rnk_no order by b.bns_grs_pln_comm_curr_amt) as n_percentile_awd_rnk_grs_bns, 
percent_rank() over (partition by b.mo_yr_key_no, b.cur_awd_awd_rnk_no order by b.imc_prsnl_pv) as n_percentile_awd_rnk_prsnl_pv, 
percent_rank() over (partition by b.mo_yr_key_no, b.cur_awd_awd_rnk_no order by b.imc_prsnl_bv) as n_percentile_awd_rnk_prsnl_bv, 
percent_rank() over (partition by b.mo_yr_key_no, b.cur_awd_awd_rnk_no order by b.imc_group_pv) as n_percentile_awd_rnk_group_pv, 
percent_rank() over (partition by b.mo_yr_key_no, b.cur_awd_awd_rnk_no order by b.imc_group_bv) as n_percentile_awd_rnk_group_bv  
from behaviors_combined b 
where b.globl_bus_stat_cd = 'ACTIVE' and b.globl_imc_type_cd = 'I' and b.cntry_key_no IN ({cntry_list})
"""

average_rank_query = """
SELECT 
b.mo_yr_key_no,  
avg(b.bns_grs_pln_comm_curr_amt) as overall_average_grs_bns, 
avg(b.imc_prsnl_pv) as overall_average_prsnl_pv, 
avg(b.imc_group_pv) as overall_average_group_pv, 
avg(b.imc_prsnl_bv) as overall_average_prsnl_bv, 
avg(b.imc_group_bv) as overall_average_group_bv  
from behaviors_combined b 
where b.globl_bus_stat_cd = 'ACTIVE' and b.globl_imc_type_cd = 'I' and b.cntry_key_no IN ({cntry_list}) group by b.mo_yr_key_no
"""

average_rank_region_query = """
SELECT 
b.mo_yr_key_no,  
b.reg_shop_prvnc_nm, 
avg(b.bns_grs_pln_comm_curr_amt) as region_average_grs_bns, 
avg(b.imc_prsnl_pv) as region_average_prsnl_pv, 
avg(b.imc_group_pv) as region_average_group_pv, 
avg(b.imc_prsnl_bv) as region_average_prsnl_bv, 
avg(b.imc_group_bv) as region_average_group_bv  
from behaviors_combined b 
where b.globl_bus_stat_cd = 'ACTIVE' and b.globl_imc_type_cd = 'I' and b.cntry_key_no IN ({cntry_list}) 
group by b.mo_yr_key_no, b.reg_shop_prvnc_nm
"""

average_rank_awd_rnk_query = """
SELECT 
b.mo_yr_key_no,  
b.cur_awd_awd_rnk_no, 
avg(b.bns_grs_pln_comm_curr_amt) as awd_rnk_average_grs_bns, 
avg(b.imc_prsnl_pv) as awd_rnk_average_prsnl_pv, 
avg(b.imc_group_pv) as awd_rnk_average_group_pv, 
avg(b.imc_prsnl_bv) as awd_rnk_average_prsnl_bv, 
avg(b.imc_group_bv) as awd_rnk_average_group_bv  
from behaviors_combined b 
where b.globl_bus_stat_cd = 'ACTIVE' and b.globl_imc_type_cd = 'I' and b.cntry_key_no IN ({cntry_list}) group by b.mo_yr_key_no, b.cur_awd_awd_rnk_no
"""
