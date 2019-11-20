import datetime
import os
import json
import yaml
import sys

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import split
from pyspark.sql.functions import to_date, year, month
from pyspark.sql.types import StructType, StructField, StringType

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("ERROR")

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--function', '-f', help="Input function", type=str)
parser.add_argument('--nextLambdaARN', '-nla', help="Next Lambda ARN", type=str)

sys.path.append('/home/hadoop/eap-bcg/')

from pipeline.libraries import util
from pipeline.libraries import etl
import pipeline.queries.layer2_queries as queries
from pyspark.sql.functions import expr

# Load configs
run = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/run.yml")
data_paths = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/data_paths.yml")
configs = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/layer2.yml")


def create_abo_dna_table(pargs, params):
    """
    This function creates ABO DNA table, with all the features needed for future modeling usage.

    :inputs: behaviors_combined_data, cntry_promo_imc_order, performance_year, support_promotions, support_first_indicators, support_first_indicators,
             support_los_plat, support_los_emerald, support_los_silver, support_los_gold, support_los_diamond, abo_dna_downline_indiv_vw,
             percentile_rank, average_rank, average_rank_region, average_rank_awd_rnk, per_capita_disposable_income, combined_yr_mo_awards,
             amwayhub_login, classroom_data, wechat_cloudcommerce, fc_dist_cleaned, gar_pf_cleaned, abo_dna_full_file
    :outputs: abo_dna_full_file
    """

    # Load feature data
    behaviors_combined_data = spark.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")

    cntry_promo_imc_order = spark.read.parquet(data_paths[configs['cntry_promo_imc_order']].format(run_mode=run['run_mode'], run_id=run['run_id']))
    cntry_promo_imc_order.createOrReplaceTempView("cntry_promo_imc_order")

    performance_year = spark.read.parquet(data_paths['performance_year'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    performance_year.createOrReplaceTempView("performance_year")

    support_promotions = spark.read.parquet(data_paths['support_promotions'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    support_promotions.createOrReplaceTempView("support_promotions")

    support_first_indicators = spark.read.parquet(data_paths['support_first_indicators'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    if configs['if_dedup']:
        support_first_indicators = support_first_indicators.dropDuplicates(['imc_key_no', 'mo_yr_key_no'])

    support_first_indicators.createOrReplaceTempView("support_first_indicators")

    support_los_plat = spark.read.parquet(data_paths['support_los_plat'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    support_los_plat.createOrReplaceTempView("support_los_plat")

    support_los_emerald = spark.read.parquet(data_paths['support_los_emerald'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    support_los_emerald.createOrReplaceTempView("support_los_emerald")

    support_los_silver = spark.read.parquet(data_paths['support_los_silver'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    support_los_silver.createOrReplaceTempView("support_los_silver")

    support_los_gold = spark.read.parquet(data_paths['support_los_gold'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    support_los_gold.createOrReplaceTempView("support_los_gold")

    support_los_diamond = spark.read.parquet(data_paths['support_los_diamond'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    support_los_diamond.createOrReplaceTempView("support_los_diamond")

    abo_dna_downline_indiv_vw = spark.read.parquet(data_paths['abo_dna_downline_indiv_vw'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    abo_dna_downline_indiv_vw.createOrReplaceTempView("abo_dna_downline_indiv_vw")

    percentile_rank = spark.read.parquet(data_paths['percentile_rank'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    percentile_rank.createOrReplaceTempView("percentile_rank")

    average_rank = spark.read.parquet(data_paths['average_rank'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    average_rank.createOrReplaceTempView("average_rank")

    average_rank_region = spark.read.parquet(data_paths['average_rank_region'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    average_rank_region.createOrReplaceTempView("average_rank_region")

    average_rank_awd_rnk = spark.read.parquet(data_paths['average_rank_awd_rnk'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    average_rank_awd_rnk.createOrReplaceTempView("average_rank_awd_rnk")

    per_capita_disposable_income = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",").load(
        data_paths['per_capita_disposable_income'])
    per_capita_disposable_income.createOrReplaceTempView("per_capita_disposable_income_v3")

    combined_yr_mo_awards = spark.read.parquet(data_paths['combined_yr_mo_awards'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    combined_yr_mo_awards.createOrReplaceTempView("combined_yr_mo_awards")

    amwayhub_login = spark.read.parquet(data_paths['amwayhub_login'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    amwayhub_login.createOrReplaceTempView("amwayhub_login")

    classroom_data = spark.read.parquet(data_paths['classroom_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    classroom_data.createOrReplaceTempView("classroom_data")

    # add wechat cloudcommerce features
    wechat_cloudcommerce = spark.read.parquet(data_paths['wechat_cloudcommerce'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    wechat_cloudcommerce.createOrReplaceTempView("wechat_cloudcommerce")

    fc_dist = spark.read.parquet(data_paths["fc_dist_cleaned"].format(run_mode=run['run_mode'], run_id=run['run_id']))
    fc_dist.createOrReplaceTempView('fc_dist')

    # seperate behaviors_combined_data into parts by distinct imc_key_no
    abo_id_table = behaviors_combined_data.select("imc_key_no").distinct()
    abo_id_groups = abo_id_table.randomSplit([1.0 / configs['num_parts'] for x in range(configs['num_parts'])], seed=configs['seed'])

    # add gar_pf features
    gar_pf_cleaned = spark.read.parquet(data_paths['gar_pf_cleaned'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    gar_pf_cleaned.createOrReplaceTempView("gar_pf_cleaned")

    # Save unique members ID lists
    for i in range(0, configs['num_parts']):
        abo_id_group_df = abo_id_groups[i]
        abo_id_group_df.write.mode('overwrite').parquet(data_paths['abo_id_group_df'].format(run_mode=run['run_mode'], run_id=run['run_id'], part_num=i))

    # Create smaller dataset with only columns used to solve computation issue.
    columns_used = configs['columns_used']
    behaviors_combined_slim = behaviors_combined_data.select(columns_used)
    behaviors_combined_slim.createOrReplaceTempView("behaviors_combined_slim")

    # Divide and save behaviors_combined_slim according to abo_id_group_df
    for i in range(0, configs['num_parts']):
        abo_id_group_df = sqlContext.read.parquet(data_paths['abo_id_group_df'].format(run_mode=run['run_mode'], run_id=run['run_id'], part_num=i))
        abo_id_group_df.createOrReplaceTempView("abo_id_group_df")
        behaviors_combined_data_group = sqlContext.sql("select b.* from behaviors_combined_slim b inner join abo_id_group_df on b.imc_key_no = abo_id_group_df.imc_key_no")
        behaviors_combined_data_group.write.mode('overwrite').option("maxRecordsPerFile", configs["maxRecordsPerFile"]).parquet(
            data_paths['behaviors_combined_data_group'].format(run_mode=run['run_mode'], run_id=run['run_id'], part_num=i))

    # trim down the size of behaviors_combined to generate ABO DNA.
    up_cols = configs['up_cols']
    behaviors_combined_up = behaviors_combined_data.select(up_cols)
    behaviors_combined_up.createOrReplaceTempView("behaviors_combined_up")

    # Left join seperated behaviors_combined_data with supporting tables
    for i in range(0, configs['num_parts']):
        behaviors_combined_partial = sqlContext.read.parquet(data_paths['behaviors_combined_data_group'].format(run_mode=run['run_mode'], run_id=run['run_id'], part_num=i))
        behaviors_combined_partial.createOrReplaceTempView("behaviors_combined_partial")
        abo_dna_partial = sqlContext.sql(queries.SQL_abo_dna_for_pyspark_v6.format(cntry_list=','.join(str(x) for x in run['cntry_key_no'])))
        abo_dna_partial.write.mode('overwrite').option("maxRecordsPerFile", configs["maxRecordsPerFile"]).parquet(
            data_paths['abo_dna_partial'].format(run_mode=run['run_mode'], run_id=run['run_id'], part_num=i))

    # Finally read all these interim tables and create one table for future modeling use
    abo_dna_full = sqlContext.read.parquet(data_paths['abo_dna_partial'].format(run_mode=run['run_mode'], run_id=run['run_id'], part_num="*"))

    # Adding dt_signup variable:
    abo_dna_full = abo_dna_full.withColumn("dt_signup", expr("add_months(mo_yr_key_no, -n_imc_months_after_signup)"))

    # Create YOY & MOM secondary features
    three_mo_col, six_mo_col, lag_cols = configs['lag_column_3m'], configs['lag_column_6m'], configs['lag_column_12m']

    abo_dna_full_mom = etl.add_3mo_mom_features(abo_dna_full, three_mo_col, six_mo_col)
    abo_dna_full_mom_yoy = etl.get_yoy_features(abo_dna_full_mom, lag_cols)

    if run['use_sample']:
        abo_dna_full_mom_yoy.write.mode('overwrite').option("maxRecordsPerFile", configs["maxRecordsPerFile"]).parquet(
            data_paths['abo_dna_sample'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    else:
        abo_dna_full_mom_yoy.write.mode('overwrite').option("maxRecordsPerFile", configs["maxRecordsPerFile"]).parquet(
            data_paths['abo_dna_full_file'].format(run_mode=run['run_mode'], run_id=run['run_id']))


if __name__ == "__main__":
    args = parser.parse_args()
    # exec(pargs.function + '(pargs, params)')
    if args.nextLambdaARN is None:
        exec(args.function + '(None, None)')
    else:
        exec("util.trigger_next_lambda('" + args.nextLambdaARN + "')")
