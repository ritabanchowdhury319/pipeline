# -*- coding: utf-8 -*-

import datetime
import os
import json
import yaml
import sys
import argparse

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession


from pyspark.sql.functions import *
from pyspark.sql.window import Window

sys.path.append('/home/hadoop/eap-bcg/')

from pipeline.libraries import util
import pipeline.queries.layer1_queries as queries

# Set up environment
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("ERROR")

parser = argparse.ArgumentParser()
parser.add_argument('--function', '-f', help="Input function", type=str)
parser.add_argument('--nextLambdaARN', '-nla', help="Next Lambda ARN", type=str)

# Load configs
run = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/run.yml")
data_paths = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/data_paths.yml")
configs = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/layer1.yml")


def create_monthly_yearly_award(pargs, params):
    """
    Read in denormalized_awards and cntry_award_no data and create combined_yr_mo_awards data

    :inputs: denormalized_awards, cntry_award_no
    :outputs: combined_yr_mo_awards
    """

    df = spark.read.option("delimiter", "|").csv(data_paths['denormalized_awards'], inferSchema=True)
    award_ref = spark.read.csv(data_paths[configs['cntry_award_no']], header=True, inferSchema=True) \
        .withColumnRenamed("awd_desc_{cntry}".format(cntry=configs["cntry"]), "awd_desc_cntry")

    # Rename columns
    orig_col = df.schema.names
    for i in range(0, len(orig_col)):
        df = df.withColumnRenamed(orig_col[i], configs["de_awd_col_names"][i])

    # Filter by country and create award rank column
    rnk_scores = configs["rnk_scores"]
    monthly_awards1 = df.filter(F.col("cntry_key_no").isin(run['cntry_key_no'])) \
        .withColumn("awd_rnk_no",
                    F.when(df.CROWN_PLUS_FLG == 1, rnk_scores["CROWN_PLUS_FLG"])
                    .when(df.F_TRIPLE_DIA_FLG == 1, rnk_scores["F_TRIPLE_DIA_FLG"])
                    .when(df.TRIPLE_DIA_FLG == 1, rnk_scores["TRIPLE_DIA_FLG"])
                    .when(df.F_DOUBLE_DIA_FLG == 1, rnk_scores["F_DOUBLE_DIA_FLG"])
                    .when(df.DOUBLE_DIA_FLG == 1, rnk_scores["DOUBLE_DIA_FLG"])
                    .when(df.F_EXEC_DIA_FLG == 1, rnk_scores["F_EXEC_DIA_FLG"])
                    .when(df.EXEC_DIA_FLG == 1, rnk_scores["EXEC_DIA_FLG"])
                    .when(df.F_DIA_FLG == 1, rnk_scores["F_DIA_FLG"])
                    .when(df.DIA_FLG == 1, rnk_scores["DIA_FLG"])
                    .when(df.F_EMRLD_FLG == 1, rnk_scores["F_EMRLD_FLG"])
                    .when(df.EMRLD_FLG == 1, rnk_scores["EMRLD_FLG"])
                    .when(df.F_SAPPHIRE_FLG == 1, rnk_scores["F_SAPPHIRE_FLG"])
                    .when(df.SAPPHIRE_FLG == 1, rnk_scores["SAPPHIRE_FLG"])
                    .when(df.PEARL_FLG == 1, rnk_scores["PEARL_FLG"])
                    .when(df.F_PLAT_FLG == 1, rnk_scores["F_PLAT_FLG"])
                    .when(df.RUBY_FLG == 1, rnk_scores["RUBY_FLG"])
                    .when(df.PLAT_FLG == 1, rnk_scores["PLAT_FLG"])
                    .when(df.GOLD_FLG == 1, rnk_scores["GOLD_FLG"])
                    .when(df.SILVER_FLG == 1, rnk_scores["SILVER_FLG"])
                    .otherwise(F.lit(None))
                    ).select('imc_key_no', 'mo_yr_key_no', 'cntry_key_no', 'awd_rnk_no')

    # Merge two tables
    monthly_awards2 = monthly_awards1.withColumn("month", expr("substring(mo_yr_key_no, length(mo_yr_key_no)-1, length(mo_yr_key_no))").cast('int')) \
        .withColumn("year", expr("substring(mo_yr_key_no, 1, 4)").cast('int'))

    monthly_awards3 = monthly_awards2.withColumn("perf_yr",
                                                 when(monthly_awards2.month >= configs["first_month_of_perf_yr"], monthly_awards2.year + 1).otherwise(monthly_awards2.year)) \
        .select('imc_key_no', 'mo_yr_key_no', 'cntry_key_no', 'perf_yr', 'awd_rnk_no')

    monthly_awards = monthly_awards3.join(award_ref, monthly_awards3.awd_rnk_no == award_ref.cur_awd_awd_rnk_no, 'left') \
        .select('imc_key_no', 'mo_yr_key_no', 'cntry_key_no', 'perf_yr', 'awd_rnk_no', 'awd_desc_cntry').withColumnRenamed("awd_desc_cntry", "i_mthly_awd_cd") \
        .withColumnRenamed("awd_rnk_no", "i_mthly_awd_rnk_no")

    yearly_awards1 = monthly_awards.groupBy('imc_key_no', 'cntry_key_no', 'perf_yr').agg(F.max("i_mthly_awd_rnk_no").alias("i_yrly_awd_rnk_no"))
    yearly_awards = yearly_awards1.join(award_ref, yearly_awards1.i_yrly_awd_rnk_no == award_ref.cur_awd_awd_rnk_no, 'left')\
        .select('imc_key_no', 'perf_yr', 'awd_desc_cntry', 'i_yrly_awd_rnk_no').withColumnRenamed("awd_desc_cntry", "i_yrly_awd_cd")\
        .withColumnRenamed("awd_desc_cntry","i_yrly_awd_cd").withColumnRenamed("imc_key_no", "imc_key_no_yr").withColumnRenamed("perf_yr", "perf_yr_yr")

    combined_awards = monthly_awards.join(yearly_awards, (monthly_awards.imc_key_no == yearly_awards.imc_key_no_yr) & (monthly_awards.perf_yr == yearly_awards.perf_yr_yr), 'left') \
        .select('imc_key_no', 'mo_yr_key_no', 'cntry_key_no', 'perf_yr', 'i_mthly_awd_cd', 'i_yrly_awd_cd', 'i_mthly_awd_rnk_no', 'i_yrly_awd_rnk_no')

    combined_awards = combined_awards.withColumn('mo_yr_key_no', combined_awards.mo_yr_key_no.cast('string')).withColumn('imc_key_no', combined_awards.imc_key_no.cast('string'))
    combined_awards = combined_awards.withColumn('mo_yr_key_no', to_timestamp(combined_awards.mo_yr_key_no, 'yyyyMM')).withColumn('mo_yr_key_no', date_format('mo_yr_key_no', 'yyyy-MM-dd'))

    # Write finial result
    combined_awards.write.parquet(data_paths['combined_yr_mo_awards'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def process_behaviors_combined(pargs, params):
    """
    Process behaviors data

    :inputs: behaviors_combined_data_full, combined_awards
    :outputs: behaviors_combined_data
    """

    if run['use_sample']:
        behaviors_combined_data = sqlContext.read.parquet(data_paths['behaviors_combined_data_full'].format(run_mode=run['run_mode'], run_id=run['run_id'])).sample(False, configs[
            'sampling_fraction'], seed=configs['sampling_seed'])
    else:
        behaviors_combined_data = sqlContext.read.parquet(data_paths['behaviors_combined_data_full'].format(run_mode=run['run_mode'], run_id=run['run_id']))

    behaviors_combined_data = behaviors_combined_data.withColumn('mo_yr_key_no', F.to_date(F.col('mo_yr_key_no_str'), 'yyyyMM'))
    behaviors_combined_data = behaviors_combined_data.filter(F.col("cntry_key_no").isin(run['cntry_key_no']))

    combined_awards = sqlContext.read.parquet(data_paths['combined_yr_mo_awards'].format(run_mode=run['run_mode'], run_id=run['run_id'])) \
        .select('imc_key_no', 'mo_yr_key_no', 'i_mthly_awd_rnk_no')

    # Merge with combined_awards
    behaviors_combined_data = behaviors_combined_data \
        .drop('cur_awd_awd_rnk_no') \
        .join(combined_awards, ['imc_key_no', 'mo_yr_key_no'], 'left') \
        .withColumnRenamed('i_mthly_awd_rnk_no', 'cur_awd_awd_rnk_no')

    # Rename columns
    behaviors_combined_data = behaviors_combined_data \
        .withColumnRenamed('REG_CITY_SEG_CD', 'I_REG_CITY_SEG_CD') \
        .withColumnRenamed('REG_SHOP_CD', 'I_REG_SHOP_CD') \
        .withColumnRenamed('REG_SHOP_RGN_CD', 'I_REG_SHOP_RGN_CD') \
        .withColumnRenamed('IMC_EARN_BNS_FLG', 'I_IMC_EARN_BNS_FLG')

    # Create tenure year
    behaviors_combined_data = behaviors_combined_data \
        .withColumn('IMC_MONTHS_AFTER_SIGNUP', F.col('IMC_MONTHS_AFTER_SIGNUP').cast(IntegerType())) \
        .withColumn('I_TENURE_YEAR', F.when(F.col('IMC_MONTHS_AFTER_SIGNUP') < 0, 0) \
                    .when(F.col('IMC_MONTHS_AFTER_SIGNUP').between(0, 11), 1) \
                    .when(F.col('IMC_MONTHS_AFTER_SIGNUP').between(12, 23), 2) \
                    .when(F.col('IMC_MONTHS_AFTER_SIGNUP').between(24, 35), 3) \
                    .otherwise(4))

    # Write into new path
    behaviors_combined_data.write.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def create_cntry_promo_imc_order(pargs, params):
    """

    :inputs: cntry_promo_imc_order_data
    :outputs: cntry_promo_imc_order
    """
    # China promo counts
    cntry_promo_imc_order_data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "|").load(
        data_paths[configs['cntry_promo_imc_order_data']])
    cntry_promo_imc_order_data.createOrReplaceTempView("cntry_promo_imc_order_data")

    cntry_promo_imc_order = sqlContext.sql(queries.cntry_promo_imc_order_sql)
    cntry_promo_imc_order.write.parquet(data_paths[configs['cntry_promo_imc_order']].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def create_performance_year(pargs, params):
    """

    :inputs: behaviors_combined_data
    :outputs: performance_year
    """
    behaviors_combined_data = spark.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")

    performance_year = sqlContext.sql(queries.performance_year_query)
    performance_year.write.parquet(data_paths['performance_year'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def create_support_promotions(pargs, params):
    """
    :inputs: support_promotion_campaigns, behaviors_combined_data
    :outputs: upport_promotions
    """
    support_promotion_campaigns = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",") \
        .load(data_paths['support_promotion_campaigns'])
    support_promotion_campaigns = support_promotion_campaigns.withColumn('promo_date', F.to_date(F.col('promo_date').substr(1, 10), 'yyyy-MM-dd'))
    support_promotion_campaigns.createOrReplaceTempView("support_promotion_campaigns")

    behaviors_combined_data = spark.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")

    promo_vw1 = sqlContext.sql(queries.promo_vw1_query)
    promo_vw1.createOrReplaceTempView("promo_vw1")

    support_promotions = sqlContext.sql(queries.support_promotions_query)
    support_promotions.write.parquet(data_paths['support_promotions'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def create_support_first_indicators(pargs, params):
    """
    :inputs: behaviors_combined_data
    :outputs: support_first_indicators
    """
    behaviors_combined_data = spark.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")

    support_first_indicators = sqlContext.sql(queries.first_indicators_query)
    support_first_indicators.write.parquet(data_paths['support_first_indicators'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def create_support_repeat_customers_vw(pargs, params):
    """
    :inputs: behaviors_combined_data
    :outputs: support_repeat_customers_vw
    """
    behaviors_combined_data = spark.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")

    support_repeat_customers_vw = sqlContext.sql(queries.repeat_customers_view_query)
    support_repeat_customers_vw.write.parquet(data_paths['support_repeat_customers_vw'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def create_support_los_plat(pargs, params):
    """
    :inputs: behaviors_combined_data, support_repeat_customers_vw
    :outputs: support_los_plat
    """
    behaviors_combined_data = spark.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")

    support_repeat_customers_vw = spark.read.parquet(data_paths['support_repeat_customers_vw'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    support_repeat_customers_vw.createOrReplaceTempView("support_repeat_customers_vw")

    support_los_plat_vw = sqlContext.sql(queries.los_plat_view_query.format(cntry_list=','.join(str(x) for x in run['cntry_key_no'])))
    support_los_plat_vw.createOrReplaceTempView("support_los_plat_vw")

    support_los_plat = sqlContext.sql(queries.los_plat_table_query)
    support_los_plat.write.parquet(data_paths['support_los_plat'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def create_support_los_emerald(pargs, params):
    """
    :inputs: behaviors_combined_data, support_repeat_customers_vw
    :outputs: support_los_emerald
    """
    behaviors_combined_data = spark.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")

    support_repeat_customers_vw = spark.read.parquet(data_paths['support_repeat_customers_vw'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    support_repeat_customers_vw.createOrReplaceTempView("support_repeat_customers_vw")

    support_los_emerald_vw = sqlContext.sql(queries.los_emerald_view_query.format(cntry_list=','.join(str(x) for x in run['cntry_key_no'])))
    support_los_emerald_vw.createOrReplaceTempView("support_los_emerald_vw")

    support_los_emerald = sqlContext.sql(queries.los_emerald_table_query)
    support_los_emerald.write.parquet(data_paths['support_los_emerald'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def create_support_los_silver(pargs, params):
    """
    :inputs: behaviors_combined_data, support_repeat_customers_vw
    :outputs: support_los_silver
    """
    behaviors_combined_data = spark.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")

    support_repeat_customers_vw = spark.read.parquet(data_paths['support_repeat_customers_vw'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    support_repeat_customers_vw.createOrReplaceTempView("support_repeat_customers_vw")

    support_los_silver_vw = sqlContext.sql(queries.los_silver_view_query.format(cntry_list=','.join(str(x) for x in run['cntry_key_no'])))
    support_los_silver_vw.createOrReplaceTempView("support_los_silver_vw")

    support_los_silver = sqlContext.sql(queries.los_silver_table_query)
    support_los_silver.write.parquet(data_paths['support_los_silver'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def create_support_los_gold(pargs, params):
    """
    :inputs: behaviors_combined_data, support_repeat_customers_vw
    :outputs: support_los_gold
    """
    behaviors_combined_data = spark.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")

    support_repeat_customers_vw = spark.read.parquet(data_paths['support_repeat_customers_vw'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    support_repeat_customers_vw.createOrReplaceTempView("support_repeat_customers_vw")

    support_los_gold_vw = sqlContext.sql(queries.los_gold_view_query.format(cntry_list=','.join(str(x) for x in run['cntry_key_no'])))
    support_los_gold_vw.createOrReplaceTempView("support_los_gold_vw")

    support_los_gold = sqlContext.sql(queries.los_gold_table_query)
    support_los_gold.write.parquet(data_paths['support_los_gold'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def create_support_los_diamond(pargs, params):
    """
    :inputs: behaviors_combined_data, support_repeat_customers_vw
    :outputs: support_los_diamond
    """
    behaviors_combined_data = spark.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")

    support_repeat_customers_vw = spark.read.parquet(data_paths['support_repeat_customers_vw'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    support_repeat_customers_vw.createOrReplaceTempView("support_repeat_customers_vw")

    support_los_diamond_vw = sqlContext.sql(queries.los_dia_view_query.format(cntry_list=','.join(str(x) for x in run['cntry_key_no'])))
    support_los_diamond_vw.createOrReplaceTempView("support_los_diamond_vw")
    support_los_diamond = sqlContext.sql(queries.los_dia_table_query)
    support_los_diamond.write.parquet(data_paths['support_los_diamond'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def create_abo_dna_downline_indiv_vw(pargs, params):
    """
    :inputs: behaviors_combined_data, support_repeat_customers_vw
    :outputs: abo_dna_downline_indiv_vw
    """
    behaviors_combined_data = spark.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")

    support_repeat_customers_vw = spark.read.parquet(data_paths['support_repeat_customers_vw'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    support_repeat_customers_vw.createOrReplaceTempView("support_repeat_customers_vw")

    abo_dna_downline_indiv_vw = sqlContext.sql(queries.abo_dna_downline_indiv_view_query.format(cntry_list=','.join(str(x) for x in run['cntry_key_no'])))
    abo_dna_downline_indiv_vw.write.parquet(data_paths['abo_dna_downline_indiv_vw'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def create_percentile_rank(pargs, params):
    """
    :inputs: behaviors_combined_data
    :outputs: percentile_rank
    """
    behaviors_combined_data = spark.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")

    percentile_rank = sqlContext.sql(queries.percentile_rank_query.format(cntry_list=','.join(str(x) for x in run['cntry_key_no'])))
    percentile_rank.write.parquet(data_paths['percentile_rank'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def create_average_rank(pargs, params):
    """
    :inputs: behaviors_combined_data
    :outputs: average_rank
    """
    behaviors_combined_data = spark.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")

    average_rank = sqlContext.sql(queries.average_rank_query.format(cntry_list=','.join(str(x) for x in run['cntry_key_no'])))
    average_rank.write.parquet(data_paths['average_rank'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def create_average_rank_region(pargs, params):
    """
    :inputs: behaviors_combined_data
    :outputs: average_rank_region
    """
    behaviors_combined_data = spark.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")

    average_rank_region = sqlContext.sql(queries.average_rank_region_query.format(cntry_list=','.join(str(x) for x in run['cntry_key_no'])))
    average_rank_region.write.parquet(data_paths['average_rank_region'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def create_average_rank_awd_rnk(pargs, params):
    """
    :inputs: behaviors_combined_data
    :outputs: average_rank_awd_rnk
    """
    behaviors_combined_data = spark.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")

    average_rank_awd_rnk = sqlContext.sql(queries.average_rank_awd_rnk_query.format(cntry_list=','.join(str(x) for x in run['cntry_key_no'])))
    average_rank_awd_rnk.write.parquet(data_paths['average_rank_awd_rnk'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def create_classroom_feature(pargs, params):
    """
    :inputs: download_df, browse_df, share_df, search_df, fav_df
    :outputs: classroom_data
    """

    download_df = spark.read.parquet(data_paths['download_df'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    browse_df = spark.read.parquet(data_paths['browse_df'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    share_df = spark.read.parquet(data_paths['share_df'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    search_df = spark.read.parquet(data_paths['search_df'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    fav_df = spark.read.parquet(data_paths['fav_df'].format(run_mode=run['run_mode'], run_id=run['run_id']))

    fav_df2 = fav_df.withColumn("date", to_date(fav_df.CRTIME, 'yyyy/MM/dd HH:mm:ss'))
    fav_df2 = fav_df2.withColumn("MONTH_tmp", F.from_unixtime(F.unix_timestamp(fav_df2.date, "yyyyMM")))
    fav_df2 = fav_df2.withColumn("MONTH", F.concat(expr("substring(MONTH_tmp, 1, 4)"), expr("substring(MONTH_tmp, 6, 2)")))
    fav_df3 = fav_df2.withColumn("ADJ_USERID", expr("substring(USERNAME, 1, length(USERNAME)-2)"))
    fav_df3 = fav_df3.withColumn("ADJ_USERID", expr("substring(ADJ_USERID, 4, length(ADJ_USERID))"))
    fav = fav_df3.withColumn("ADJ_USERID", regexp_replace(F.col("ADJ_USERID"), "^0*", ""))
    fav = fav.groupby(['ADJ_USERID', 'MONTH']).count()
    fav = fav.withColumnRenamed("count", "num_fav")

    download_df2 = download_df.withColumn("date", to_date(download_df.CRTIME, 'yyyy/MM/dd HH:mm:ss'))
    download_df2 = download_df2.withColumn("MONTH_tmp", F.from_unixtime(F.unix_timestamp(download_df2.date, "yyyyMM")))
    download_df2 = download_df2.withColumn("MONTH", F.concat(expr("substring(MONTH_tmp, 1, 4)"), expr("substring(MONTH_tmp, 6, 2)")))
    download_df3 = download_df2.withColumn("ADJ_USERID", expr("substring(USERID, 1, length(USERID)-2)"))
    download_df4 = download_df3.withColumn("ADJ_USERID", regexp_replace(F.col("ADJ_USERID"), "^0*", ""))
    download = download_df4.groupby(['ADJ_USERID', 'MONTH']).count()
    download = download.withColumnRenamed("count", "num_" + "download")

    browse_df2 = browse_df.withColumn("date", to_date(browse_df.CRTIME, 'yyyy/MM/dd HH:mm:ss'))
    browse_df2 = browse_df2.withColumn("MONTH_tmp", F.from_unixtime(F.unix_timestamp(browse_df2.date, "yyyyMM")))
    browse_df2 = browse_df2.withColumn("MONTH", F.concat(expr("substring(MONTH_tmp, 1, 4)"), expr("substring(MONTH_tmp, 6, 2)")))
    browse_df3 = browse_df2.withColumn("ADJ_USERID", expr("substring(USERID, 1, length(USERID)-2)"))
    browse_df4 = browse_df3.withColumn("ADJ_USERID", regexp_replace(F.col("ADJ_USERID"), "^0*", ""))
    browse = browse_df4.groupby(['ADJ_USERID', 'MONTH']).count()
    browse = browse.withColumnRenamed("count", "num_" + "browse")

    share_df2 = share_df.withColumn("date", to_date(share_df.CRTIME, 'yyyy/MM/dd HH:mm:ss'))
    share_df2 = share_df2.withColumn("MONTH_tmp", F.from_unixtime(F.unix_timestamp(share_df2.date, "yyyyMM")))
    share_df2 = share_df2.withColumn("MONTH", F.concat(expr("substring(MONTH_tmp, 1, 4)"), expr("substring(MONTH_tmp, 6, 2)")))
    share_df3 = share_df2.withColumn("ADJ_USERID", expr("substring(USERID, 1, length(USERID)-2)"))
    share_df4 = share_df3.withColumn("ADJ_USERID", regexp_replace(F.col("ADJ_USERID"), "^0*", ""))
    share = share_df4.groupby(['ADJ_USERID', 'MONTH']).count()
    share = share.withColumnRenamed("count", "num_" + "share")

    search_df2 = search_df.withColumn("date", to_date(search_df.CRTIME, 'yyyy/MM/dd HH:mm:ss'))
    search_df2 = search_df2.withColumn("MONTH_tmp", F.from_unixtime(F.unix_timestamp(search_df2.date, "yyyyMM")))
    search_df2 = search_df2.withColumn("MONTH", F.concat(expr("substring(MONTH_tmp, 1, 4)"), expr("substring(MONTH_tmp, 6, 2)")))
    search_df3 = search_df2.withColumn("ADJ_USERID", expr("substring(USERID, 1, length(USERID)-2)"))
    search_df4 = search_df3.withColumn("ADJ_USERID", regexp_replace(F.col("ADJ_USERID"), "^0*", ""))
    search = search_df4.groupby(['ADJ_USERID', 'MONTH']).count()
    search = search.withColumnRenamed("count", "num_" + "search")

    data = [("2013-01-01", str(datetime.date.today()))]

    df = spark.createDataFrame(data, ["minDate", "maxDate"])

    df = df.withColumn("monthsDiff", F.months_between("maxDate", "minDate")) \
        .withColumn("repeat", F.expr("split(repeat(',', monthsDiff), ',')")) \
        .select("*", F.posexplode("repeat").alias("date", "val")) \
        .withColumn("date", F.expr("add_months(minDate, date)")) \
        .select('date')
    df = df.withColumn("MONTH", F.from_unixtime(F.unix_timestamp(F.col("date")), "yyyyMM")).select('MONTH')
    unique_id = download.select('ADJ_USERID').distinct() \
        .union(browse.select('ADJ_USERID').distinct()) \
        .union(share.select('ADJ_USERID').distinct()) \
        .union(search.select('ADJ_USERID').distinct()) \
        .union(fav.select('ADJ_USERID').distinct())
    unique_id = unique_id.distinct()
    all_abo_month = unique_id.crossJoin(df)
    combine = download.select(['ADJ_USERID', 'MONTH']).union(browse.select(['ADJ_USERID', 'MONTH'])) \
        .union(share.select(['ADJ_USERID', 'MONTH'])) \
        .union(search.select(['ADJ_USERID', 'MONTH'])) \
        .union(fav.select(['ADJ_USERID', 'MONTH']))
    min_max_date = combine.groupby("ADJ_USERID").agg(F.min("MONTH"), F.max("MONTH"))
    all_abo_month = all_abo_month.join(min_max_date, all_abo_month.ADJ_USERID == min_max_date.ADJ_USERID, how='left').drop(min_max_date.ADJ_USERID)
    all_abo_month = all_abo_month.filter(F.col("MONTH") >= F.col("min(MONTH)"))
    all_abo_month = all_abo_month.filter(F.col("MONTH") <= F.col("max(MONTH)"))

    all_abo_month = all_abo_month.select(["ADJ_USERID", "MONTH"])

    download = all_abo_month.join(download, ['ADJ_USERID', 'MONTH'], 'left').na.fill(0)
    for n in range(1, 12):
        download = download.withColumn('num_' + "download" + str(n), F.lag(download['num_' + "download"], n, 0) \
                                       .over(Window.partitionBy("ADJ_USERID").orderBy("MONTH")))
    download = download.withColumn("n_lag_currentyr_" + "download" + "_sum_3m",
                                   download['num_' + "download"] + download['num_' + "download" + "1"] + download['num_' + "download" + "2"])
    download = download.withColumn("n_lag_currentyr_" + "download" + "_sum_6m",
                                   download["n_lag_currentyr_" + "download" + "_sum_3m"] + download['num_' + "download" + "3"] + download['num_' + "download" + "4"] + download[
                                       'num_' + "download" + "5"])
    download = download.withColumn("n_lag_currentyr_" + "download" + "_sum_9m",
                                   download["n_lag_currentyr_" + "download" + "_sum_6m"] + download['num_' + "download" + "6"] + download['num_' + "download" + "7"] + download[
                                       'num_' + "download" + "8"])
    download = download.withColumn("n_lag_currentyr_" + "download" + "_sum_12m",
                                   download["n_lag_currentyr_" + "download" + "_sum_9m"] + download['num_' + "download" + "9"] + download['num_' + "download" + "10"] + download[
                                       'num_' + "download" + "11"])
    droplist = []
    for n in range(1, 12):
        droplist = droplist + ['num_' + "download" + str(n)]
    download = download.drop(*droplist)

    browse = all_abo_month.join(browse, ['ADJ_USERID', 'MONTH'], 'left').na.fill(0)
    for n in range(1, 12):
        browse = browse.withColumn('num_' + "browse" + str(n), F.lag(browse['num_' + "browse"], n, 0) \
                                   .over(Window.partitionBy("ADJ_USERID").orderBy("MONTH")))
    browse = browse.withColumn("n_lag_currentyr_" + "browse" + "_sum_3m", browse['num_' + "browse"] + browse['num_' + "browse" + "1"] + browse['num_' + "browse" + "2"])
    browse = browse.withColumn("n_lag_currentyr_" + "browse" + "_sum_6m",
                               browse["n_lag_currentyr_" + "browse" + "_sum_3m"] + browse['num_' + "browse" + "3"] + browse['num_' + "browse" + "4"] + browse[
                                   'num_' + "browse" + "5"])
    browse = browse.withColumn("n_lag_currentyr_" + "browse" + "_sum_9m",
                               browse["n_lag_currentyr_" + "browse" + "_sum_6m"] + browse['num_' + "browse" + "6"] + browse['num_' + "browse" + "7"] + browse[
                                   'num_' + "browse" + "8"])
    browse = browse.withColumn("n_lag_currentyr_" + "browse" + "_sum_12m",
                               browse["n_lag_currentyr_" + "browse" + "_sum_9m"] + browse['num_' + "browse" + "9"] + browse['num_' + "browse" + "10"] + browse[
                                   'num_' + "browse" + "11"])
    droplist = []
    for n in range(1, 12):
        droplist = droplist + ['num_' + "browse" + str(n)]
    browse = browse.drop(*droplist)

    share = all_abo_month.join(share, ['ADJ_USERID', 'MONTH'], 'left').na.fill(0)
    for n in range(1, 12):
        share = share.withColumn('num_' + "share" + str(n), F.lag(share['num_' + "share"], n, 0) \
                                 .over(Window.partitionBy("ADJ_USERID").orderBy("MONTH")))
    share = share.withColumn("n_lag_currentyr_" + "share" + "_sum_3m", share['num_' + "share"] + share['num_' + "share" + "1"] + share['num_' + "share" + "2"])
    share = share.withColumn("n_lag_currentyr_" + "share" + "_sum_6m",
                             share["n_lag_currentyr_" + "share" + "_sum_3m"] + share['num_' + "share" + "3"] + share['num_' + "share" + "4"] + share['num_' + "share" + "5"])
    share = share.withColumn("n_lag_currentyr_" + "share" + "_sum_9m",
                             share["n_lag_currentyr_" + "share" + "_sum_6m"] + share['num_' + "share" + "6"] + share['num_' + "share" + "7"] + share['num_' + "share" + "8"])
    share = share.withColumn("n_lag_currentyr_" + "share" + "_sum_12m",
                             share["n_lag_currentyr_" + "share" + "_sum_9m"] + share['num_' + "share" + "9"] + share['num_' + "share" + "10"] + share['num_' + "share" + "11"])
    droplist = []
    for n in range(1, 12):
        droplist = droplist + ['num_' + "share" + str(n)]
    share = share.drop(*droplist)

    search = all_abo_month.join(search, ['ADJ_USERID', 'MONTH'], 'left').na.fill(0)
    for n in range(1, 12):
        search = search.withColumn('num_' + "search" + str(n), F.lag(search['num_' + "search"], n, 0) \
                                   .over(Window.partitionBy("ADJ_USERID").orderBy("MONTH")))
    search = search.withColumn("n_lag_currentyr_" + "search" + "_sum_3m", search['num_' + "search"] + search['num_' + "search" + "1"] + search['num_' + "search" + "2"])
    search = search.withColumn("n_lag_currentyr_" + "search" + "_sum_6m",
                               search["n_lag_currentyr_" + "search" + "_sum_3m"] + search['num_' + "search" + "3"] + search['num_' + "search" + "4"] + search[
                                   'num_' + "search" + "5"])
    search = search.withColumn("n_lag_currentyr_" + "search" + "_sum_9m",
                               search["n_lag_currentyr_" + "search" + "_sum_6m"] + search['num_' + "search" + "6"] + search['num_' + "search" + "7"] + search[
                                   'num_' + "search" + "8"])
    search = search.withColumn("n_lag_currentyr_" + "search" + "_sum_12m",
                               search["n_lag_currentyr_" + "search" + "_sum_9m"] + search['num_' + "search" + "9"] + search['num_' + "search" + "10"] + search[
                                   'num_' + "search" + "11"])
    droplist = []
    for n in range(1, 12):
        droplist = droplist + ['num_' + "search" + str(n)]
    search = search.drop(*droplist)

    fav = all_abo_month.join(fav, ['ADJ_USERID', 'MONTH'], 'left').na.fill(0)
    for n in range(1, 12):
        fav = fav.withColumn('num_' + "fav" + str(n), F.lag(fav['num_' + "fav"], n, 0) \
                             .over(Window.partitionBy("ADJ_USERID").orderBy("MONTH")))
    fav = fav.withColumn("n_lag_currentyr_" + "fav" + "_sum_3m", fav['num_' + "fav"] + fav['num_' + "fav" + "1"] + fav['num_' + "fav" + "2"])
    fav = fav.withColumn("n_lag_currentyr_" + "fav" + "_sum_6m",
                         fav["n_lag_currentyr_" + "fav" + "_sum_3m"] + fav['num_' + "fav" + "3"] + fav['num_' + "fav" + "4"] + fav['num_' + "fav" + "5"])
    fav = fav.withColumn("n_lag_currentyr_" + "fav" + "_sum_9m",
                         fav["n_lag_currentyr_" + "fav" + "_sum_6m"] + fav['num_' + "fav" + "6"] + fav['num_' + "fav" + "7"] + fav['num_' + "fav" + "8"])
    fav = fav.withColumn("n_lag_currentyr_" + "fav" + "_sum_12m",
                         fav["n_lag_currentyr_" + "fav" + "_sum_9m"] + fav['num_' + "fav" + "9"] + fav['num_' + "fav" + "10"] + fav['num_' + "fav" + "11"])
    droplist = []
    for n in range(1, 12):
        droplist = droplist + ['num_' + "fav" + str(n)]
    fav = fav.drop(*droplist)

    classroom_data = all_abo_month.join(download, ['ADJ_USERID', 'MONTH'], 'left').join(browse, ['ADJ_USERID', 'MONTH'], 'left').join(share, ['ADJ_USERID', 'MONTH'], 'left').join(
        search, ['ADJ_USERID', 'MONTH'], 'left').join(fav, ['ADJ_USERID', 'MONTH'], 'left').na.fill(0)
    classroom_data = classroom_data.withColumnRenamed("ADJ_USERID", "imc_no")
    classroom_data = classroom_data.withColumnRenamed("MONTH", "mo_yr_key_no")

    df = classroom_data
    df = df.withColumn('mo_yr_key_no', df.mo_yr_key_no.cast('string'))
    df = df.withColumn('mo_yr_key_no', to_timestamp(df.mo_yr_key_no, 'yyyyMM'))
    df = df.withColumn('mo_yr_key_no', date_format('mo_yr_key_no', 'yyyy-MM-dd'))
    classroom_data = df

    print("now saving the data")
    classroom_data.write.parquet(data_paths['classroom_data'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def create_amwayhub_login(pargs, params):
    """
    :inputs: amwayhub_login_input
    :outputs: amwayhub_login
    """
    df = spark.read.csv(data_paths['amwayhub_login_input'], header=True, inferSchema=True)
    amway_hub = df.select(F.col("SHIJ").alias("mo_yr_key_no"), F.col("ADA").alias("imc_no"), F.col("CNT").alias("n_num_amwayhub_login"))

    amway_hub = amway_hub.withColumn('mo_yr_key_no', amway_hub.mo_yr_key_no.cast('string'))
    amway_hub = amway_hub.withColumn('mo_yr_key_no', F.to_timestamp(amway_hub.mo_yr_key_no, 'yyyyMM'))\
        .withColumn('mo_yr_key_no', F.date_format('mo_yr_key_no', 'yyyy-MM-dd'))

    amway_hub.write.parquet(data_paths['amwayhub_login'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def wechat_cloudcommerce(pargs, params):
    """
    wechat cloud commerce (yungou) feature engineering including search,browse,order placement, product purchased
    :inputs: wechat_miniprogram_input
    :outputs: wechat_cloudcommerce
    """

    wechat_mini = spark.read.option("delimiter", "\t").option("header", "true").option("encoding", "UTF-8").csv(data_paths['wechat_miniprogram_input'])

    wechat_mini = wechat_mini.withColumn('time', to_timestamp('时间戳', 'yyyy-MM-dd'))
    wechat_mini = wechat_mini.withColumn('month', to_timestamp('时间戳', 'yyyy-MM'))
    wechat_mini = wechat_mini.withColumn('month', date_format('month', 'yyyyMM'))

    wechat_mini2 = wechat_mini.withColumnRenamed('事件类型', 'event_type') \
        .withColumnRenamed('时间戳', 'timestamp') \
        .withColumnRenamed('诸葛id', 'trip_id') \
        .withColumnRenamed('事件id', 'event_id') \
        .withColumnRenamed('事件名', 'event_name') \
        .withColumnRenamed('商品id', 'product_id') \
        .withColumnRenamed('商品名称', 'product_name') \
        .withColumnRenamed('搜索词', 'search_word')

    # clean up imc_no
    wechat_mini3 = wechat_mini2.withColumn("leading360", expr("substring(amwayid, 1, 3)"))
    wechat_mini4 = wechat_mini3.withColumn("ADJ_USERID", when(F.col("leading360") == "360", expr("substring(amwayid, 4, length(amwayid)-2)")).otherwise(F.col("amwayid")))
    wechat_mini5 = wechat_mini4.withColumn("imc_no", regexp_replace(F.col("ADJ_USERID"), "^0*", ""))
    wechat_mini_all = wechat_mini5.withColumn("imc_no", when(F.col("leading360") == "360", expr("substring(imc_no, 1, length(imc_no)-2)")).otherwise(F.col("imc_no")))

    # browse
    wechat_mini_browse = wechat_mini_all.where((F.col("event_type") == '页面浏览'))
    wechat_mini_browse2 = wechat_mini_browse.groupBy('imc_no', 'month').agg(F.count("event_id").alias("n_num_cloudcommerce_browse"))

    # search
    wechat_mini_search = wechat_mini_all.where((F.col("event_type") == '站内搜索'))
    wechat_mini_search2 = wechat_mini_search.groupBy('imc_no', 'month').agg(F.count("event_id").alias("n_num_cloudcommerce_search"))

    # order
    wechat_mini_order = wechat_mini_all.where((F.col("event_name") == '小程序_订单确认'))
    wechat_mini_order2 = wechat_mini_order.groupBy('imc_no', 'month').agg(F.count("event_id").alias("n_num_cloudcommerce_order"))

    # cart
    purchase_trip = wechat_mini_order.select('trip_id').distinct()
    wechat_mini_cart = wechat_mini_all.join(purchase_trip, 'trip_id', 'inner').where((F.col("event_type") == '商品加购'))
    wechat_mini_cart2 = wechat_mini_cart.groupBy('imc_no', 'month', 'trip_id').agg(F.count("product_id").alias("n_num_cloudcommerce_product_per_cart"))
    wechat_mini_cart3 = wechat_mini_cart2.groupBy('imc_no', 'month').agg(F.avg("n_num_cloudcommerce_product_per_cart").alias("n_num_cloudcommerce_product_per_cart"))

    # all abo and month combination
    unique_id = wechat_mini_all.select('imc_no').distinct()
    month = wechat_mini_all.select('month').distinct()
    all_abo_month = unique_id.crossJoin(month)
    min_max_date = wechat_mini_all.groupby("imc_no").agg(F.min("month"), F.max("month"))
    all_abo_month = all_abo_month.join(min_max_date, all_abo_month.imc_no == min_max_date.imc_no, how='left').drop(min_max_date.imc_no)
    all_abo_month = all_abo_month.filter(F.col("month") >= F.col("min(month)"))
    all_abo_month = all_abo_month.filter(F.col("month") <= F.col("max(month)"))

    # join everything together
    combine1 = all_abo_month.join(wechat_mini_browse2, ['imc_no', 'month'], 'left').na.fill(0)
    combine2 = combine1.join(wechat_mini_search2, ['imc_no', 'month'], 'left').na.fill(0)
    combine3 = combine2.join(wechat_mini_order2, ['imc_no', 'month'], 'left').na.fill(0)
    combine4 = combine3.join(wechat_mini_cart3, ['imc_no', 'month'], 'left').na.fill(0)

    # create lag features
    combine = combine4.withColumnRenamed("month", "mo_yr_key_no")
    feature_list = ['n_num_cloudcommerce_browse', 'n_num_cloudcommerce_search', 'n_num_cloudcommerce_order', 'n_num_cloudcommerce_product_per_cart']
    lag_features = configs["lag_features"]

    for feature in feature_list:
        for lag_mo in lag_features:
            for lag in range(0, lag_mo):
                colname = feature + "_" + str(lag)
                feature_col = feature + "_sum_" + str(lag_mo) + "m"
                combine = combine.withColumn(colname, F.lag(combine[feature], lag).over(Window.partitionBy("imc_no").orderBy("mo_yr_key_no")))
                if lag == 0:
                    combine = combine.withColumn(feature_col, combine[colname])
                else:
                    combine = combine.withColumn(feature_col, combine[feature_col] + combine[colname])

    main_col = ['imc_no', 'mo_yr_key_no']
    selected_feature = []
    for feature in feature_list:
        for lag_mo in lag_features:
            feature_col = feature + "_sum_" + str(lag_mo) + "m"
            selected_feature.append(feature_col)

    selected_feature = main_col + feature_list + selected_feature
    wechat_cloudcommerce = combine.select(selected_feature)

    wechat_formatting = wechat_cloudcommerce
    wechat_formatting = wechat_formatting.withColumn('mo_yr_key_no', wechat_formatting.mo_yr_key_no.cast('string'))
    # wechat_formatting = wechat_formatting.withColumn('imc_no',wechat_formatting.imc_no.cast('string'))
    wechat_formatting = wechat_formatting.withColumn('mo_yr_key_no', to_timestamp(wechat_formatting.mo_yr_key_no, 'yyyyMM'))
    wechat_formatting = wechat_formatting.withColumn('mo_yr_key_no', date_format('mo_yr_key_no', 'yyyy-MM-dd'))
    wechat_cloudcommerce = wechat_formatting

    wechat_cloudcommerce.write.parquet(data_paths['wechat_cloudcommerce'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def gar_pf_data_cleansing(pargs, params):
    """
    gar_pf include the countries, imc_no, year and  MB GAR Rank, extract the Rank number adn Rank Name
    :inputs: gar_pf
    :outputs: gar_pf_cleaned
    """
    gar_df = spark.read.parquet(data_paths['gar_pf'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    gar2 = gar_df.where(F.col("location").isin(configs["gar_data_regions"]))
    gar2 = gar2.withColumn("rank", regexp_replace(F.col("gar_rank"), " ", ""))
    df2 = gar2.select("imc_no", "i_perf_yr", F.split("rank", "-").alias("col2"))
    df_sizes = df2.select(F.size("col2").alias("col2"))
    df_max = df_sizes.agg(F.max("col2"))
    nb_columns = df_max.collect()[0][0]
    df_result = df2.select("imc_no", "i_perf_yr", *[df2["col2"][i] for i in range(nb_columns)])
    df_result = df_result.withColumnRenamed("col2[0]", "gar_rank_no") \
        .withColumnRenamed("col2[1]", "gar_rank_desc")

    gar_pf_cleaned = df_result

    gar_pf_cleaned.write.parquet(data_paths['gar_pf_cleaned'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')



if __name__ == "__main__":
    args = parser.parse_args()
    # exec(pargs.function + '(pargs, params)')
    if args.nextLambdaARN is None:
        exec(args.function + '(None, None)')
    else:
        exec("util.trigger_next_lambda('" + args.nextLambdaARN + "')")
