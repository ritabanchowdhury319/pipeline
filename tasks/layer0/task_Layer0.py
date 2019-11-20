import datetime
import os
import json
import yaml
import sys
import argparse

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession


sys.path.append('/home/hadoop/eap-bcg/')

from pipeline.libraries import util
import pipeline.queries.layer0_queries as queries

# Set up environment
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("ERROR")

parser = argparse.ArgumentParser()
parser.add_argument('--function', '-f', help="Input function", type= str)
parser.add_argument('--nextLambdaARN', '-nla', help="Next Lambda ARN", type= str)

# Load configs
run = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/run.yml")
data_paths = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/data_paths.yml")
configs = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/layer0.yml")


def ingest_behaviors_data(pargs, params):
    """
    Reads in raw 2013-2019 Behaviors data files and saves to parquet

    :inputs: raw_behaviors_data, behavior_additions_data_June2019, behavior_additions_data_July2019, behavior_additions_data_August2019
    :outputs: behaviors_combined_data_full
    """

    # Read behaviors data
    latest_data_year = configs['last_year']

    # If the behaviors data hasn't been processed yet, process it:
    for year in range(configs['first_year'], latest_data_year + 1):

        if not util.poke_s3(data_paths['raw_behaviors_data'][:-4].format(year=year) + ".parquet", sc):

            raw_behaviors_data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "|").load(data_paths['raw_behaviors_data'].format(year=year))

            raw_behaviors_data.write.parquet(data_paths['raw_behaviors_data'][:-4].format(year=year) + ".parquet")

    behaviors_data = sqlContext.read.parquet(data_paths['raw_behaviors_data'][:-4].format(year="*") + ".parquet")
    behaviors_data = behaviors_data.withColumnRenamed("mo_yr_key_no", "mo_yr_key_no_str")

    # Read in and update the relatively small behavior additions data
    additional_data_schema = StructType([StructField("mo_yr_key_no_str", StringType(), True), StructField("imc_key_no", StringType(), True), StructField("cur_sil_imc_name", StringType(), True), StructField("cur_sil_imc_no", StringType(), True), StructField("cur_awd_awd_cd", StringType(), True), StructField("cur_awd_awd_desc", StringType(), True), StructField("distb_90dy_activ_cmplt_flg", StringType(), True), StructField("reg_with_prod_flg", StringType(), True), StructField("org_app_full_dt", DateType(), True), StructField("state_cd", StringType(), True), StructField("state_nm", StringType(), True), StructField("contb_distb_flg", StringType(), True), StructField("imc_gndr_cd", StringType(), True), StructField("imc_gndr_desc", StringType(), True), StructField("stly_seg_desc", StringType(), True), StructField("imc_ord_flg", StringType(), True), StructField("ctd_imc_key_no", StringType(), True), StructField("imc_slvr_prod_mo_flg", StringType(), True), StructField("imc_slvr_prod_qv_mo_flg", StringType(), True), StructField("imc_expire_flg", StringType(), True), StructField("imc_elg_renew_flg", StringType(), True), StructField("imc_term_early_flg", StringType(), True), StructField("imc_cntrc_renew_flg", StringType(), True), StructField("imc_1st_cntrc_flg", StringType(), True), StructField("imc_acct_tenure", IntegerType(), True)])

    behavior_additions_data = sqlContext.read.format("com.databricks.spark.csv").schema(additional_data_schema).option("delimiter", "|").load(data_paths['behavior_additions_data'])

    # Add additional behavior data to behavior data:
    behaviors_data.createOrReplaceTempView("behaviors")
    behavior_additions_data.createOrReplaceTempView("addt_behaviors")
    behaviors_combined_data = spark.sql(queries.combine_behaviors_query)
    behaviors_combined_data = behaviors_combined_data.withColumn('mo_yr_key_no',F.to_date(F.col('mo_yr_key_no_str'), 'yyyyMM'))

    # Adding new June/July/August/September 2019 data
    behavior_additions_data_June2019 = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").load(data_paths['behavior_additions_data_June2019'])
    behavior_additions_data_July2019 = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").load(data_paths['behavior_additions_data_July2019'])
    behavior_additions_data_August2019 = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").load(data_paths['behavior_additions_data_August2019'])
    behavior_additions_data_September2019 = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").load(data_paths['behavior_additions_data_September2019'])

    behaviors_combined_data = behaviors_combined_data.drop("mo_yr_key_no") \
        .unionAll(behavior_additions_data_June2019) \
        .unionAll(behavior_additions_data_July2019) \
        .unionAll(behavior_additions_data_August2019)
    behaviors_combined_data = behaviors_combined_data.withColumn('mo_yr_key_no',F.to_date(F.col('mo_yr_key_no_str'), 'yyyyMM'))

    behaviors_combined_data.write.mode('overwrite').parquet(data_paths['behaviors_combined_data_full'].format(run_mode=run['run_mode'], run_id=run['run_id']))


def ingest_external_data(pargs, params):
    """
    Reads in external data files and saves to parquet, including:
    - province mapping data
    - disposble income data

    :inputs: province_names_mapping_input, disposable_income_data_input
    :outputs: province_names_mapping, disposable_income_data
    """
    # Province mapping data
    province_mapping_schema = StructType([StructField("province_name_en", StringType(), True), StructField("province_name_zh", StringType(), True)])
    province_names_mapping = sqlContext.read.format("com.databricks.spark.csv").schema(province_mapping_schema).option("delimiter", ",").load(data_paths["province_names_mapping_input"])
    province_names_mapping.write.mode('overwrite').parquet(data_paths["province_names_mapping"].format(run_mode=run['run_mode'], run_id=run['run_id']))

    # China disposable income data
    disposable_income_schema = StructType([StructField("region", StringType(), True), StructField("disposable_income_yuan", StringType(), True)])
    disposable_income_data = sqlContext.read.format("com.databricks.spark.csv").schema(disposable_income_schema).option("delimiter", ",").load(data_paths["disposable_income_data_input"])
    disposable_income_data.write.mode('overwrite').parquet(data_paths["disposable_income_data"].format(run_mode=run['run_mode'], run_id=run['run_id']))

def ingest_classroom_data(pargs, params):
    """
    Reads in classroom data files and saves to parquet, including:
    - download
    - browse
    - share
    - search
    - fav

    :inputs: classroom_download_history classroom_browse_history classroom_sharing_history classroom_search_history classroom_saved_to_fav_history
    :outputs: download_df browse_df share_df search_df fav_df
    """
    download_df = spark.read.csv(data_paths["classroom_download_history"],header=True,inferSchema=True)
    browse_df = spark.read.csv(data_paths["classroom_browse_history"],header=True,inferSchema=True)
    share_df = spark.read.csv(data_paths["classroom_sharing_history"],header=True,inferSchema=True)
    search_df = spark.read.csv(data_paths["classroom_search_history"],header=True,inferSchema=True)
    fav_df = spark.read.csv(data_paths["classroom_saved_to_fav_history"],header=True,inferSchema=True)

    download_df.write.mode('overwrite').parquet(data_paths["download_df"].format(run_mode=run['run_mode'], run_id=run['run_id']))
    browse_df.write.mode('overwrite').parquet(data_paths["browse_df"].format(run_mode=run['run_mode'], run_id=run['run_id']))
    share_df.write.mode('overwrite').parquet(data_paths["share_df"].format(run_mode=run['run_mode'], run_id=run['run_id']))
    search_df.write.mode('overwrite').parquet(data_paths["search_df"].format(run_mode=run['run_mode'], run_id=run['run_id']))
    fav_df.write.mode('overwrite').parquet(data_paths["fav_df"].format(run_mode=run['run_mode'], run_id=run['run_id']))

def ingest_gar_data(pargs, params):
    """
    Reads in gar data files and saves to parquet, including:

    :inputs: gar_pf_input
    :outputs: gar_pf
    """

    gar = spark.read.csv(data_paths["gar_pf_input"],header=True,inferSchema=True)
    gar_df = gar.withColumnRenamed("Mb Imc No", "imc_no")\
                .withColumnRenamed("I_PF_YEAR","i_perf_yr")\
                .withColumnRenamed("Mb Cntry Short Nm","location")\
                .withColumnRenamed("MB GAR Rank","gar_rank")

    gar_df.write.mode('overwrite').parquet(data_paths["gar_pf"].format(run_mode=run['run_mode'], run_id=run['run_id']))

def ingest_fc_dist(pargs, params):
    """
    Reads in fc id data files and saves to parquet, including:

    :inputs:
    :outputs:
    """

    fc = spark.read.csv(data_paths["fc_dist_df"],header=True,inferSchema=True)
    fc_df = fc.withColumnRenamed("DIST_NUM", "imc_no")\
                .withColumnRenamed("FIRST_FC_DIST_NUM", "dist_num")
    fc_df.write.mode('overwrite').parquet(data_paths["fc_dist_cleaned"].format(run_mode=run['run_mode'], run_id=run['run_id']))

if __name__ == "__main__":
    args=parser.parse_args()
    #exec(pargs.function + '(pargs, params)')
    if args.nextLambdaARN is None:
        exec(args.function + '(None, None)')
    else:
        exec("util.trigger_next_lambda('" + args.nextLambdaARN + "')")
