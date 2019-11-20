import datetime
import os
import json
import yaml
import sys
import re

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

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
import pipeline.queries.layer2_queries as queries

# Load configs
run = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/run.yml")
data_paths = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/data_paths.yml")
configs = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/layer2.yml")


def create_renewal_response_variable(pargs, params):
    """
    This function creates an target table reporting if the ABO renewed or not

    :inputs: behaviors_combined_data
    :outputs: tbl_year1_abo_pc_renewal
    """
    # Read in behaviors data
    behaviors_combined_data = sqlContext.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")

    # Generate final target table through Spark SQL
    tbl_abo_renewal_flag_step0 = spark.sql(queries.step0_query.format(cntry_list=','.join(str(x) for x in run['cntry_key_no'])))
    tbl_abo_renewal_flag_step0.createOrReplaceTempView("tbl_abo_renewal_flag_step0")

    tbl_abo_renewal_flag_step1 = spark.sql(queries.step1_query)
    tbl_abo_renewal_flag_step1.createOrReplaceTempView("tbl_abo_renewal_flag_step1")

    tbl_abo_renewal_flag_step2 = spark.sql(queries.step2_query)
    tbl_abo_renewal_flag_step2.createOrReplaceTempView("tbl_abo_renewal_flag_step2")

    tbl_abo_renewal_flag_step3 = spark.sql(queries.step3_query)
    tbl_abo_renewal_flag_step3.createOrReplaceTempView("tbl_abo_renewal_flag_step3")

    tbl_year1_abo_pc_renewal = spark.sql(queries.tbl_year1_abo_pc_renewal_query)

    # Write final table
    tbl_year1_abo_pc_renewal.write.mode('overwrite').parquet(data_paths['tbl_year1_abo_pc_renewal'].format(run_mode=run['run_mode'], run_id=run['run_id']))


def create_renewal_training_data_step1(pargs, params):
    """
    Create training data separately according to members' tenure months(1 to 15 months)

    :inputs:
    :outputs: renewal_training_tbl_1 to renewal_training_tbl_15
    """

    # Create last n month feature if not created yet in ABO_DNA_FULL
    months_need = configs['month_regeneration']
    schemas = None
    if months_need[0] == 1:
        months_need.pop(0)
        months_need.append(1)
    if months_need[0] == 1 and len(months_need) == 1:
        print("Function does not support month_regeneration for only first month")
        return

    # Create and write training table for each tenure level
    for i in months_need:
        join_query = create_temporary_table(i)
        if run['use_sample']:
            abo_dna_full = sqlContext.read.parquet(data_paths['abo_dna_sample'].format(run_mode=run['run_mode'], run_id=run['run_id']))
        else:
            abo_dna_full = sqlContext.read.parquet(data_paths['abo_dna_full_file'].format(run_mode=run['run_mode'], run_id=run['run_id']))
        abo_dna_full = abo_dna_full.filter(F.col('n_imc_months_after_signup') == i)

        if i not in [1, 3, 6, 9, 12]:
            query_last_features = "SELECT b.imc_key_no, b.mo_yr_key_no,\n"
            query_last_features = query_last_features + "\n" + create_last_n_month_feature(i) + "\n" + create_extra_last_feature(i)
            query_last_features = query_last_features[:-2] + join_query
            last_features_tbl = sqlContext.sql(query_last_features)
            abo_dna_full = abo_dna_full.join(last_features_tbl, ['imc_key_no', 'mo_yr_key_no'], 'left')
        classrm, wechat = create_extra_last_feature2([i])
        abo_dna_full = abo_dna_full.join(classrm, ['imc_no', 'mo_yr_key_no'], 'left')
        abo_dna_full = abo_dna_full.join(wechat, ['imc_no', 'mo_yr_key_no'], 'left')
        abo_dna_full.createOrReplaceTempView("abo_dna")
        script_agg = "SELECT *,\n"
        if i == 1:
            script_agg = "SELECT *\n"
        script_agg = script_agg + find_agg_feature(abo_dna_full.schema.names, i)[:-2] + "\nFROM abo_dna"
        abo_dna_full = sqlContext.sql(script_agg)
        spark.catalog.dropTempView("abo_dna")
        if i in [3, 6, 9, 12]:
            drop_features = ['n_abos_added_past_month_agg', 'n_upline_abos_added_month_agg', 'n_los_sil_customers_avg_month_agg',
                             'n_los_sil_customers_repeat_avg_month_agg', 'n_los_sil_q_mth_avg_month_agg', 'n_los_sil_head_frontln_q_mth_avg_month_agg',
                             'n_los_gld_customers_avg_month_agg',
                             'n_los_gld_customers_repeat_avg_month_agg', 'n_los_gld_q_mth_avg_month_agg', 'n_los_gld_head_frontln_q_mth_avg_month_agg',
                             'n_los_plat_customers_avg_month_agg', 'n_los_plat_customers_repeat_avg_month_agg', 'n_los_plat_q_mth_avg_month_agg',
                             'n_los_plat_head_frontln_q_mth_avg_month_agg',
                             'n_los_emd_customers_avg_month_agg', 'n_los_emd_customers_repeat_avg_month_agg', 'n_los_emd_q_mth_avg_month_agg',
                             'n_los_emd_head_frontln_q_mth_avg_month_agg',
                             'n_los_dia_customers_avg_month_agg', 'n_los_dia_customers_repeat_avg_month_agg', 'n_los_dia_q_mth_avg_month_agg',
                             'n_los_dia_head_frontln_q_mth_avg_month_agg']
        else:
            drop_features = ['N_PCT_REGION_DISP_INCOME_EARNED_month_agg']
        drop_features = drop_features + drop_lag_feature(abo_dna_full.schema.names)
        abo_dna_full = abo_dna_full.drop(*drop_features)
        if i == months_need[0]:
            schemas = abo_dna_full.schema.names
        if i == 1:
            agg_features_first_month = []
            for feature in schemas:
                if feature not in abo_dna_full.schema.names:
                    agg_features_first_month.append(feature)
            for feature in agg_features_first_month:
                abo_dna_full = abo_dna_full.withColumn(feature, F.lit(None).cast(StringType()))

        abo_dna_full.write.mode('overwrite').option("maxRecordsPerFile", configs["maxRecordsPerFile"]) \
            .parquet("s3://bcgds/pipeline/{run_mode}/{run_id}/renewal_training_tbl_{i}/".format(run_mode=run['run_mode'], run_id=run['run_id'], i=i))


def create_renewal_training_data_step2(pargs, params):
    """
    Union all training data and do some processes towards special columns

    :inputs: renewal_training_tbl_1 to renewal_training_tbl_15
    :outputs: yr1_renewal_v1
    """

    # Union samples in each tenure level
    abo_dna_renewal = sqlContext.read.parquet(
        "s3://bcgds/pipeline/{run_mode}/{run_id}/renewal_training_tbl_{i}/".format(run_mode=run['run_mode'], run_id=run['run_id'], i=configs['month_regeneration'][0]))

    for i in range(1, len(configs['month_regeneration'])):
        abo_dna_renewal = abo_dna_renewal.union(sqlContext.read.parquet(
            "s3://bcgds/pipeline/{run_mode}/{run_id}/renewal_training_tbl_{i}/".format(run_mode=run['run_mode'], run_id=run['run_id'], i=configs['month_regeneration'][i])))

    # Join with label
    tbl_year1_abo_pc_renewal = sqlContext.read.parquet(data_paths['tbl_year1_abo_pc_renewal'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    tbl_year1_abo_pc_renewal = tbl_year1_abo_pc_renewal.drop(*['DT_SIGNUP_MONTH', 'dt_signup_year', 'AMWAY_CNTRY_CD', 'I_M3_IMC_TYPE', 'I_YEAR1'])
    for i in tbl_year1_abo_pc_renewal.schema.names:
        tbl_year1_abo_pc_renewal = tbl_year1_abo_pc_renewal.withColumnRenamed(i, i.lower())

    y1renewal = abo_dna_renewal.join(tbl_year1_abo_pc_renewal, ['imc_key_no'], 'inner')

    # Final refine of columns
    normalized_names = find_normalized_feature(y1renewal.schema.names)

    for feature in normalized_names:
        y1renewal = y1renewal.withColumn(feature, y1renewal[feature] / y1renewal['n_imc_months_after_signup'])

    y1renewal = y1renewal.withColumnRenamed('gar_rank_no', 'i_gar_rank_no').withColumnRenamed('gar_rank_desc', 'i_gar_rank_desc') \
        .withColumn('n_signup_month', F.month(F.col('dt_signup')))

    # write final table
    y1renewal.repartition(200).write.mode('overwrite').option("maxRecordsPerFile", configs["maxRecordsPerFile"]).parquet(
        data_paths['yr1_renewal_v1'].format(run_mode=run['run_mode'], run_id=run['run_id']))


def create_renewal_training_data_step3(pargs, params):
    """
    Create final training table for model training. Create scoring table for prediction and MD dashboard

    :inputs: yr1_renewal_v1
    :outputs: yr1_renewal_train_raw, yr1_renewal_scoring_raw
    """

    # Read renewal table
    y1renewal = sqlContext.read.parquet(data_paths['yr1_renewal_v1'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    y1renewal = y1renewal.filter("i_globl_bus_stat_cd = 'ACTIVE'").filter("i_globl_imc_type_cd = 'I'").drop(*['dt_signup_year', 'i_globl_bus_stat_cd', 'i_globl_imc_type_cd'])

    # Create training table
    training_tbl = y1renewal.filter("dt_signup < '" + str(configs['scoring_data_date_cutoff']) + "'")
    training_tbl = training_tbl.drop(*['dt_signup'])

    # Create scoring table
    scoring_tbl_step1 = y1renewal.filter("dt_signup >= '" + str(configs['scoring_data_date_cutoff']) + "'")
    win_find_recent = Window.partitionBy('imc_no').orderBy(F.col('mo_yr_key_no').desc())
    scoring_tbl_step2 = scoring_tbl_step1.withColumn('latest_date', F.first(F.col('mo_yr_key_no')).over(win_find_recent)).filter("latest_date = mo_yr_key_no")

    scoring_tbl_final = scoring_tbl_step2.drop(*['dt_signup', 'latest_date'])

    # Write final tables
    training_tbl.repartition(200).write.mode('overwrite').option("maxRecordsPerFile", configs["maxRecordsPerFile"]).parquet(
        data_paths['yr1_renewal_train_raw'].format(run_mode=run['run_mode'], run_id=run['run_id']))

    scoring_tbl_final.repartition(200).write.mode('overwrite').option("maxRecordsPerFile", configs["maxRecordsPerFile"]).parquet(
        data_paths['yr1_renewal_scoring_raw'].format(run_mode=run['run_mode'], run_id=run['run_id']))


def create_scoring_output(pargs, params):
    """
    Generate final renewal scoring CSV file for MD dashboard

    :inputs: renewal_scoring_output
    :outputs: md_prediction_1_firstyr_renewal
    """

    # Read and refine scoring output table
    model_result = spark.read.parquet(data_paths["renewal_scoring_output"].format(run_mode=run['run_mode'], run_id=run['run_id']))
    model_result = model_result.dropDuplicates(['imc_no'])
    output_tbl = model_result.groupBy(["n_signup_year", "n_signup_month"]).count().withColumnRenamed('count', 'num_population')

    # Create new fields for MD dashboard
    output_prob = model_result.groupBy(["n_signup_year", "n_signup_month"]).sum('renewal_label').withColumnRenamed('sum(renewal_label)', 'num_renew')
    output_tbl = output_tbl.join(output_prob, ["n_signup_year", "n_signup_month"])
    output_tbl = output_tbl.withColumn('renewal_pct', output_tbl['num_renew'] / output_tbl['num_population'])
    output_tbl = output_tbl.withColumn('signup_month', F.concat(F.col("n_signup_year"), F.lit(""), F.col("n_signup_month"))) \
        .withColumn('signup_month', F.to_date(F.col('signup_month'), 'yyyyMM')).drop(*["n_signup_year", "n_signup_month"]) \
        .withColumn('month', F.add_months('signup_month', 15))
    output_tbl = output_tbl.withColumn('signup_month', F.date_format(F.col("signup_month"), "yyyyMM")).withColumn('month', F.date_format(F.col("month"), "yyyyMM")).orderBy(
        'signup_month')

    # Write final table
    output_tbl.repartition(1).write.format("com.databricks.spark.csv").mode('overwrite') \
        .csv(data_paths['md_prediction_1_firstyr_renewal'].format(run_mode=run['run_mode'], run_id=run['run_id']), header='true')


# supportive function
def find_agg_feature(names, month_para):
    output = ""
    features_dic = {'n_pct_q_mth_Q1': 1, 'n_pct_q_mth_Q2': 1, 'n_pv_ratio': 1, 'n_pct_q_mth_qv': 1, 'n_pct_region_disp_income_earned': 1}
    change_dic = {'n_lag_currentyr_search_sum': 'n_lag_search_classrm_sum', 'n_lag_currentyr_share_sum': 'n_lag_share_classrm_sum'
        , 'n_lag_currentyr_browse_sum': 'n_lag_browse_classrm_sum', 'n_lag_currentyr_download_sum': 'n_lag_download_classrm_sum'
        , 'n_lag_currentyr_fav_sum': 'n_lag_fav_classrm_sum'}
    for iter in range(len(names)):
        i = names[iter]
        if re.search("_\d+?m$", i, re.IGNORECASE):
            find_month = re.search("_\d+?m$", i)
            month_alias = int(find_month.group(0)[1:-1])
            renamed_alias = i[:find_month.start()]
            if (renamed_alias in features_dic) or (int(month_alias) != month_para):
                continue
            else:
                features_dic[renamed_alias] = 1
                if renamed_alias in change_dic:
                    query = "{target} as {new_feature}" \
                        .format(target=i, new_feature=change_dic[renamed_alias] + "_month_agg")
                else:
                    query = "{target} as {new_feature}" \
                        .format(target=i, new_feature=renamed_alias + "_month_agg")
                output = output + query + ',\n'
    return output


def find_normalized_feature(names):
    output = []
    for iter in range(len(names)):
        i = names[iter]
        if re.search("_sum_\d+?m$", i, re.IGNORECASE) or re.search("_count_\d+?m$", i, re.IGNORECASE) or re.search("_nonzero_mths_\d+?m$", i, re.IGNORECASE):
            output.append(i)
    return output


def drop_lag_feature(names):
    output = []
    for iter in range(len(names)):
        i = names[iter]
        if re.search("_\d+?m$", i, re.IGNORECASE):
            output.append(i)
        elif re.search("\d+?m_mom$", i, re.IGNORECASE):
            output.append(i)
        elif re.search("\d+?m_lyr$", i, re.IGNORECASE):
            output.append(i)
    return output


def create_last_n_month_feature(month_para):
    if month_para in [1, 3, 6, 9, 12]:
        return ""
    script = queries.SQL_abo_dna_for_pyspark_v6
    script = script.split("\n")
    output = ""
    features_dic = {'n_pct_q_mth_Q1': 1, 'n_pct_q_mth_Q2': 1, 'n_pv_ratio': 1, 'n_pct_q_mth_qv': 1, 'n_pct_region_disp_income_earned': 1}
    for iter in range(len(script)):
        i = script[iter]
        target_feature, target_name = None, None
        if re.search("OVER.*PARTITION BY.*ORDER BY.*ROWS.*,", i):
            re_name = re.search("as.*,", i[re.search("OVER.*PARTITION BY.*ORDER BY.*ROWS.*\)", i).end() + 1:], re.IGNORECASE)
            renamed_alias_with_month = i[re.search("OVER.*PARTITION BY.*ORDER BY.*ROWS.*\)", i).end() + 1:][re_name.start() + 3:-1]
            re_name1 = re.search("\d+?m", renamed_alias_with_month, re.IGNORECASE)
            renamed_alias = renamed_alias_with_month[:re_name1.start() - 1]
            if renamed_alias in features_dic:
                continue
            else:
                features_dic[renamed_alias] = 1
            re_feature = re.search("OVER.*PARTITION BY.*ORDER BY.*ROWS.*,", i)
            target_feature = i[:re_feature.start()]
            query = "{target_feature} OVER (PARTITION BY b.imc_key_no ORDER BY b.mo_yr_key_no ROWS {month} PRECEDING) as {new_feature}" \
                .format(target_feature=target_feature, month=str(month_para - 1), new_feature=renamed_alias + "_" + str(month_para) + 'm')
            output = output + query + ',\n'
    return output


def create_extra_last_feature2(months):
    from pyspark.sql import functions as f
    from pyspark.sql.functions import date_format, to_date, to_timestamp
    import pyspark.sql.functions as func
    from pyspark.sql.functions import expr
    from pyspark.sql.window import Window
    download_df = spark.read.parquet(data_paths['download_df'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    browse_df = spark.read.parquet(data_paths['browse_df'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    share_df = spark.read.parquet(data_paths['share_df'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    search_df = spark.read.parquet(data_paths['search_df'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    fav_df = spark.read.parquet(data_paths['fav_df'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    fav_df2 = fav_df.withColumn("date", to_date(fav_df.CRTIME, 'yyyy/MM/dd HH:mm:ss'))
    fav_df2 = fav_df2.withColumn("MONTH_tmp", f.from_unixtime(f.unix_timestamp(fav_df2.date, "yyyyMM")))
    fav_df2 = fav_df2.withColumn("MONTH", f.concat(expr("substring(MONTH_tmp, 1, 4)"), expr("substring(MONTH_tmp, 6, 2)")))
    fav_df3 = fav_df2.withColumn("ADJ_USERID", expr("substring(USERNAME, 1, length(USERNAME)-2)"))
    fav_df3 = fav_df3.withColumn("ADJ_USERID", expr("substring(ADJ_USERID, 4, length(ADJ_USERID))"))
    fav = fav_df3.withColumn("ADJ_USERID", regexp_replace(f.col("ADJ_USERID"), "^0*", ""))
    fav = fav.groupby(['ADJ_USERID', 'MONTH']).count()
    fav = fav.withColumnRenamed("count", "num_fav")
    download_df2 = download_df.withColumn("date", to_date(download_df.CRTIME, 'yyyy/MM/dd HH:mm:ss'))
    download_df2 = download_df2.withColumn("MONTH_tmp", f.from_unixtime(f.unix_timestamp(download_df2.date, "yyyyMM")))
    download_df2 = download_df2.withColumn("MONTH", f.concat(expr("substring(MONTH_tmp, 1, 4)"), expr("substring(MONTH_tmp, 6, 2)")))
    download_df3 = download_df2.withColumn("ADJ_USERID", expr("substring(USERID, 1, length(USERID)-2)"))
    download_df4 = download_df3.withColumn("ADJ_USERID", regexp_replace(col("ADJ_USERID"), "^0*", ""))
    download = download_df4.groupby(['ADJ_USERID', 'MONTH']).count()
    download = download.withColumnRenamed("count", "num_" + "download")
    browse_df2 = browse_df.withColumn("date", to_date(browse_df.CRTIME, 'yyyy/MM/dd HH:mm:ss'))
    browse_df2 = browse_df2.withColumn("MONTH_tmp", f.from_unixtime(f.unix_timestamp(browse_df2.date, "yyyyMM")))
    browse_df2 = browse_df2.withColumn("MONTH", f.concat(expr("substring(MONTH_tmp, 1, 4)"), expr("substring(MONTH_tmp, 6, 2)")))
    browse_df3 = browse_df2.withColumn("ADJ_USERID", expr("substring(USERID, 1, length(USERID)-2)"))
    browse_df4 = browse_df3.withColumn("ADJ_USERID", regexp_replace(col("ADJ_USERID"), "^0*", ""))
    browse = browse_df4.groupby(['ADJ_USERID', 'MONTH']).count()
    browse = browse.withColumnRenamed("count", "num_" + "browse")
    share_df2 = share_df.withColumn("date", to_date(share_df.CRTIME, 'yyyy/MM/dd HH:mm:ss'))
    share_df2 = share_df2.withColumn("MONTH_tmp", f.from_unixtime(f.unix_timestamp(share_df2.date, "yyyyMM")))
    share_df2 = share_df2.withColumn("MONTH", f.concat(expr("substring(MONTH_tmp, 1, 4)"), expr("substring(MONTH_tmp, 6, 2)")))
    share_df3 = share_df2.withColumn("ADJ_USERID", expr("substring(USERID, 1, length(USERID)-2)"))
    share_df4 = share_df3.withColumn("ADJ_USERID", regexp_replace(col("ADJ_USERID"), "^0*", ""))
    share = share_df4.groupby(['ADJ_USERID', 'MONTH']).count()
    share = share.withColumnRenamed("count", "num_" + "share")
    search_df2 = search_df.withColumn("date", to_date(search_df.CRTIME, 'yyyy/MM/dd HH:mm:ss'))
    search_df2 = search_df2.withColumn("MONTH_tmp", f.from_unixtime(f.unix_timestamp(search_df2.date, "yyyyMM")))
    search_df2 = search_df2.withColumn("MONTH", f.concat(expr("substring(MONTH_tmp, 1, 4)"), expr("substring(MONTH_tmp, 6, 2)")))
    search_df3 = search_df2.withColumn("ADJ_USERID", expr("substring(USERID, 1, length(USERID)-2)"))
    search_df4 = search_df3.withColumn("ADJ_USERID", regexp_replace(col("ADJ_USERID"), "^0*", ""))
    search = search_df4.groupby(['ADJ_USERID', 'MONTH']).count()
    search = search.withColumnRenamed("count", "num_" + "search")
    data = [("2013-01-01", str(datetime.date.today()))]
    df = spark.createDataFrame(data, ["minDate", "maxDate"])
    df = df.withColumn("monthsDiff", f.months_between("maxDate", "minDate")) \
        .withColumn("repeat", f.expr("split(repeat(',', monthsDiff), ',')")) \
        .select("*", f.posexplode("repeat").alias("date", "val")) \
        .withColumn("date", f.expr("add_months(minDate, date)")) \
        .select('date')
    df = df.withColumn("MONTH", f.from_unixtime(f.unix_timestamp(f.col("date")), "yyyyMM")).select('MONTH')
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
    min_max_date = combine.groupby("ADJ_USERID").agg(f.min("MONTH"), f.max("MONTH"))
    all_abo_month = all_abo_month.join(min_max_date, all_abo_month.ADJ_USERID == min_max_date.ADJ_USERID, how='left').drop(min_max_date.ADJ_USERID)
    all_abo_month = all_abo_month.filter(f.col("MONTH") >= f.col("min(MONTH)"))
    all_abo_month = all_abo_month.filter(f.col("MONTH") <= f.col("max(MONTH)"))
    all_abo_month = all_abo_month.select(["ADJ_USERID", "MONTH"])
    download = all_abo_month.join(download, ['ADJ_USERID', 'MONTH'], 'left').na.fill(0)
    for n in range(1, n_max(12, n_max1(months))):
        download = download.withColumn('num_' + "download" + str(n), func.lag(download['num_' + "download"], n, 0) \
                                       .over(Window.partitionBy("ADJ_USERID").orderBy("MONTH")))
    for i in months:
        if i in [1, 3, 6, 9, 12]:
            continue
        download = download.withColumn("n_lag_currentyr_" + "download" + "_sum_{month}m".format(month=i)
                                       , download['num_' + "download"])
        for iter in range(1, n_max(12, n_max1(months))):
            download = download.withColumn("n_lag_currentyr_" + "download" + "_sum_{month}m".format(month=i)
                                           , download["n_lag_currentyr_" + "download" + "_sum_{month}m".format(month=i)]
                                           + download['num_' + "download" + str(iter)])
    droplist = []
    for n in range(1, n_max(12, n_max1(months))):
        droplist = droplist + ['num_' + "download" + str(n)]
    download = download.drop(*droplist)
    browse = all_abo_month.join(browse, ['ADJ_USERID', 'MONTH'], 'left').na.fill(0)
    for n in range(1, n_max(12, n_max1(months))):
        browse = browse.withColumn('num_' + "browse" + str(n), func.lag(browse['num_' + "browse"], n, 0) \
                                   .over(Window.partitionBy("ADJ_USERID").orderBy("MONTH")))
    for i in months:
        if i in [1, 3, 6, 9, 12]:
            continue
        browse = browse.withColumn("n_lag_currentyr_" + "browse" + "_sum_{month}m".format(month=i)
                                   , browse['num_' + "browse"])
        for iter in range(1, n_max(12, n_max1(months))):
            browse = browse.withColumn("n_lag_currentyr_" + "browse" + "_sum_{month}m".format(month=i)
                                       , browse["n_lag_currentyr_" + "browse" + "_sum_{month}m".format(month=i)]
                                       + browse['num_' + "browse" + str(iter)])
    droplist = []
    for n in range(1, n_max(12, n_max1(months))):
        droplist = droplist + ['num_' + "browse" + str(n)]
    browse = browse.drop(*droplist)
    share = all_abo_month.join(share, ['ADJ_USERID', 'MONTH'], 'left').na.fill(0)
    for n in range(1, n_max(12, n_max1(months))):
        share = share.withColumn('num_' + "share" + str(n), func.lag(share['num_' + "share"], n, 0) \
                                 .over(Window.partitionBy("ADJ_USERID").orderBy("MONTH")))
    for i in months:
        if i in [1, 3, 6, 9, 12]:
            continue
        share = share.withColumn("n_lag_currentyr_" + "share" + "_sum_{month}m".format(month=i)
                                 , share['num_' + "share"])
        for iter in range(1, n_max(12, n_max1(months))):
            share = share.withColumn("n_lag_currentyr_" + "share" + "_sum_{month}m".format(month=i)
                                     , share["n_lag_currentyr_" + "share" + "_sum_{month}m".format(month=i)]
                                     + share['num_' + "share" + str(iter)])
    droplist = []
    for n in range(1, n_max(12, n_max1(months))):
        droplist = droplist + ['num_' + "share" + str(n)]
    share = share.drop(*droplist)
    search = all_abo_month.join(search, ['ADJ_USERID', 'MONTH'], 'left').na.fill(0)
    for n in range(1, n_max(12, n_max1(months))):
        search = search.withColumn('num_' + "search" + str(n), func.lag(search['num_' + "search"], n, 0) \
                                   .over(Window.partitionBy("ADJ_USERID").orderBy("MONTH")))
    for i in months:
        if i in [1, 3, 6, 9, 12]:
            continue
        search = search.withColumn("n_lag_currentyr_" + "search" + "_sum_{month}m".format(month=i)
                                   , search['num_' + "search"])
        for iter in range(1, n_max(12, n_max1(months))):
            search = search.withColumn("n_lag_currentyr_" + "search" + "_sum_{month}m".format(month=i)
                                       , search["n_lag_currentyr_" + "search" + "_sum_{month}m".format(month=i)]
                                       + search['num_' + "search" + str(iter)])
    droplist = []
    for n in range(1, n_max(12, n_max1(months))):
        droplist = droplist + ['num_' + "search" + str(n)]
    search = search.drop(*droplist)
    fav = all_abo_month.join(fav, ['ADJ_USERID', 'MONTH'], 'left').na.fill(0)
    for n in range(1, n_max(12, n_max1(months))):
        fav = fav.withColumn('num_' + "fav" + str(n), func.lag(fav['num_' + "fav"], n, 0) \
                             .over(Window.partitionBy("ADJ_USERID").orderBy("MONTH")))
    for i in months:
        if i in [1, 3, 6, 9, 12]:
            continue
        fav = fav.withColumn("n_lag_currentyr_" + "fav" + "_sum_{month}m".format(month=i)
                             , fav['num_' + "fav"])
        for iter in range(1, n_max(12, n_max1(months))):
            fav = fav.withColumn("n_lag_currentyr_" + "fav" + "_sum_{month}m".format(month=i)
                                 , fav["n_lag_currentyr_" + "fav" + "_sum_{month}m".format(month=i)]
                                 + fav['num_' + "fav" + str(iter)])
    droplist = []
    for n in range(1, n_max(12, n_max1(months))):
        droplist = droplist + ['num_' + "fav" + str(n)]
    fav = fav.drop(*droplist)
    classroom_data = all_abo_month.join(download, ['ADJ_USERID', 'MONTH'], 'left').join(browse, ['ADJ_USERID', 'MONTH'], 'left').join(share, ['ADJ_USERID', 'MONTH'], 'left').join(
        search, ['ADJ_USERID', 'MONTH'], 'left').join(fav, ['ADJ_USERID', 'MONTH'], 'left').na.fill(0)
    classroom_data = classroom_data.withColumnRenamed("ADJ_USERID", "imc_no")
    classroom_data = classroom_data.withColumnRenamed("MONTH", "mo_yr_key_no")
    classroom_data = classroom_data.withColumn('mo_yr_key_no', classroom_data.mo_yr_key_no.cast('string'))
    classroom_data = classroom_data.withColumn('mo_yr_key_no', to_timestamp(classroom_data.mo_yr_key_no, 'yyyyMM'))
    classroom_data = classroom_data.withColumn('mo_yr_key_no', date_format('mo_yr_key_no', 'yyyy-MM-dd'))
    classroom_data = classroom_data.drop(*['num_download', 'num_browse', 'num_share', 'num_search', 'num_fav'])
    from pyspark.sql.window import Window
    import pyspark.sql.functions as f
    # from pyspark.sql.functions import *
    from pyspark.sql.types import IntegerType
    from pyspark.sql.functions import date_format, to_date, to_timestamp
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
    wechat_mini4 = wechat_mini3.withColumn("ADJ_USERID", when(f.col("leading360") == "360", expr("substring(amwayid, 4, length(amwayid)-2)")).otherwise(f.col("amwayid")))
    wechat_mini5 = wechat_mini4.withColumn("imc_no", regexp_replace(col("ADJ_USERID"), "^0*", ""))
    wechat_mini_all = wechat_mini5.withColumn("imc_no", when(f.col("leading360") == "360", expr("substring(imc_no, 1, length(imc_no)-2)")).otherwise(f.col("imc_no")))
    # browse
    wechat_mini_browse = wechat_mini_all.where((f.col("event_type") == '页面浏览'))
    wechat_mini_browse2 = wechat_mini_browse.groupBy('imc_no', 'month').agg(f.count("event_id").alias("n_num_cloudcommerce_browse"))
    # search
    wechat_mini_search = wechat_mini_all.where((f.col("event_type") == '站内搜索'))
    wechat_mini_search2 = wechat_mini_search.groupBy('imc_no', 'month').agg(f.count("event_id").alias("n_num_cloudcommerce_search"))
    # order
    wechat_mini_order = wechat_mini_all.where((f.col("event_name") == '小程序_订单确认'))
    wechat_mini_order2 = wechat_mini_order.groupBy('imc_no', 'month').agg(f.count("event_id").alias("n_num_cloudcommerce_order"))
    # cart
    purchase_trip = wechat_mini_order.select('trip_id').distinct()
    wechat_mini_cart = wechat_mini_all.join(purchase_trip, 'trip_id', 'inner').where((f.col("event_type") == '商品加购'))
    wechat_mini_cart2 = wechat_mini_cart.groupBy('imc_no', 'month', 'trip_id').agg(f.count("product_id").alias("n_num_cloudcommerce_product_per_cart"))
    wechat_mini_cart3 = wechat_mini_cart2.groupBy('imc_no', 'month').agg(f.avg("n_num_cloudcommerce_product_per_cart").alias("n_num_cloudcommerce_product_per_cart"))
    # all abo and month combination
    unique_id = wechat_mini_all.select('imc_no').distinct()
    month = wechat_mini_all.select('month').distinct()
    all_abo_month = unique_id.crossJoin(month)
    min_max_date = wechat_mini_all.groupby("imc_no").agg(f.min("month"), f.max("month"))
    all_abo_month = all_abo_month.join(min_max_date, all_abo_month.imc_no == min_max_date.imc_no, how='left').drop(min_max_date.imc_no)
    all_abo_month = all_abo_month.filter(f.col("month") >= f.col("min(month)"))
    all_abo_month = all_abo_month.filter(f.col("month") <= f.col("max(month)"))
    # join everything together
    combine1 = all_abo_month.join(wechat_mini_browse2, ['imc_no', 'month'], 'left').na.fill(0)
    combine2 = combine1.join(wechat_mini_search2, ['imc_no', 'month'], 'left').na.fill(0)
    combine3 = combine2.join(wechat_mini_order2, ['imc_no', 'month'], 'left').na.fill(0)
    combine4 = combine3.join(wechat_mini_cart3, ['imc_no', 'month'], 'left').na.fill(0)
    # create lag features
    combine = combine4.withColumnRenamed("month", "mo_yr_key_no")
    feature_list = ['n_num_cloudcommerce_browse', 'n_num_cloudcommerce_search', 'n_num_cloudcommerce_order', 'n_num_cloudcommerce_product_per_cart']
    lag_features = months
    for feature in feature_list:
        for lag_mo in lag_features:
            if lag_mo in [1, 3, 6, 9, 12]:
                continue
            for lag in range(0, lag_mo):
                colname = feature + "_" + str(lag)
                feature_col = feature + "_sum_" + str(lag_mo) + "m"
                combine = combine.withColumn(colname, f.lag(combine[feature], lag).over(Window.partitionBy("imc_no").orderBy("mo_yr_key_no")))
                if lag == 0:
                    combine = combine.withColumn(feature_col, combine[colname])
                else:
                    combine = combine.withColumn(feature_col, combine[feature_col] + combine[colname])
    main_col = ['imc_no', 'mo_yr_key_no']
    selected_feature = []
    for feature in feature_list:
        for lag_mo in lag_features:
            if lag_mo in [1, 3, 6, 9, 12]:
                continue
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
    wechat_cloudcommerce = wechat_cloudcommerce.drop(
        *['n_num_cloudcommerce_browse', 'n_num_cloudcommerce_search', 'n_num_cloudcommerce_order', 'n_num_cloudcommerce_product_per_cart'])
    return classroom_data, wechat_cloudcommerce


def create_temporary_table(month):
    behaviors_combined_data = spark.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id'])) \
        .filter(F.col('imc_months_after_signup') == month)
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")
    china_promo_imc_order = spark.read.parquet(data_paths['china_promo_imc_order'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    china_promo_imc_order.createOrReplaceTempView("china_promo_imc_order")
    performance_year = spark.read.parquet(data_paths['performance_year'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    performance_year.createOrReplaceTempView("performance_year")
    support_promotions = spark.read.parquet(data_paths['support_promotions'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    support_promotions.createOrReplaceTempView("support_promotions")
    support_first_indicators = spark.read.parquet(data_paths['support_first_indicators'].format(run_mode=run['run_mode'], run_id=run['run_id']))
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
    # add gar_pf features
    gar_pf_cleaned = spark.read.parquet(data_paths['gar_pf_cleaned'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    gar_pf_cleaned.createOrReplaceTempView("gar_pf_cleaned")
    columns_used = configs['columns_used']
    behaviors_combined_slim = behaviors_combined_data.select(columns_used)
    behaviors_combined_slim.createOrReplaceTempView("behaviors_combined_slim")
    up_cols = configs['up_cols']
    behaviors_combined_up = behaviors_combined_data.select(up_cols)
    behaviors_combined_up.createOrReplaceTempView("behaviors_combined_up")
    output = """\nfrom behaviors_combined b
    left join percentile_rank pr on pr.imc_key_no = b.imc_key_no and pr.mo_yr_key_no = b.mo_yr_key_no
    left join average_rank ar on ar.mo_yr_key_no = b.mo_yr_key_no
    left join average_rank_region ar_region on ar_region.mo_yr_key_no = b.mo_yr_key_no and ar_region.reg_shop_prvnc_nm = b.reg_shop_prvnc_nm
    left join average_rank_awd_rnk ar_awd on ar_awd.mo_yr_key_no = b.mo_yr_key_no and ar_awd.cur_awd_awd_rnk_no = b.cur_awd_awd_rnk_no
    left join per_capita_disposable_income_v3 dispy on dispy.year = extract(year from b.mo_yr_key_no) and dispy.province_name_zh = b.reg_shop_prvnc_nm
    left join performance_year py on py.mo_yr_key_no = b.mo_yr_key_no
    left join support_first_indicators f ON b.imc_key_no = f.imc_key_no and b.mo_yr_key_no = f.mo_yr_key_no
    left join support_promotions pm on pm.imc_key_no = b.imc_key_no and pm.mo_yr_key_no = b.mo_yr_key_no
    left join abo_dna_downline_indiv_vw dn on dn.imc_key_no = b.imc_key_no and dn.mo_yr_key_no = b.mo_yr_key_no
    left join behaviors_combined_up up on up.imc_key_no = b.spon_imc_key_no and up.mo_yr_key_no = b.mo_yr_key_no
    left join abo_dna_downline_indiv_vw upg on upg.imc_key_no = b.spon_imc_key_no and upg.mo_yr_key_no = b.mo_yr_key_no
    left join support_LOS_silver sil on b.cur_sil_imc_no = sil.cur_sil_imc_no and b.mo_yr_key_no = sil.mo_yr_key_no
    left join support_LOS_gold gld on b.cur_gld_imc_no = gld.cur_gld_imc_no and b.mo_yr_key_no = gld.mo_yr_key_no
    left join support_LOS_plat plat on b.cur_plat_imc_no = plat.cur_plat_imc_no and b.mo_yr_key_no = plat.mo_yr_key_no
    left join support_los_emerald emd on b.cur_emd_imc_no = emd.cur_emd_imc_no and b.mo_yr_key_no = emd.mo_yr_key_no
    left join support_los_diamond dia on b.cur_dia_imc_no = dia.cur_dia_imc_no and b.mo_yr_key_no = dia.mo_yr_key_no
    left join china_promo_imc_order cpio on b.imc_key_no = cpio.ord_imc_key_no and b.mo_yr_key_no = cpio.mo_yr_key_no
    left join combined_yr_mo_awards awd on awd.imc_key_no = b.imc_key_no and awd.mo_yr_key_no = b.mo_yr_key_no
    left join amwayhub_login amh on amh.imc_no = b.imc_no and amh.mo_yr_key_no = b.mo_yr_key_no
    left join classroom_data classrm on classrm.imc_no = b.imc_no and classrm.mo_yr_key_no = b.mo_yr_key_no
    left join wechat_cloudcommerce cloudcom on cloudcom.imc_no = b.imc_no and cloudcom.mo_yr_key_no = b.mo_yr_key_no
    left join fc_dist fc on fc.imc_no = b.imc_no
    left join gar_pf_cleaned gar on gar.imc_no = b.imc_no and gar.i_perf_yr = py.perf_yr"""
    return output


def create_extra_last_feature(month_para):
    if month_para in [1, 3, 6, 9, 12]:
        return ""
    output_last = """
    nvl(SUM(b.bns_grs_pln_comm_curr_amt) OVER (PARTITION BY b.imc_key_no ORDER BY b.mo_yr_key_no ROWS {lag} PRECEDING) / dispy.disposable_income_yuan, 0) AS N_PCT_REGION_DISP_INCOME_EARNED_{month},\n"""
    return output_last.format(lag=str(month_para - 1), month=str(month_para) + 'm')


def create_extra_first_feature(month_para):
    if month_para in [1, 3, 6, 9, 12]:
        return ""
    output_first = "N_PCT_REGION_DISP_INCOME_EARNED_{month}M as N_PCT_REGION_DISP_INCOME_EARNED_{month}M_first,\n".format(month=str(month_para))
    return output_first


def n_max(a, b):
    return a if a >= b else b


def n_max1(a):
    res = a[0]
    for i in range(1, len(a)):
        res = n_max(a[i], res)
    return res


if __name__ == "__main__":
    args = parser.parse_args()
    # exec(pargs.function + '(pargs, params)')
    if args.nextLambdaARN is None:
        exec(args.function + '(None, None)')
    else:
        exec("util.trigger_next_lambda('" + args.nextLambdaARN + "')")
