import datetime
import os
import json
import yaml
import sys

from pyspark.sql import functions as F
from pyspark.sql.functions import lag
from pyspark.sql.functions import year, expr, col
from pyspark.sql.window import Window
from pyspark.sql.session import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext

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
import pipeline.queries.layer2_migration_queries as queries

# Load configs
run = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/run.yml")
data_paths = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/data_paths.yml")
configs = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/layer2.yml")


def create_migration_monthly_target_variables(pargs, params):
    """
    This function creates bns migration target variables table.
    Target variable: bns_migration_label, contains values (UP, SAME, DOWN)

    :inputs: behaviors_combined_data
    :outputs: bns_migration_3m_outcomes or bns_migration_3m_outcomes
    """

    # Load data
    behaviors_combined_data = sqlContext.read.parquet(data_paths['behaviors_combined_data'].format(run_mode=run['run_mode'], run_id=run['run_id']))

    # Create time realted columns and adjust columns type. Generate temporary table for SQL queries
    behaviors_combined_data = behaviors_combined_data.withColumn('mo_yr_key_no', F.to_date(F.col('mo_yr_key_no_str'), 'yyyyMM'))
    behaviors_combined_data = behaviors_combined_data.withColumn('calendar_year', year(F.col('mo_yr_key_no')))
    behaviors_combined_data = behaviors_combined_data.withColumn('imc_months_after_signup', behaviors_combined_data['imc_months_after_signup'].cast('int')) \
        .withColumn('bns_pct', behaviors_combined_data['bns_pct'].cast('int'))
    behaviors_combined_data.createOrReplaceTempView("behaviors_combined")

    # Set parameters for 3 months or 6 months tables
    if configs['target_3m']:
        monthPara = "3m"
        monthNum = "3"
        monthLag = '2'
    else:
        monthPara = "6m"
        monthNum = "6"
        monthLag = '5'

    # Processes of generating bns migration target table in Spark SQL
    bns_migration_eda_step_0 = sqlContext.sql(
        queries.bns_migration_eda_step0_sql.format(monthLag=monthLag, monthPara=monthPara, cntry_list=','.join(str(x) for x in run['cntry_key_no'])))
    bns_migration_eda_step_0 = bns_migration_eda_step_0.withColumn("mo_yr_key_no_" + monthPara + "_pre", F.add_months('mo_yr_key_no', -int(monthNum)))
    bns_migration_eda_step_0.createOrReplaceTempView("bns_migration_eda_step_0")

    bns_migration_eda_step_1 = sqlContext.sql(queries.bns_migration_eda_step1_sql.format(monthPara=monthPara))
    bns_migration_eda_step_1.createOrReplaceTempView("bns_migration_eda_step_1")

    bns_migration_monthly_outcomes = sqlContext.sql(queries.bns_migration_outcomes_sql.format(monthLag=monthLag, monthPara=monthPara))
    bns_migration_monthly_outcomes = bns_migration_monthly_outcomes.withColumn("original_back_" + monthNum, F.add_months('mo_yr_key_no', -int(monthNum)))

    # Write final table
    writePath = data_paths['bns_migration_{monthPara}_outcomes'.format(monthPara=monthPara)]
    bns_migration_monthly_outcomes.write.mode('overwrite').option("maxRecordsPerFile", configs["maxRecordsPerFile"]) \
        .parquet(writePath.format(run_mode=run['run_mode'], run_id=run['run_id']))


def create_bns_migration_training_table(pargs, params):
    """
    Create finial training tables for bonus migration model
    When joining ABO DNA and target variable data, there are many null values in 'max_bns_6m','max_bns_6m','bns_migration_label'. They are filled as 0, 0, and SAME for now.

    :inputs: abo_dna_full_file, bns_migration_3m_outcomes or bns_migration_6m_outcomes
    :outputs: bns_migration_model_max_bns_3m_positive + bns_migration_model_max_bns_3m_zero / bns_migration_model_max_bns_6m_positive + bns_migration_model_max_bns_6m_zero
    """

    # Set parameters for 3 months or 6 months tables
    if configs['target_3m']:
        monthPara = "3m"
    else:
        monthPara = "6m"

    # Read target table
    bns_migration_monthly_outcomes = sqlContext.read.parquet(
        data_paths['bns_migration_{monthPara}_outcomes'.format(monthPara=monthPara)].format(run_mode=run['run_mode'], run_id=run['run_id']))

    if run['use_sample']:
        abo_dna_full = sqlContext.read.parquet(data_paths['abo_dna_sample'].format(run_mode=run['run_mode'], run_id=run['run_id'])) \
            .sample(False, configs['sampling_fraction'], seed=configs['sampling_seed'])
    else:
        abo_dna_full = sqlContext.read.parquet(data_paths['abo_dna_full_file'].format(run_mode=run['run_mode'], run_id=run['run_id']))

    # Filter abo_dna table according to business rules
    abo_dna_full = abo_dna_full.filter("i_globl_bus_stat_cd = 'ACTIVE'").filter("i_globl_imc_type_cd = 'I'")

    # Generate temporary tables for Spark SQL processes
    abo_dna_full.createOrReplaceTempView("abo_dna_full_partial")
    bns_migration_monthly_outcomes.createOrReplaceTempView("bns_migration_monthly_outcomes_partial")

    bns_migration_final_sample = sqlContext.sql(queries.bns_migration_v1_final_sql.format(monthPara=monthPara))
    bns_migration_final_sample = bns_migration_final_sample.fillna(
        {'max_bns_{monthPara}'.format(monthPara=monthPara): 0, 'bns_migration_label_{monthPara}'.format(monthPara=monthPara): 'SAME'})

    # Create finial training tables according to maximum bonus in last 3/6 months
    bns_migration_model_data_max_bns_zero = bns_migration_final_sample.filter("max_bns_{monthPara} = 0".format(monthPara=monthPara))
    bns_migration_model_data_max_bns_positive = bns_migration_final_sample.filter("max_bns_{monthPara} > 0".format(monthPara=monthPara))

    # Write tables
    bns_migration_model_data_max_bns_zero.write.mode('overwrite').option("maxRecordsPerFile", configs["maxRecordsPerFile"]) \
        .parquet(data_paths['bns_migration_model_max_bns_{monthPara}_zero'.format(monthPara=monthPara)].format(run_mode=run['run_mode'], run_id=run['run_id']))

    bns_migration_model_data_max_bns_positive.write.mode('overwrite').option("maxRecordsPerFile", configs["maxRecordsPerFile"]) \
        .parquet(data_paths['bns_migration_model_max_bns_{monthPara}_positive'.format(monthPara=monthPara)].format(run_mode=run['run_mode'], run_id=run['run_id']))

    # Generate sampling CSV files
    if configs['outputCSV']:
        bns_migration_model_data_max_bns_zero.sample(False, 0.02, seed=configs['sampling_seed']).repartition(1) \
            .write.format("com.databricks.spark.csv").mode('overwrite') \
            .csv(data_paths['bns_migration_model_max_bns_{monthPara}_zero_sample_csv'.format(monthPara=monthPara)], header='true')

        bns_migration_model_data_max_bns_positive.sample(False, 0.1, seed=configs['sampling_seed']).repartition(1) \
            .write.format("com.databricks.spark.csv").mode('overwrite') \
            .csv(data_paths['bns_migration_model_max_bns_{monthPara}_positive_sample_csv'.format(monthPara=monthPara)], header='true')


def create_yearly_migration_training_label(pargs, params):
    """
    This function creates yearly migration label for members. Then it is used to join with full ABO DNA table for completed training table.
    Target variable: label_aug, label_nov, label_feb, with values 'INVALID', 'UP', 'SAME', 'DOWN'

    :inputs: abo_dna_full_file, pin_award_mapping
    :outputs: yrly_label_tbl
    """

    # Read files
    if run['use_sample']:
        abo_dna_selected = sqlContext.read.parquet(
            data_paths['abo_dna_sample'].format(run_mode=run['run_mode'], run_id=run['run_id'])).select(['mo_yr_key_no', 'imc_key_no', 'i_yrly_awd_rnk_no', 'gar_rank_no'])
    else:
        abo_dna_selected = sqlContext.read.parquet(
            data_paths['abo_dna_full_file'].format(run_mode=run['run_mode'], run_id=run['run_id'])).select(['mo_yr_key_no', 'imc_key_no', 'i_yrly_awd_rnk_no', 'gar_rank_no'])

    # Create mix score columns combined old and new system
    abo_dna_selected = abo_dna_selected.fillna({"i_yrly_awd_rnk_no": -1, "gar_rank_no": -1})
    abo_dna_selected = abo_dna_selected.withColumn("combined_awd_no", F.when(abo_dna_selected['gar_rank_no'] != -1, abo_dna_selected['gar_rank_no'])
                                                   .otherwise(abo_dna_selected['i_yrly_awd_rnk_no']))

    awd_tbl = spark.read.csv(data_paths['pin_award_mapping'].format(run_mode=run['run_mode'], run_id=run['run_id']), header=True)
    awd_tbl.rdd.map(lambda x: x[0]).collect()
    up, bottom, rnk = awd_tbl.rdd.map(lambda x: x[1]).collect(), awd_tbl.rdd.map(lambda x: x[0]).collect(), awd_tbl.rdd.map(lambda x: x[2]).collect()
    for i in range(len(up)):
        up[i], bottom[i], rnk[i] = int(up[i]), int(bottom[i]), int(rnk[i])

    abo_dna_selected = abo_dna_selected.withColumn('combined_awd_rnk', abo_dna_selected['combined_awd_no'])
    for i in range(len(rnk)):
        abo_dna_selected = abo_dna_selected.withColumn('combined_awd_rnk',
                                                       F.when((abo_dna_selected['combined_awd_no'] <= up[i]) & (abo_dna_selected['combined_awd_no'] >= bottom[i]), rnk[i])
                                                       .otherwise(abo_dna_selected['combined_awd_rnk']))

    # Cache the table for loop operation
    yr_mo_awards = abo_dna_selected.filter("combined_awd_rnk > 0")
    yr_mo_awards.cache().count()

    move = ["add_months(mo_yr_key_no, 4)", "add_months(mo_yr_key_no, 1)", "add_months(mo_yr_key_no, -2)"]
    name = ["_aug", "_nov", "_feb"]
    tableList = []

    # create latest date award information for Aug, Nov and Feb
    for i in range(len(name)):
        # find financial year
        yr_mo_awards1 = yr_mo_awards.withColumn("new_date", expr(move[i]))
        yr_mo_awards1 = yr_mo_awards1.withColumn('f_yr', year('new_date'))
        # find latest status each year
        winLatestEachYr = Window.partitionBy(['imc_key_no', 'f_yr']).orderBy(col('mo_yr_key_no').desc())
        latestEachYr = F.max('mo_yr_key_no').over(winLatestEachYr)
        yr_mo_awards1 = yr_mo_awards1.withColumn('latest_date_yrly' + name[i], latestEachYr)
        yr_mo_awards1 = yr_mo_awards1.filter("latest_date_yrly" + name[i] + " = mo_yr_key_no")
        yr_mo_awards1 = yr_mo_awards1.select(
            ['imc_key_no', "latest_date_yrly" + name[i], 'f_yr', 'combined_awd_rnk']).withColumnRenamed(
            'combined_awd_rnk', 'combined_awd_rnk' + name[i])
        tableList.append(yr_mo_awards1)

    # Left join
    label_aug, label_nov, label_feb = tableList[0], tableList[1], tableList[2]
    label_tbl = label_aug.join(label_nov, ['imc_key_no', 'f_yr'], 'left')
    label_tbl = label_tbl.join(label_feb, ['imc_key_no', 'f_yr'], 'left')

    # Create new feature for labels
    for i in range(len(name)):
        label_tbl = label_tbl.fillna({'combined_awd_rnk' + name[i]: -1})
        label_tbl = label_tbl.withColumn('latest_date_yrly' + name[i], F.when(label_tbl['combined_awd_rnk' + name[i]] == -1, label_tbl['latest_date_yrly_aug']).otherwise(
            label_tbl['latest_date_yrly' + name[i]])) \
            .withColumn('combined_awd_rnk' + name[i],
                        F.when(label_tbl['combined_awd_rnk' + name[i]] == -1, label_tbl['combined_awd_rnk_aug']).otherwise(label_tbl['combined_awd_rnk' + name[i]]))

    winSelect = Window.partitionBy('imc_key_no').orderBy('f_yr')
    value_yrly_awd = lag('combined_awd_rnk_aug', -1).over(winSelect)
    nxt_yr = lag('f_yr', -1).over(winSelect)
    nxt_date_aug = lag('latest_date_yrly_aug', -1).over(winSelect)
    nxt_date_nov = lag('latest_date_yrly_nov', -1).over(winSelect)
    nxt_date_feb = lag('latest_date_yrly_feb', -1).over(winSelect)

    label_tbl = label_tbl.withColumn('nxt_yrly_awd', value_yrly_awd).withColumn('nxt_yr', nxt_yr) \
        .withColumn('nxt_date_aug', nxt_date_aug).withColumn('nxt_date_nov', nxt_date_nov).withColumn('nxt_date_feb', nxt_date_feb) \
        .fillna({'nxt_yrly_awd': -1, 'nxt_yr': -1})

    # Labeling
    for i in range(len(name)):
        label_tbl = label_tbl.withColumn('label' + name[i],
                                         F.when(label_tbl['combined_awd_rnk' + name[i]] == -1, 'INVALID')
                                         .when(label_tbl['nxt_yrly_awd'] == -1, 'INVALID')
                                         .when(label_tbl['nxt_yr'] == -1, 'INVALID')
                                         .when(label_tbl['nxt_date' + name[i]] == label_tbl['latest_date_yrly' + name[i]], 'INVALID')
                                         .when((label_tbl['f_yr'] + 1 == label_tbl['nxt_yr']) & (label_tbl['combined_awd_rnk' + name[i]] == label_tbl['nxt_yrly_awd']), "SAME")
                                         .when((label_tbl['f_yr'] + 1 == label_tbl['nxt_yr']) & (label_tbl['combined_awd_rnk' + name[i]] < label_tbl['nxt_yrly_awd']), "UP")
                                         .when((label_tbl['f_yr'] + 1 == label_tbl['nxt_yr']) & (label_tbl['combined_awd_rnk' + name[i]] > label_tbl['nxt_yrly_awd']), "DOWN")
                                         .when((label_tbl['f_yr'] + 1 < label_tbl['nxt_yr']) & (label_tbl['nxt_yr'] != -1), "DOWN")
                                         .otherwise('INVALID'))

    # Correct Nov/Feb label
    for i in range(1, len(name)):
        label_tbl = label_tbl.withColumn('label' + name[i],
                                         F.when((label_tbl['label' + name[i]] == "SAME") & (label_tbl['label_aug'] == "UP"), 'INVALID')
                                         .otherwise(label_tbl['label' + name[i]]))

    # Write yrly_label_tbl
    label_tbl.repartition(1).write.mode('overwrite').option("maxRecordsPerFile", configs["maxRecordsPerFile"]) \
        .parquet(data_paths['yrly_label_tbl'].format(run_mode=run['run_mode'], run_id=run['run_id']))


def create_yearly_migration_training_table(pargs, params):
    """
    This function creates final yearly migration training table with three versions by months
    Target variable: migration_label, with values 'UP', 'SAME', 'DOWN'
    """

    # For loop generating final training table according to months selected
    for labelMonth in ['aug_label', 'nov_label', 'feb_label']:
        if run['use_sample']:
            abo_dna_full = sqlContext.read.parquet(data_paths['abo_dna_sample'].format(run_mode=run['run_mode'], run_id=run['run_id'])) \
                .sample(False, configs['sampling_fraction'], seed=configs['sampling_seed'])
        else:
            abo_dna_full = sqlContext.read.parquet(data_paths['abo_dna_full_file'].format(run_mode=run['run_mode'], run_id=run['run_id']))

        # Filter by business rules
        abo_dna_full = abo_dna_full.filter("i_globl_bus_stat_cd = 'ACTIVE'").filter("i_globl_imc_type_cd = 'I'")

        if not configs[labelMonth]:
            continue

        # Set parameters for different tables
        if labelMonth == 'aug_label':
            monthCondition = 'month = 8'
            move = "add_months(mo_yr_key_no, 4)"
            usedCol = ['latest_date_yrly_aug', 'imc_key_no', 'f_yr', 'label_aug', 'combined_awd_rnk_aug']
            month = "_aug"
        elif labelMonth == 'nov_label':
            monthCondition = 'month = 11'
            move = "add_months(mo_yr_key_no, 1)"
            usedCol = ['latest_date_yrly_nov', 'imc_key_no', 'f_yr', 'label_nov', 'combined_awd_rnk_nov']
            month = "_nov"
        else:
            monthCondition = 'month = 2'
            move = "add_months(mo_yr_key_no, -2)"
            usedCol = ['latest_date_yrly_feb', 'imc_key_no', 'f_yr', 'label_feb', 'combined_awd_rnk_feb']
            month = "_feb"

        # Changes of abo_dna_full features
        abo_dna_full = abo_dna_full.withColumn('month', F.month('mo_yr_key_no')).withColumn("new_date", expr(move)).withColumn('f_yr', year('new_date'))
        abo_dna_full = abo_dna_full.filter(monthCondition)
        abo_dna_full = abo_dna_full.drop("new_date").drop('month')
        abo_dna_full = abo_dna_full.withColumn("combined_awd_no",
                                               F.when(abo_dna_full['gar_rank_no'] != -1, abo_dna_full['gar_rank_no']).otherwise(abo_dna_full['i_yrly_awd_rnk_no'])) \
            .withColumn("combined_awd_cd", F.when(abo_dna_full['gar_rank_desc'] != -1, abo_dna_full['gar_rank_desc']).otherwise(abo_dna_full['i_yrly_awd_cd']))

        # Join features from abo_dna_full with the label from yrly_table
        yrly_label = sqlContext.read.parquet(data_paths['yrly_label_tbl'].format(run_mode=run['run_mode'], run_id=run['run_id'])).select(usedCol)
        yrly_migration_final = abo_dna_full.join(F.broadcast(yrly_label), ['imc_key_no', 'f_yr'], 'inner')

        # Refine finial table
        yrly_migration_final = yrly_migration_final.withColumnRenamed('f_yr', 'I_PF_YEAR').withColumnRenamed('label' + month, 'I_PF_MIGRATION')
        yrly_migration_final = yrly_migration_final.filter("I_PF_MIGRATION != 'INVALID'")

        # Write final table
        yrly_migration_final.write.mode('overwrite').option("maxRecordsPerFile", configs["maxRecordsPerFile"]).parquet(
            data_paths['yrly_migration_final' + month].format(run_mode=run['run_mode'], run_id=run['run_id']))

        # write yearly migration training table into csv file
        if configs['outputCSV']:
            write_csv_path = data_paths['yrly_migration_final' + month + "_csv"]
            yrly_migration_final.repartition(1).write.format("com.databricks.spark.csv").mode('overwrite') \
                .csv(write_csv_path.format(run_mode=run['run_mode'], run_id=run['run_id']), header='true')


if __name__ == "__main__":
    args = parser.parse_args()

    if args.nextLambdaARN is None:
        exec(args.function + '(None, None)')
    else:
        exec("util.trigger_next_lambda('" + args.nextLambdaARN + "')")
