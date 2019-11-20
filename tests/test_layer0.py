import datetime
import os
import json
import yaml
import sys
import argparse
sys.path.append('/home/hadoop/eap-bcg/airflow/')

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
import spark_df_profiling

sys.path.append('../..')
from airflow.libraries import util
import airflow.queries.layer0_queries as queries


# Set up environment
sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

parser = argparse.ArgumentParser()
parser.add_argument('--function', '-f', help="Input function", type= str)

def test_ingest_behaviors_data(pargs, params):
    """
        **Tests the output of ingest_behaviors_data**

        Inputs:
            Parquet files
                s3n://bcgds/gdwdataextracts/layer0_data/behaviors_combined.parquet

        :return: Null

    """

    behaviors_combined_data = sqlContext.read.parquet("s3n://bcgds/gdwdataextracts/layer0_data/behaviors_combined.parquet")

    # Ensure that all time periods are reflected in the file:
    max_mo_yr_key_no = behaviors_combined_data.agg({"mo_yr_key_no": "max"}).collect()[0][0]
    min_mo_yr_key_no = behaviors_combined_data.agg({"mo_yr_key_no": "min"}).collect()[0][0]
    target_max_date = datetime.datetime(2019, 7, 1)
    target_min_date = datetime.datetime(2013, 1, 1)
    assert max_mo_yr_key_no == target_max_date, "Max date test failed"
    assert min_mo_yr_key_no == target_min_date, "Min date test failed"


def ingest_external_data(pargs, params):
    """
        **Tests the output of ingest_external_data**
        
        Inputs:
        :return: Null

    """
    pass


if __name__ == "__main__":
    args=parser.parse_args()
    #exec(pargs.function + '(pargs, params)')
    exec(args.function + '(None, None)')
