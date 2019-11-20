"""Functions used in etl tasks"""

import datetime
import os
import json
import yaml
import sys

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import split
from pyspark.sql.functions import to_date, year , month
from pyspark.sql.types import StructType, StructField, StringType


def get_lag_cols(df):
	three_mo_col = configs['lag_column_3m']
	six_mo_col = configs['lag_column_6m']
	twelve_mo_col = configs['lag_column_12m']
	lag_cols =  twelve_mo_col
	return three_mo_col,six_mo_col,lag_cols


def add_3mo_mom_features(df,three_mo_col,six_mo_col):
	for lag_3m in three_mo_col:
		for lag_6m in six_mo_col:
			lag_feature_3m = lag_3m[:-2]
			lag_feature_6m = lag_6m[:-2]
			if lag_feature_3m == lag_feature_6m:
				print('Same feature type')
			else:
				continue
			mom_feature = lag_feature_3m+"3m_mom"
			df = df.withColumn(mom_feature,df[lag_3m]/(df[lag_6m]-df[lag_3m]))
	return df


def get_yoy_features (df,lag_cols):
	for col in lag_cols:
		col_ly = col + "_lyr"
		df = df.withColumn(col_ly,F.lag(df[col],12).over(Window.partitionBy("imc_no").orderBy("mo_yr_key_no")))
		mom_feature = col+"12m_mom"
		df = df.withColumn(mom_feature,df[col]/df[col_ly])
	return df
