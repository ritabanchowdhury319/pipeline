import pyspark

from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler, StringIndexer, VectorIndexer, StringIndexerModel, VectorIndexerModel, OneHotEncoderEstimator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.linalg import SparseVector
from pyspark.ml import Pipeline
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

from pyspark.sql import SQLContext, Row
import pandas as pd

import os
import random
from datetime import datetime
import subprocess
import yaml
import time


def sampling(df, sampling_fraction=1.0, seed=1):
    """
	This function samples dataframe by fraction
    """
    df = df.sample(False, sampling_fraction, seed)
    return df


def removeLyrFeatures(df):
    """
	This function removes any feature which are not needed
    """
    df = df.select([col for col in df.columns if not col.endswith('_lyr')])
    return df


def removeFeatures(df, features_to_exclude):
    """
	This function removes columns from input data frame
    """
    
    columns_to_keep = [col_name for col_name in df.columns if col_name not in features_to_exclude]
    df = df.select(columns_to_keep)
    
    return df


def extractSignupDateFeatures(df):
    """
	This function extracts features from signup date
    """

    try:
        df = df.withColumnRenamed('dt_signup_month', 'i_signup_month')
        df.drop('dt_signup_month')
    except:
        pass
    try:
        df = df.withColumnRenamed('dt_signup_year', 'i_signup_year')
        df.drop('dt_signup_year')
    except:
        pass
    
    return df


def imputeMissing(df, non_categorical_columns, categorical_columns, fillna_non_categorical_value=0, fillna_categorical_value='unknown_filled'):
    """
	This function fills null with default values based on column data type
    """
    df = df \
        .fillna(fillna_non_categorical_value, non_categorical_columns) \
        .fillna(fillna_categorical_value, categorical_columns)

    return df


def applyStringIndexer(df, string_columns, string_indexers=[]):
    """
	This function convert a data frame's string columns to indexes using spark StringIndexer
    """

    if len(string_indexers) == 0:
        string_indexers = [StringIndexer(inputCol=col_name, outputCol=col_name+"_indexed", handleInvalid="keep").fit(df) for col_name in string_columns if col_name in df.columns]
    string_indexers_pipeline = Pipeline(stages=string_indexers)
    df = string_indexers_pipeline.fit(df).transform(df)
    string_columns_indexed = [col_name+"_indexed" for col_name in string_columns if col_name in df.columns]
    
    return df, string_indexers, string_columns_indexed


def assembleFeaturesIntoVector(df, feature_columns_final, features_assembled_vector="features_assembled_vector"):
    """
	This function assembles all features into a vector stored in a column using spark VectorAssembler
    """
    
    feature_vector_assembler = VectorAssembler(inputCols = feature_columns_final, outputCol = features_assembled_vector)
    df_assembled = feature_vector_assembler.transform(df)
    return df_assembled


def splitTrainAndTest(df, split_ratio, seed, split_col=""):
    """
    This function splits one dataframe into train and test data sets
    """
    if split_col == "":
        train, test = df.randomSplit(split_ratio, seed)
    else:
        key_col_df = df.select(split_col).distinct()
        train_key, test_key = key_col_df.randomSplit(split_ratio, seed)
        train = df.join(train_key, split_col)
        test = df.join(test_key, split_col)
    
    return train, test


def ensureColsAreNumeric(df, non_categorical_columns):
    """
    This function ensures that any columne in the passed list of cols is cast to DoubleType
    """
    cols_and_types = df.dtypes

    for col_entry in cols_and_types:
        if (col_entry[0] in non_categorical_columns) and (col_entry[1] == 'string'):
            df = df.withColumn(col_entry[0], df[col_entry[0]].cast(DoubleType()))

    return df


def ensureColsAreString(df, categorical_columns):
    """
    This function ensure that any columne in the passed list of cols is cast to StringType
    """
    cols_and_types = df.dtypes

    for col_entry in cols_and_types:
        if (col_entry[0] in categorical_columns) and (col_entry[1] != 'string'):
            df = df.withColumn(col_entry[0], df[col_entry[0]].cast(StringType()))

    return df


def oneHotEncoding(df, string_cols_indexed):
    """
    This function performs the one-hot encoding of stringindexed columns
    """
    encoder_stages = []
    string_cols_vec = [col_name + "_vec" for col_name in string_cols_indexed]
    string_cols_encoder = OneHotEncoderEstimator(inputCols=string_cols_indexed, outputCols=string_cols_vec, dropLast=False)
    encoder_stages.append(string_cols_encoder)
    encoders_pipeline = Pipeline(stages=encoder_stages)
    df = encoders_pipeline.fit(df).transform(df)

    return df, string_cols_vec

def getOneHotEncodingPipeline(df, string_columns):
    """
    This function convert a data frame's string columns to indexes using spark StringIndexer
    """
    encoder_stages = [StringIndexer(inputCol=col_name, outputCol=col_name+"_indexed", handleInvalid="keep").fit(df) for col_name in string_columns if col_name in df.columns]
    string_columns_indexed = [col_name+"_indexed" for col_name in string_columns if col_name in df.columns]
    string_cols_vec = [col_name + "_vec" for col_name in string_columns_indexed]
    string_cols_encoder = OneHotEncoderEstimator(inputCols=string_columns_indexed, outputCols=string_cols_vec, dropLast=False)
    encoder_stages.append(string_cols_encoder)
    encoders_pipeline = Pipeline(stages=encoder_stages)
    pipeline_model = encoders_pipeline.fit(df)
    return pipeline_model





def removeLowVarFeatures(df, feature_list, threshold):
    """
    This function removes features with low variance
    """
    # set list of low variance features
    low_var_features = []
    
    # loop over input feature lists
    for f in feature_list:
        # compute standard deviation of column 'f'
        std = float(df.describe(f).filter("summary = 'stddev'").select(f).collect()[0].asDict()[f])
        var = std*std
        # check if column 'f' variance is less of equal to threshold
        if var <= threshold:
            # Drop the low var feature
            low_var_features.append(f)
            df = df.drop(f)

    return df, low_var_features


def recodeExtremeValues(df, threshold):
    """
    This function recodes the extreme values of numerical features
    """
    # Get numeric features
    n_cols = [col_name for col_name in df.columns if col_name[0:2] == "n_"]
    
    # loop over input feature lists
    for f in n_cols:
        # compute standard deviation of column 'f'
        q = df.approxQuantile(f, [threshold], 0.25)[0]
        df = df.withColumn(f, F.when(F.col(f) > q, q).otherwise(F.col(f)))

    return df


def localModelShapleyGeneration(rows, input_cols, input_explainer):
    """
    This function produces Shapley values on a worker node using local Python and Pandas calculations
    """
    rows_pd = pd.DataFrame(rows, columns=input_cols)

    shap_values = input_explainer.shap_values(rows_pd.drop(["imc_key_no"], axis=1))

    return [Row(*([int(rows_pd["imc_key_no"][i])] + [float(f) for f in shap_values[i]])) for i in range(len(shap_values))]


def processMigrationLabel(df, model_label_type, input_target_variable):
    """
    This function process the label col according to designated classification type.
    If binary, we coalesce the "SAME" and "UP" values.
    """
    if model_label_type == "binary":

        df = df.withColumn(input_target_variable, F.when(F.col(input_target_variable) == "UP", "SAME").otherwise(F.col(input_target_variable)))
        df = df.withColumn(input_target_variable, F.when(F.col(input_target_variable) == "SAME", 1).otherwise(0))

    else:

        df = df.withColumn(input_target_variable, F.when(F.col(input_target_variable) == "UP", 2).otherwise(F.when(F.col(input_target_variable) == "SAME", 1).otherwise(0)))

    return df


