import datetime
import os
import json
import yaml
import sys
import pickle

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import Row
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import split
from pyspark.sql.functions import to_date, year , month
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml.classification import GBTClassifier, GBTClassificationModel, RandomForestClassifier, RandomForestClassificationModel

from pyspark.sql.types import *
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, StringIndexerModel, VectorAssembler
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession

import boto3
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
import shap

# from importlib import reload

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("ERROR")

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--function', '-f', help="Input function", type= str)
parser.add_argument('--nextLambdaARN', '-nla', help="Next Lambda ARN", type= str)

sys.path.append('/home/hadoop/eap-bcg/')

from pipeline.libraries import util
from pipeline.libraries import model

# Load configs
run = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/run.yml")
data_paths = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/data_paths.yml")
configs = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/layer3.yml")['renewal']


def preprocess_renewal_model_training_data(pargs, params):
    """
    Function to pre-process raw training data for renewal model
    """
    
    # Load parameters
    train_filter_flag = configs['train_filter_flag']
    train_filter_condition = configs['train_filter_condition']
    train_sampling_flag = configs['train_sampling_flag']
    train_sampling_fraction = configs['train_sampling_fraction']
    features_to_exclude = configs['features_to_exclude']
    primary_key_columns = configs['primary_key_columns']
    fillna_non_categorical_value = configs['fillna_non_categorical_value']
    fillna_categorical_value = configs['fillna_categorical_value']
    train_test_split_flag = configs['train_test_split_flag']
    train_test_split_ratio = configs['train_test_split_ratio']
    train_test_split_col = configs['train_test_split_col']
    target_variable = configs['target_variable']
    seed = configs['seed']
    s3_bucket = configs['s3_bucket']
    
    # Load training data
    train_raw = spark.read.parquet(data_paths['yr1_renewal_train_raw'].format(run_mode=run['run_mode'], run_id=run['run_id']))

    # Filter training data
    if train_filter_flag:
        train_raw = train_raw.filter(train_filter_condition)
    
    # Sample training data
    if train_sampling_flag:
        train_raw = model.sampling(train_raw, train_sampling_fraction, seed)
    
    # Remove features not needed
    train_raw = model.removeFeatures(train_raw, features_to_exclude)

    # Get categorical/non-categorical columns
    non_categorical_columns = [col for col in train_raw.columns if (col.startswith('n_')) and (col != target_variable) and (col not in primary_key_columns)]
    categorical_columns = [col for col in train_raw.columns if (col.startswith('i_')) and (col != target_variable) and (col not in primary_key_columns)]
    
    # Ensure that all "n_" cols are indeed numeric
    train_raw = model.ensureColsAreNumeric(train_raw, non_categorical_columns)

    # Ensure that all "i_" cols are indeed string
    train_raw = model.ensureColsAreString(train_raw, categorical_columns)

    # Impute missing values
    train_raw = model.imputeMissing(train_raw, non_categorical_columns, categorical_columns, fillna_non_categorical_value, fillna_categorical_value)
    
    # Apply string indexer on string columns
    train_raw, string_indexers, categorical_columns_indexed = model.applyStringIndexer(train_raw, categorical_columns)
    
    # Assemble the final feature list
    feature_list = non_categorical_columns + categorical_columns
    feature_list_indexed = non_categorical_columns + categorical_columns_indexed
    train_raw = model.assembleFeaturesIntoVector(train_raw, feature_list_indexed)
    
    # Split into train and test
    # Save train and test data
    if train_test_split_flag:
        train, test = model.splitTrainAndTest(train_raw, train_test_split_ratio, seed, train_test_split_col)
        train.write.parquet(data_paths['renewal_train'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')
        test.write.parquet(data_paths['renewal_test'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')
    else:
        train = train_raw
        train.write.parquet(data_paths['renewal_train'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')
    
    # Save string indexers
    for i in range(len(string_indexers)):
        string_indexers[i].write().overwrite().save(data_paths['renewal_string_indexer'].format(run_mode=run['run_mode'], run_id=run['run_id'], i=i))

    # Save feature list and string column list
    feature_config = {'feature_list': feature_list, 'feature_list_indexed': feature_list_indexed, 'categorical_columns': categorical_columns, 'non_categorical_columns': non_categorical_columns}
    util.save_yml_file_to_s3(feature_config, s3_bucket, data_paths['renewal_feature_config'].format(run_mode=run['run_mode'], run_id=run['run_id'])[12:])


def train_renewal_model(pargs, params):
    """
    Function to train the renewal model using spark GBT
    """

    # Load parameters
    target_variable = configs['target_variable']
    seed = configs['seed']
    maxDepth = configs['maxDepth'] 
    maxBins = configs['maxBins']
    maxIter = configs['maxIter']
    stepSize = configs['stepSize']
    maxMemoryInMB = configs['maxMemoryInMB']
    cacheNodeIds = configs['cacheNodeIds']
    subsamplingRate = configs['subsamplingRate']
    featureSubsetStrategy = configs['featureSubsetStrategy']
    cv_folds = configs['cv_folds']
    grid_search_maxDepth = configs['grid_search_maxDepth']
    grid_search_maxBins = configs['grid_search_maxBins']
    grid_search_maxIter = configs['grid_search_maxIter']

    # Load processed train and test data
    train = spark.read.parquet(data_paths['renewal_train'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    test = spark.read.parquet(data_paths['renewal_test'].format(run_mode=run['run_mode'], run_id=run['run_id']))

    # Train renewal model
    trained_model = None
    gbt = GBTClassifier(labelCol=target_variable, featuresCol="features_assembled_vector", 
        maxDepth=maxDepth, maxBins=maxBins, maxIter=maxIter, stepSize=stepSize,
        maxMemoryInMB=maxMemoryInMB, cacheNodeIds=cacheNodeIds, seed=seed, 
        subsamplingRate=subsamplingRate, featureSubsetStrategy=featureSubsetStrategy)
    trained_model = gbt.fit(train)

    # Get feature importance
    print(trained_model.featureImportances)

    # Predict on test data
    prediction = trained_model.transform(test)

    # Evaluate prediction
    print("Model evaluation results:")
    auc = BinaryClassificationEvaluator(rawPredictionCol="probability", labelCol=target_variable, metricName="areaUnderROC").evaluate(prediction)
    print("auc: {}".format(auc))
    accuracy = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol=target_variable, metricName="accuracy").evaluate(prediction)
    print("accuracy: {}".format(accuracy))
    f1 = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol=target_variable, metricName="f1").evaluate(prediction)
    print("f1: {}".format(f1))
    weightedPrecision = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol=target_variable, metricName="weightedPrecision").evaluate(prediction)
    print("weightedPrecision: {}".format(weightedPrecision))
    weightedRecall = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol=target_variable, metricName="weightedRecall").evaluate(prediction)
    print("weightedRecall: {}".format(weightedRecall))

    # Also perform hyperparameter grid search and CV:
    param_grid = (ParamGridBuilder()
             .addGrid(gbt.maxDepth, grid_search_maxDepth)
             .addGrid(gbt.maxBins, grid_search_maxBins)
             .addGrid(gbt.maxIter, grid_search_maxIter)
             .build())

    gbt_evaluator = BinaryClassificationEvaluator(rawPredictionCol="probability", labelCol=target_variable, metricName="areaUnderROC")
    cv = CrossValidator(estimator=gbt, estimatorParamMaps=param_grid, evaluator=gbt_evaluator, numFolds=cv_folds)
    cv_model = cv.fit(train)
    gbt_cv_predictions = cv_model.transform(test)
    cv_eval = gbt_evaluator.evaluate(gbt_cv_predictions)
    print("CV AUC: {}".format(cv_eval))

    # Save trained model
    trained_model.write().overwrite().save(data_paths['renewal_model'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    cv_model.write().overwrite().save(data_paths['renewal_model_CrossValidatorModel'].format(run_mode=run['run_mode'], run_id=run['run_id']))

    # Save performance metrics
    metrics_list = [auc, accuracy, f1, weightedPrecision, weightedRecall]
    metrics_df = spark.createDataFrame(list(map(lambda x: Row(metrics=x), metrics_list)))
    metrics_df.write.parquet(data_paths['renewal_model_performance_metrics'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def preprocess_renewal_model_scoring_data(pargs, params):
    """
    Function to pre-process raw scoring data for renewal model
    """
    
    # Load parameters
    score_filter_flag = configs['score_filter_flag']
    score_filter_condition = configs['score_filter_condition']
    score_sampling_flag = configs['score_sampling_flag']
    score_sampling_fraction = configs['score_sampling_fraction']
    primary_key_columns = configs['primary_key_columns']
    fillna_non_categorical_value = configs['fillna_non_categorical_value']
    fillna_categorical_value = configs['fillna_categorical_value']
    target_variable = configs['target_variable']
    seed = configs['seed']
    s3_bucket = configs['s3_bucket']
    
    # Load raw scoring data
    score_raw = spark.read.parquet(data_paths['yr1_renewal_scoring_raw'].format(run_mode=run['run_mode'], run_id=run['run_id']))

    # Load feature config saved in the pre-processing step
    feature_config = util.load_yml_file_from_s3(s3_bucket, data_paths['renewal_feature_config'].format(run_mode=run['run_mode'], run_id=run['run_id'])[12:])
    feature_list = feature_config['feature_list']
    feature_list_indexed = feature_config['feature_list_indexed']
    categorical_columns = feature_config['categorical_columns']
    non_categorical_columns = feature_config['non_categorical_columns']

    # Load string indexers
    string_indexers = []
    for i in range(len(categorical_columns)):
        string_indexers.append(StringIndexerModel.load(data_paths['renewal_string_indexer'].format(run_mode=run['run_mode'], run_id=run['run_id'], i=i)))
    
    # Select only the features used in model training
    score_raw = score_raw.select(primary_key_columns + feature_list)

    # Filter scoring data
    if score_filter_flag:
        score_raw = score_raw.filter(score_filter_condition)

    # Sample scoring data
    if score_sampling_flag:
        score_raw = model.sampling(score_raw, score_sampling_fraction, seed)
    
    # Ensure that all "n_" cols are indeed numeric
    score_raw = model.ensureColsAreNumeric(score_raw, non_categorical_columns)

    # Ensure that all "i_" cols are indeed string
    score_raw = model.ensureColsAreString(score_raw, categorical_columns)
    
    # Impute missing values
    score_raw = model.imputeMissing(score_raw, non_categorical_columns, categorical_columns, fillna_non_categorical_value, fillna_categorical_value)
    
    # Apply string indexer on string columns
    score_raw, string_indexers, categorical_columns_indexed = model.applyStringIndexer(score_raw, categorical_columns, string_indexers)
    
    # Assemble the final feature list
    score_raw = model.assembleFeaturesIntoVector(score_raw, feature_list_indexed)
    
    # Save score data
    score_raw.write.parquet(data_paths['renewal_score'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def score_renewal_model(pargs, params):
    """
    Function to score the input data using the trained renewal model
    """

    # Load parameters
    probability_threshhold = configs['probability_threshhold']

    # Load processed scoring data
    score = spark.read.parquet(data_paths['renewal_score'].format(run_mode=run['run_mode'], run_id=run['run_id']))

    # Load trained model
    trained_model = None
    trained_model = GBTClassificationModel.load(data_paths['renewal_model'].format(run_mode=run['run_mode'], run_id=run['run_id']))

    # Produce scoring
    scoring_output = trained_model.transform(score)

    # Extract probability value (from vector [prob for neg class, prob for positive class])
    extrac_prob_udf = F.udf(lambda v: float(v[1]), DoubleType())
    scoring_output = scoring_output \
        .withColumn('renewal_probability', extrac_prob_udf(F.col('probability'))) \
        .withColumn('renewal_label', F.when(F.col('renewal_probability') >= probability_threshhold, 1).otherwise(0))

    # Save scoring output
    scoring_output.write.parquet(data_paths['renewal_scoring_output'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def run_local_renewal_model_for_shapley(pargs, params):
    """
    Function for training a local version of the Spark model
    """

    # Configs for renewal local model
    local_model_sample_size = configs['local_model_sample_size']
    local_model_num_trees = configs['local_model_num_trees'] 

    # Read in Spark. model training data and take a sample
    training_data = spark.read.parquet(data_paths['renewal_train'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    train_data_sample = training_data.limit(local_model_sample_size)

    # Create pandas DFs for training local models
    train_data_sample_X = train_data_sample.drop(target_variable)
    train_data_sample_y = train_data_sample.select(target_variable)

    train_data_sample_X_pandas = train_data_sample_X.fillna(0).toPandas()
    train_data_sample_y_pandas = train_data_sample_y.fillna(0).toPandas()

    # Train local model
    trained_model = None
    trained_model = GradientBoostingClassifier(n_estimators = local_model_num_trees, max_depth = 5, random_state = 42)
    trained_model.fit(train_data_sample_X_pandas.select_dtypes(include=['double', 'integer']), train_data_sample_y_pandas)

    # Save the trained model to S3
    pickle_byte_obj = pickle.dumps(trained_model)
    bucket = "bcgds"
    key = data_paths['renewal_model_local'].format(run_mode=run['run_mode'], run_id=run['run_id'])
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, key).put(Body=pickle_byte_obj)

    # Save the samples used to train the local model
    train_data_sample_X.write.parquet(data_paths['renewal_model_train_data_sample_X'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')
    train_data_sample_y.write.parquet(data_paths['renewal_model_train_data_sample_Y'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


def produce_shapley_values_renewal_model(pargs, params):
    """
    Function for producing shapley values from trained local model
    """

    # Load local model
    bucket = "bcgds"
    key = data_paths['renewal_model_local'].format(run_mode=run['run_mode'], run_id=run['run_id'])
    s3 = boto3.resource('s3')
    trained_model = pickle.loads(s3.Bucket(bucket).Object(key).get()['Body'].read())

    shap_explainer = shap.TreeExplainer(trained_model)

    # Apply shap explainer onto full scoring sample, using distributed calculations
    scoring_data = spark.read.parquet(data_paths['renewal_scoring_output'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    scoring_data_cols = scoring_data.columns
    scoring_data_with_shaps = scoring_data.rdd.mapPartitions(lambda j: model.localModelShapleyGeneration(j, scoring_data_cols, shap_explainer)).toDF(scoring_data_cols)

    # Save Shapley values
    scoring_data_with_shaps.write.mode('overwrite').parquet(data_paths['renewal_scoring_data_shapley_values'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


if __name__ == "__main__":
    args=parser.parse_args()
    #exec(pargs.function + '(pargs, params)')
    if args.nextLambdaARN is None:
        exec(args.function + '(None, None)')
    else:
        exec("util.trigger_next_lambda('" + args.nextLambdaARN + "')")

