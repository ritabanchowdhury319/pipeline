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

model_name = run['migration_model']['model_name']
configs = util.load_yml_file("/home/hadoop/eap-bcg/pipeline/config/layer3.yml")['bonus_migration'][model_name]


def preprocess_migration_model_training_data(pargs, params):
    """
    Function to pre-process training data for bonus migration models
    """
    
    # Load parameters
    sampling_flag = configs['sampling_flag']
    sampling_fraction = configs['sampling_fraction']
    train_test_split = configs['train_test_split']
    train_test_split_col = configs['train_test_split_col']
    target_variable = configs['target_variable']
    seed = configs['seed']
    
    # Load training data
    train_raw = spark.read.parquet(data_paths[configs['raw_train_data_path']].format(run_mode=run['run_mode'], run_id=run['run_id']))
    
    scoring_filter_column = configs['scoring_filter_column']
    scoring_filter_date = datetime.datetime.strptime(str(configs['scoring_filter_date']), '%Y%m%d') 
    
    # filter training data
    train_raw = train_raw.filter(F.col(scoring_filter_column) < scoring_filter_date)

    # Sample training data
    if sampling_flag:
        train_raw = model.sampling(train_raw, sampling_fraction, seed)
    
    train_raw, onehot_pipeline, final_feature_list = preprocess_migration_model_data(train_raw, True, None, pargs, params)
    
    onehot_pipeline.write().overwrite().save(data_paths['migration_onehot_model'].format(run_mode=run['run_mode'], run_id=run['run_id']))

    # Split into train and test
    train_test_splits = configs["train_test_split"]
    training_data, test_data = model.splitTrainAndTest(train_raw, train_test_splits, seed, train_test_split_col)
    
    return training_data, test_data, target_variable, final_feature_list


def preprocess_migration_model_data(df, process_labels, onehot_pipeline, pargs, params):

    """
    Function to pre-process training data for bonus migration models
    """
    
    # Load parameters
    remove_lyr_features_flag = configs['remove_lyr_features_flag']
    features_to_exclude = configs['features_to_exclude']
    string_columns = configs['string_columns']
    ensure_numeric_columns = configs['ensure_numeric_columns']
    fillna_non_categorical_value = configs['fillna_non_categorical_value']
    fillna_string_value = configs['fillna_string_value']
    target_variable = configs['target_variable']
    # low_var_threshold = configs['low_var_threshold']
    # extreme_recode_threshold = configs['extreme_recode_threshold']
    label_class_type = configs['binary_or_multiclass']
    
    # Remove features ending with lyr for now
    if remove_lyr_features_flag:
        df = model.removeLyrFeatures(df)
    
    # Remove features not needed
    df = model.removeFeatures(df, features_to_exclude)
    
    # Process label col:
    if process_labels:
        df = model.processMigrationLabel(df, label_class_type, target_variable)
    
    # Extract features from signup date
    df = model.extractSignupDateFeatures(df)

    # Get numeric columns
    n_cols = [col_name for col_name in df.columns if col_name[0:2] == "n_"]

    # Ensure that all "n_" cols are indeed numeric
    df = model.ensureColsAreNumeric(df, n_cols + ensure_numeric_columns)
    
    # Impute missing values
    df = model.imputeMissing(df, n_cols, string_columns, fillna_non_categorical_value, fillna_string_value)
    
    # Apply string indexer on string columns
    if onehot_pipeline is None:
        onehot_pipeline = model.getOneHotEncodingPipeline(df, string_columns)

    string_columns_indexed_vec = onehot_pipeline.stages[len(onehot_pipeline.stages) - 1].getOrDefault('outputCols')

    df = onehot_pipeline.transform(df)

    # Remove low var features:
    # Note: shelving this for now. The below code works but is very inefficient.  
    #n_cols = [col_name for col_name in df.columns if col_name[0:2] == "n_"]
    #current_feature_list = n_cols + string_columns_indexed_vec
    #df = model.removeLowVarFeatures(df, current_feature_list, low_var_threshold)
    
    # Recode extreme feature values:
    #df = model.recodeExtremeValues(df, extreme_recode_threshold)
    
    # Assemble the final feature list
    final_feature_list = n_cols + string_columns_indexed_vec
    df = model.assembleFeaturesIntoVector(df, final_feature_list)

    return df, onehot_pipeline, final_feature_list


def run_migration_model(pargs, params):
    """
    Function to train the migration model.
    """

    # Load parameters
    label_class_type = configs['binary_or_multiclass']
    saved_model_path = data_paths[configs['saved_model_path']].format(run_mode=run['run_mode'], run_id=run['run_id'])
    saved_model_path_cv = data_paths[configs['saved_model_path_CrossValidatorModel']].format(run_mode=run['run_mode'], run_id=run['run_id'])
    train_data_path = data_paths[configs['train_data_path']]
    test_data_path = data_paths[configs['test_data_path']]
    feature_list_path = data_paths[configs['feature_list_path']]
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
    num_trees = configs['num_trees']
    seed = configs['seed']

    training_data, test_data, target_variable, final_feature_list  = preprocess_migration_model_training_data(pargs, params)

    trained_model = None

    if label_class_type == "binary":
        gbt = GBTClassifier(labelCol=target_variable, featuresCol="features_assembled_vector", 
                 maxDepth=maxDepth, maxBins=maxBins, maxIter=maxIter, stepSize=stepSize,
                 maxMemoryInMB=maxMemoryInMB, cacheNodeIds=cacheNodeIds, seed=seed, 
                 subsamplingRate=subsamplingRate, featureSubsetStrategy=featureSubsetStrategy)

        trained_model = gbt.fit(training_data)
    else:

        rfc = RandomForestClassifier(labelCol=target_variable, featuresCol="features_assembled_vector", numTrees = num_trees, 
                        maxDepth=maxDepth, maxBins=maxBins, maxMemoryInMB=maxMemoryInMB, cacheNodeIds=cacheNodeIds, seed=seed, 
                        subsamplingRate=subsamplingRate, featureSubsetStrategy=featureSubsetStrategy)

        trained_model = rfc.fit(training_data)

    # Save the trained model
    trained_model.write().overwrite().save(saved_model_path.format(run_mode=run['run_mode'], run_id=run['run_id']))

    # Predict on test data
    prediction = trained_model.transform(test_data)

    # Evaluate prediction
    print("Model evaluation results:")
    auc = None
    if label_class_type == "binary":

        auc = BinaryClassificationEvaluator(rawPredictionCol="probability", labelCol=target_variable, metricName="areaUnderROC").evaluate(prediction)

    else:

        auc = MulticlassClassificationEvaluator(rawPredictionCol="probability", labelCol=target_variable, metricName="areaUnderROC").evaluate(prediction)

    print("auc: {}".format(auc))
    accuracy = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol=target_variable, metricName="accuracy").evaluate(prediction)
    print("accuracy: {}".format(accuracy))
    f1 = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol=target_variable, metricName="f1").evaluate(prediction)
    print("f1: {}".format(f1))
    weightedPrecision = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol=target_variable, metricName="weightedPrecision").evaluate(prediction)
    print("weightedPrecision: {}".format(weightedPrecision))
    weightedRecall = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol=target_variable, metricName="weightedRecall").evaluate(prediction)
    print("weightedRecall: {}".format(weightedRecall))

    # Save performance metrics
    metrics_list = [auc, accuracy, f1, weightedPrecision, weightedRecall]
    metrics_df = spark.createDataFrame(list(map(lambda x: Row(metrics=x), metrics_list)))
    metrics_df.write.parquet(data_paths['migration_model_performance_metrics'].format(run_mode=run['run_mode'], run_id=run['run_id']), mode='overwrite')


    # Save the train and test DFs. Also save the list of variables.
    training_data.write.parquet(train_data_path.format(run_mode=run['run_mode'], run_id=run['run_id']), mode = 'overwrite')
    test_data.write.parquet(test_data_path.format(run_mode=run['run_mode'], run_id=run['run_id']), mode = 'overwrite')
    training_data[final_feature_list].limit(1).write.parquet(feature_list_path.format(run_mode=run['run_mode'], run_id=run['run_id']), mode = 'overwrite')



def scoring_post_model(pargs, params):
    """
    Function to score the input data using the saved model.
    """

    # Load parameters
    label_class_type = configs['binary_or_multiclass']
    saved_model_path = data_paths[configs['saved_model_path']].format(run_mode=run['run_mode'], run_id=run['run_id'])
    scoring_filter_column = configs['scoring_filter_column']
    scoring_filter_date = datetime.datetime.strptime(str(configs['scoring_filter_date']), '%Y%m%d') 
    feature_list_path = data_paths[configs['feature_list_path']]
    output_scored_data = data_paths[configs['scored_data_path']]   # scored data output

    if run['use_sample']:
        abo_dna_data = sqlContext.read.parquet(data_paths['abo_dna_sample'].format(run_mode=run['run_mode'], run_id=run['run_id']))
    else:
        abo_dna_data = sqlContext.read.parquet(data_paths['abo_dna_full_file'].format(run_mode=run['run_mode'], run_id=run['run_id']))

    
    trained_model = None

    if label_class_type == "binary":
        trained_model = GBTClassificationModel.load(saved_model_path)

    else:
        trained_model = RandomForestClassificationModel.load(saved_model_path)

    # Select which subset of abo dna data we want to use for scoring
    abo_dna_data_scoring = abo_dna_data.filter(F.col(scoring_filter_column) >= scoring_filter_date)

    onehot_pipeline = PipelineModel.load(data_paths['migration_onehot_model'].format(run_mode=run['run_mode'], run_id=run['run_id']))

    scoring_data, onehot_pipeline, final_feature_list = preprocess_migration_model_data(abo_dna_data_scoring, False, onehot_pipeline, None, None) 

    # validate that the same input columns are here as training (except the label based columns)
    if final_feature_list != list(sqlContext.read.parquet(feature_list_path.format(run_mode=run['run_mode'], run_id=run['run_id'])).columns):
        raise ValueError("Mismatch in training input and test input.")

    # Produce scoring:
    scored_data = trained_model.transform(scoring_data)

    scored_data.write.parquet(output_scored_data.format(run_mode=run['run_mode'], run_id=run['run_id']), mode = 'overwrite')


def run_local_model_for_shapley(pargs, params):
    """
    Function for training a local version of the Spark model
    """

    # Load parameters
    label_class_type = configs['binary_or_multiclass']
    saved_model_path = data_paths[configs['saved_model_path']]
    train_data_path = data_paths[configs['train_data_path']]
    test_data_path = data_paths[configs['test_data_path']]
    feature_list_path = data_paths[configs['feature_list_path']]
    target_variable = configs['target_variable']
    local_model_sample_size = configs['local_model_sample_size']
    num_trees = configs['num_trees'] 
    maxDepth = configs['maxDepth'] 

    # Output paths
    trained_local_model_path = data_paths['trained_local_model_path']
    local_model_sample_X = data_paths['local_model_sample_X']
    local_model_sample_y = data_paths['local_model_sample_y']

    # Read in Spark. model training data and take a sample
    training_data = spark.read.parquet(train_data_path)
    train_data_sample = training_data.limit(local_model_sample_size)

    # Create pandas DFs for training local models
    train_data_sample_X = train_data_sample.drop(target_variable)
    train_data_sample_y = train_data_sample.select(target_variable)

    train_data_sample_X_pandas = train_data_sample_X.fillna(0).toPandas()
    train_data_sample_y_pandas = train_data_sample_y.fillna(0).toPandas()

    trained_model = None

    if label_class_type == "binary":

        trained_model = GradientBoostingClassifier(n_estimators = num_trees, max_depth = maxDepth, random_state = 42)

        trained_model.fit(train_data_sample_X_pandas.select_dtypes(include=['double', 'integer']), train_data_sample_y_pandas)

    else:

        trained_model = RandomForestClassifier(n_estimators = num_trees, max_depth = maxDepth, random_state = 42, n_jobs = -1)

        trained_model.fit(train_data_sample_X_pandas.select_dtypes(include=['double', 'integer']), train_data_sample_y_pandas)

    # Save the trained model to S3
    pickle_byte_obj = pickle.dumps(trained_model)
    bucket = "bcgds"
    key = trained_local_model_path
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, key).put(Body=pickle_byte_obj)

    # Save the samples used to train the local model
    train_data_sample_X.write.parquet(local_model_sample_X)
    train_data_sample_y.write.parquet(local_model_sample_y)


def produce_shapley_values_renewal_model(pargs, params):
    """
    Function for producing shapley values from trained local model
    """

    # Load parameters
    trained_local_model_path = data_paths['trained_local_model_path']
    migration_scored_data = data_paths[configs['scored_data_path']]

    scoring_data_shapley_values = data_paths['scoring_data_shapley_values']

    # Load local model
    bucket = "bcgds"
    key = trained_local_model_path
    s3 = boto3.resource('s3')
    trained_model = pickle.loads(s3.Bucket(bucket).Object(key).get()['Body'].read())

    shap_explainer = shap.TreeExplainer(trained_model)

    # Apply shap explainer onto full scoring sample, using distributed calculations
    scoring_data = spark.read.parquet(migration_scored_data)
    scoring_data_cols = scoring_data.columns
    scoring_data_with_shaps = scoring_data.rdd.mapPartitions(lambda j: model.localModelShapleyGeneration(j, scoring_data_cols, shap_explainer)).toDF(scoring_data_cols)

    # Save Shapley values
    scoring_data_with_shaps.write.mode('overwrite').parquet(scoring_data_shapley_values)


if __name__ == "__main__":
    args=parser.parse_args()
    #exec(pargs.function + '(pargs, params)')
    if args.nextLambdaARN is None:
        exec(args.function + '(None, None)')
    else:
        exec("util.trigger_next_lambda('" + args.nextLambdaARN + "')")
