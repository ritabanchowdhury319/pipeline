"""Utilities used by the main task files"""

from py4j.protocol import Py4JJavaError
from pyspark import SparkContext
from pyspark.sql import SQLContext
import yaml
import boto3


def poke_s3(s3_path, sc_inst):
    """ Checks to see if the s3 path exists """
    try:
        rdd = sc_inst.textFile(s3_path)
        rdd.take(1)
        return True
    except Py4JJavaError as e:
        return False


def load_yml_file(file_location):
    """ Function to load yml file from work directory """
    with open(file_location, 'r') as stream:
        config = yaml.load(stream)

    return config


def save_yml_file_to_s3(file_content, s3_bucket, file_location):
    """ Function to save yml file to S3"""
    yaml_dict = yaml.dump(file_content, default_flow_style=False)
    bucket = s3_bucket
    key = file_location
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, key).put(Body=yaml_dict)


def load_yml_file_from_s3(s3_bucket, file_location):
    """ Function to load yml file from S3"""
    bucket = s3_bucket
    key = file_location
    s3_resource = boto3.resource('s3')
    config = yaml.safe_load(s3_resource.Object(bucket, key).get()['Body'])
    return config


def trigger_next_lambda(next_lambda_arn):
    """ Function to trigger next lamdba function """
    sns = boto3.client('sns')
    response = sns.publish(TopicArn=next_lambda_arn,Message='Start Spark pipeline run',)
