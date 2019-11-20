import json
import boto3
import time

def lambda_handler(event, context):
    conn = boto3.client("emr")
    s3 = boto3.resource('s3')
    obj = s3.Object('bcgds', 'pipeline/current_cluster_id.txt')
    curr_cluster_id = obj.get()['Body'].read().decode('utf-8').split("\n")[0].strip()
    # Set repo location
    CODE_DIR = "/home/hadoop/eap-bcg/"

    # Configure the layer steps
    layer_functions = [
        'preprocess_renewal_model_training_data',
        'train_renewal_model',
        'preprocess_renewal_model_scoring_data',
        'score_renewal_model',
        'run_migration_model',
        'scoring_post_model',
        'trigger_terminate'
    ]
    
    #FIXME:
    #layer_functions = [
    #    'run_migration_model',
    #    'scoring_post_model',
    #    'trigger_terminate'
    #]
    next_topic_arn = 'arn:aws:sns:us-east-1:731399195875:TerminateClusterForPipelineRun'
    
    # Specify the file in which each function resides.
    function_file_mapping = {
        'preprocess_renewal_model_training_data': 'task_Layer3_renewal_model.py',
        'train_renewal_model': 'task_Layer3_renewal_model.py',
        'preprocess_renewal_model_scoring_data': 'task_Layer3_renewal_model.py',
        'score_renewal_model': 'task_Layer3_renewal_model.py',
        'run_migration_model': 'task_Layer3_migration_model.py',
        'scoring_post_model': 'task_Layer3_migration_model.py'
    }

    # Add function step configs to EMR Steps call
    layer_function_steps = []
    task_function_configs = ['--executor-memory', '32g', '--executor-cores', '4', '--driver-memory', '20g', '--conf', 'spark.sql.shuffle.partitions=300']
    for l_func in layer_functions:
        if 'trigger_' in l_func:
            func_submit_call_args = ["/usr/bin/spark-submit", CODE_DIR + "pipeline/tasks/layer1/task_Layer1.py", '--function', l_func, '--nextLambdaARN', next_topic_arn]
        else:
            func_submit_call_args = ["/usr/bin/spark-submit"] + task_function_configs + [CODE_DIR + "pipeline/tasks/layer3/" + function_file_mapping[l_func], '--function', l_func]
        step = {"Name": l_func + "-" + time.strftime("%Y%m%d-%H:%M"),
            # FIXME
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            #'ActionOnFailure':'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': func_submit_call_args
            }
        }
        layer_function_steps.append(step)

    # Specify the final Steps flow
    action = conn.add_job_flow_steps(JobFlowId=curr_cluster_id, Steps=layer_function_steps)
    return "Added step: %s"%(action)

