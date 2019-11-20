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
    layer_functions = ['ingest_behaviors_data', \
                        'ingest_external_data', \
                        'ingest_classroom_data', \
                        'ingest_gar_data', \
                        'ingest_fc_dist', \
                        'ingest_fc_dist' \
                        'trigger_layer1']
    
    next_topic_arn = 'arn:aws:sns:us-east-1:731399195875:TriggerLayer1Topic'

    # Add function step configs to EMR Steps call
    layer_function_steps = []
    task_function_configs = ['--executor-memory', '32g', '--executor-cores', '4', '--driver-memory', '20g', '--conf', 'spark.sql.shuffle.partitions=300', '--conf', 'spark.driver.maxResultSize=4g']
    for l_func in layer_functions:
        if 'trigger_' in l_func:
            func_submit_call_args = ["/usr/bin/spark-submit", CODE_DIR + "pipeline/tasks/layer0/task_Layer0.py", '--function', l_func, '--nextLambdaARN', next_topic_arn]
        else:
            func_submit_call_args = ["/usr/bin/spark-submit"] + task_function_configs + [CODE_DIR + "pipeline/tasks/layer0/task_Layer0.py", '--function', l_func]
        step = {"Name": l_func + "-" + time.strftime("%Y%m%d-%H:%M"),
            'ActionOnFailure':'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': func_submit_call_args
            }
        }
        layer_function_steps.append(step)

    # Specify the final Steps flow
    print("Sending work to cluster: ", curr_cluster_id)
    action = conn.add_job_flow_steps(JobFlowId=curr_cluster_id, Steps=layer_function_steps)
    return "Added step: %s"%(action)
