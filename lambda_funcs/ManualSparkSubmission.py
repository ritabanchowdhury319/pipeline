import json
import boto3
import time

# Lambda for manually running PySpark functions
# Specify the functions and their associated layer script in function_script_combinations below

def lambda_handler(event, context):
    # Specify the cluster id here:
    curr_cluster_id = ""
    
    # Set repo location
    CODE_DIR = "/home/hadoop/eap-bcg/"

    # Configure the layer steps
    function_script_combinations = [['preprocess_renewal_model_training_data', 'layer3/task_Layer3_renewal_model.py'], \
                        ['train_renewal_model', 'layer3/task_Layer3_renewal_model.py']]

    # Add function step configs to EMR Steps call
    layer_function_steps = []
    task_function_configs = ['--executor-memory', '32g', '--executor-cores', '4', '--driver-memory', '20g', '--conf', 'spark.sql.shuffle.partitions=300']

    for func_combo in function_script_combinations:

        func_submit_call_args = ["/usr/bin/spark-submit"] + task_function_configs + [CODE_DIR + "pipeline/tasks/" + func_combo[1], '--function', func_combo[0]]

        step = {"Name": l_func + "-" + time.strftime("%Y%m%d-%H:%M"),
            'ActionOnFailure':'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': func_submit_call_args
            }
        }
        layer_function_steps.append(step)

    # Specify the final Steps flow
    action = conn.add_job_flow_steps(JobFlowId=curr_cluster_id, Steps=layer_function_steps)
    return "Added step: %s"%(action)
