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
    layer_functions = ['create_abo_dna_table', \
                        'create_migration_monthly_target_variables', \
                        'create_bns_migration_training_table', \
                        'create_yearly_migration_training_label', \
                        'create_yearly_migration_training_table', \
                        'create_renewal_response_variable', \
                        'create_renewal_training_data_step1', \
                        'create_renewal_training_data_step2', \
                        'create_renewal_training_data_step3', \
                        'trigger_layer3']
    # FIXME
    #layer_functions = ['create_abo_dna_table', 'create_migration_monthly_target_variables', 'create_bns_migration_training_table', 'create_yearly_migration_training_label', 'create_yearly_migration_training_table', 'trigger_layer3']
                        
    next_topic_arn = 'arn:aws:sns:us-east-1:731399195875:TriggerLayer3Topic'

    # Specify the file in which each function resides.
    function_file_mapping = {'create_abo_dna_table': 'task_Layer2_ABO_DNA.py', \
                                'create_migration_monthly_target_variables': 'task_Layer2_migration_model.py', \
                                'create_bns_migration_training_table': 'task_Layer2_migration_model.py',\
                                'create_yearly_migration_training_label': 'task_Layer2_migration_model.py',\
                                'create_yearly_migration_training_table': 'task_Layer2_migration_model.py',\
                                'create_renewal_response_variable': 'task_Layer2_renewal_model.py', \
                                'create_renewal_training_data_step1': 'task_Layer2_renewal_model.py',
                                'create_renewal_training_data_step2': 'task_Layer2_renewal_model.py',
                                'create_renewal_training_data_step3': 'task_Layer2_renewal_model.py'}

    # Add function step configs to EMR Steps call
    layer_function_steps = []
    # task_function_configs = ['--deploy-mode', 'cluster', '--conf', 'spark.executor.memory=40g', '--conf', 'spark.driver.maxResultSize=4096', '--conf', 'spark.executor.instances=19', '--conf', 'spark.executor.memoryOverhead=4g', '--conf', 'spark.driver.memory=300g', '--conf', 'spark.driver.maxResultSize=200g']
    task_function_configs = ['--executor-memory', '32g', '--executor-cores', '4', '--driver-memory', '20g', '--conf', 'spark.sql.shuffle.partitions=300', '--conf', 'spark.driver.maxResultSize=4g', '--conf', 'spark.driver.maxResultSize=4g']
    for l_func in layer_functions:
        if 'trigger_' in l_func:
            func_submit_call_args = ["/usr/bin/spark-submit", CODE_DIR + "pipeline/tasks/layer1/task_Layer1.py", '--function', l_func, '--nextLambdaARN', next_topic_arn]
        else:
            func_submit_call_args = ["/usr/bin/spark-submit"] + task_function_configs + [CODE_DIR + "pipeline/tasks/layer2/" + function_file_mapping[l_func], '--function', l_func]
        step = {"Name": l_func + "-" + time.strftime("%Y%m%d-%H:%M"),
            # FIXME
            'ActionOnFailure': 'CANCEL_AND_WAIT',
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
