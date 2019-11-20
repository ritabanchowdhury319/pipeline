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
    layer_functions = ['create_monthly_yearly_award', \
                        'process_behaviors_combined', \
                        'create_cntry_promo_imc_order', \
                        'create_performance_year', \
                        'create_support_promotions', \
                        'create_support_first_indicators', \
                        'create_support_repeat_customers_vw', \
                        'create_support_los_plat', \
                        'create_support_los_emerald', \
                        'create_support_los_silver', \
                        'create_support_los_gold', \
                        'create_support_los_diamond', \
                        'create_abo_dna_downline_indiv_vw', \
                        'create_percentile_rank', \
                        'create_average_rank', \
                        'create_average_rank_region', \
                        'create_average_rank_awd_rnk', \
                        'create_classroom_feature', \
                        'create_amwayhub_login', \
                        'wechat_cloudcommerce', \
                        'gar_pf_data_cleansing', \
                        'trigger_layer2']
    next_topic_arn = 'arn:aws:sns:us-east-1:731399195875:TriggerLayer2Topic'

    # Add function step configs to EMR Steps call
    layer_function_steps = []
    task_function_configs = ['--executor-memory', '32g', '--executor-cores', '4', '--driver-memory', '20g', '--conf', 'spark.sql.shuffle.partitions=300', '--conf', 'spark.driver.maxResultSize=4g']
    for l_func in layer_functions:
        if 'trigger_' in l_func:
            func_submit_call_args = ["/usr/bin/spark-submit", CODE_DIR + "pipeline/tasks/layer1/task_Layer1.py", '--function', l_func, '--nextLambdaARN', next_topic_arn]
        else:
            func_submit_call_args = ["/usr/bin/spark-submit"] + task_function_configs + [CODE_DIR + "pipeline/tasks/layer1/task_Layer1.py", '--function', l_func]
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
