import json
import boto3
import time

# specify branch name here
run_mode = 'dev'
target_branch = "develop"
master_instance_type = "r5d.24xlarge"
worker_instance_type = "r5d.24xlarge"
count_workers = 6
cluster_name = "bcgds_pipeline_cluster"
keypair_name = "bcgds-key"

# Everything below is static!

s3 = boto3.resource('s3')
obj = s3.Object('bcgds', 'pipeline/current_branch_name.txt')
obj.put(Body=target_branch)

region = 'us-east-1'
log_uri = 's3n://aws-logs-731399195875-{}/elasticmapreduce/'.format(region)
release_label = 'emr-5.25.0'

subnet = "subnet-0f74d953"
security_group = "sg-0e82598a902aa5fe0"
ebs_root_volume_size = 10 
auto_scaling_role = 'EMR_AutoScaling_DefaultRole'

cluster_wait_timeout_s = 12 * 60
step_wait_timeout_s = 150
_sleep_interval = 30


instance_groups = [
    {   "InstanceCount": count_workers,
        "EbsConfiguration":
            {   "EbsBlockDeviceConfigs": [
                    {"VolumeSpecification": {"SizeInGB":192,"VolumeType":"gp2"},"VolumesPerInstance":4}]},
        "InstanceRole": "CORE",
        "InstanceType": worker_instance_type,
        "Name": "Core - 2"},
    {   "InstanceCount": 1,
        "EbsConfiguration":
            {   "EbsBlockDeviceConfigs": [
                {"VolumeSpecification":{"SizeInGB":192,"VolumeType":"gp2"},"VolumesPerInstance":4}]},
        "InstanceRole": "MASTER",
        "InstanceType": master_instance_type,
        "Name": "Master - 1"}]


def wait_ready(conn, cluster_id):
    sleep_epochs = int(cluster_wait_timeout_s / _sleep_interval)
    for i in range(sleep_epochs):
        time.sleep(_sleep_interval)
        response = conn.describe_cluster(ClusterId = cluster_id)
        state = response['Cluster']['Status']['State']
        if state == "RUNNING" or state == "WAITING":
            return
        print("{}: Sleeping...".format(i))

    raise TimeoutError("Cluster was not read in {}s".format(sleep_epochs * _sleep_interval))


def wait_step_completed(conn, cluster_id, step_id):

    sleep_epochs = int(step_wait_timeout_s / _sleep_interval)
    for i in range(sleep_epochs):
        time.sleep(_sleep_interval)
        response = conn.describe_step(ClusterId = cluster_id, StepId = step_id)
        state = response['Step']['Status']['State']
        if state == 'COMPLETED':
            return
        print("{}: Sleeping...".format(i))

    raise TimeoutError("Step was not completed in {}s".format(sleep_epochs * _sleep_interval))

def lambda_handler(event, context):
    
    try:
        conn = boto3.client("emr")
        
        response = conn.run_job_flow(
            Name = cluster_name,
            ReleaseLabel=release_label,
            VisibleToAllUsers = True,
            LogUri = log_uri,
            JobFlowRole = "EMR_EC2_DefaultRole",
            ServiceRole = "EMR_DefaultRole",
            AutoScalingRole = auto_scaling_role,
            Instances={

                'InstanceGroups': instance_groups,
                'Ec2KeyName': keypair_name,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'HadoopVersion': 'string',
                'Ec2SubnetId': subnet,
                'EmrManagedMasterSecurityGroup': security_group,
                'EmrManagedSlaveSecurityGroup': security_group
            },
            Steps = [{
                'Name': 'Setup hadoop debugging',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['state-pusher-script']
                }
            }],
            Applications=[
                { 'Name': 'Hadoop' },
                { 'Name': 'Hive' },
                { 'Name': 'Pig' },
                { 'Name': 'Hue' },
                { 'Name': 'Spark' },
                { 'Name': 'Zeppelin' } 
                    
            ]
        )
        return_code = response['ResponseMetadata']['HTTPStatusCode']
        
        if return_code != 200:
            print("An error has occured in the launch cluster request. The HTTP reponse code is {}".format(return_code))
            raise ValueError("An error has occured in the launch cluster request. The reponse shows and error. Response: {}".format(response))

        print("Response: ", response)

        cluster_id = response['JobFlowId']
        print("Succesfully launched cluster with id: ", cluster_id)
        
        print("Waiting for cluster to be in ready state.")
        wait_ready(conn, cluster_id)
        print("Cluster became ready. Now adding script pipeline_cluster_setup as a step.")

        print("Updating current_cluster_id.txt with the cluster id ", cluster_id)
        obj = s3.Object('bcgds', 'pipeline/current_cluster_id.txt')
        obj.put(Body = cluster_id)
        print("Finished updating current_cluster_id.txt")

        response = conn.add_job_flow_steps(
                JobFlowId = cluster_id,
                Steps = [{
                    'Name': "pipeline_cluster_setup",
                    'ActionOnFailure' : 'CANCEL_AND_WAIT',
                    'HadoopJarStep' : { 
                        "Args": ["s3://bcgds/pipeline/pipeline_cluster_setup.sh", run_mode],
                        "Jar": "s3://{}.elasticmapreduce/libs/script-runner/script-runner.jar".format(region)
                    }
                }])
        return_code = response['ResponseMetadata']['HTTPStatusCode']
        
        if return_code != 200:
            print("An error has occured when in the request to add step for pipeline_cluster_setup. The HTTP reponse code is {}".format(return_code))
            raise ValueError("An error has occured when in the request to add step for pipeline_cluster_setup. The reponse shows and error. Response: {}".format(response))

        print("Response: ", response)
        step_id = response['StepIds'][0]
        
        print("Waiting for step to complete.")
        wait_step_completed(conn, cluster_id, step_id)
        print("Step was successfully completed.")

        print("The cluster setup is complete and pipeline_cluster_setup run and completed successfully as well. Exiting now.")
    

    except Exception as err:
        message = "An exception has occured. {}".format(err)
        print(message)
        raise err

