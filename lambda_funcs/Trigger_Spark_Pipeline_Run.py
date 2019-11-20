'''
Triggers a new Spark pipeline run.

Prior to this, we should've started a new EMR cluster (with the boostrap action script), cloned the git repo on it, and checked out our desired branch.

Below, we may either specify the cluster id of the cluster we've just created, or we may leave it blank and allow the remainder of the pipeline to use the most-recent active cluster.

'''


import json
import boto3
import datetime

def lambda_handler(event, context):
	# Specify the current cluster's cluster_id or otherwise leave it blank to utilize the most recent active cluster.
	curr_cluster_id = ""
	s3 = boto3.resource('s3')

	# Write the cluster id to S3
	if curr_cluster_id != "":
		obj = s3.Object('bcgds', 'pipeline/Trigger_Spark_Pipeline_Run.txt')
		obj.put(Body=curr_cluster_id)
	else:
		# Get the cluster id of the most-recent active cluster, and write to the current cluster id file.
		print("Getting cluster from list of clusters and picking the top one.")
		conn = boto3.client("emr")
		clusters = conn.list_clusters()
		clusters = [c["Id"] for c in clusters["Clusters"] if c["Status"]["State"] in ["RUNNING", "WAITING"]]
		if not clusters:
			sys.stderr.write("No valid clusters\n")
			sys.stderr.exit()
		curr_cluster_id = clusters[0]
		obj = s3.Object('bcgds', 'pipeline/current_cluster_id.txt')
		obj.put(Body=curr_cluster_id)
	print("Running on cluster: ", curr_cluster_id)
	
	# Send message to the next Layer lambda function via SNS
	sns = boto3.client('sns')
	response = sns.publish(TopicArn='arn:aws:sns:us-east-1:731399195875:TriggerPipelineTopic',Message='Start Spark pipeline run',)
	print(response)
	return "Started Spark pipeline run at: " + str(datetime.datetime.now())
