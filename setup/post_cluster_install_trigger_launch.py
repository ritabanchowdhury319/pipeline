import boto3

if __name__ == "__main__":
	# Trigger the pipeline trigger lambda
	sns = boto3.client('sns', 'us-east-1')
	response = sns.publish(TopicArn='arn:aws:sns:us-east-1:731399195875:PostClusterLaunchTrigger',Message='Start Spark pipeline run',)