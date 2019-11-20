import boto3
import time

cluster_wait_timeout_s = 10 * 60
_sleep_interval = 15


def wait_terminated(conn, cluster_id):
    sleep_epochs = int(cluster_wait_timeout_s / _sleep_interval)
    for i in range(sleep_epochs):
        time.sleep(_sleep_interval)
        response = conn.describe_cluster(ClusterId = cluster_id)
        state = response['Cluster']['Status']['State']
        if state == "TERMINATED":
            print("Cluster status has become TERMINATED. ")
            return
        print("{}: Sleeping...".format(i))

    raise TimeoutError("Cluster was not terminated in {}s".format(sleep_epochs * _sleep_interval))

def lambda_handler(event, context):
    
    try:

        conn = boto3.client("emr")
        s3 = boto3.resource('s3')
        obj = s3.Object('bcgds', 'pipeline/current_cluster_id.txt')
        cluster_id = obj.get()['Body'].read().decode('utf-8').split("\n")[0].strip()
        
        print("Cluster that will be terminated: ", cluster_id)
        
        response = conn.terminate_job_flows(JobFlowIds = [cluster_id])

        return_code = response['ResponseMetadata']['HTTPStatusCode']
        
        if return_code != 200:
            print("An error has occured in the terminate cluster request. The HTTP reponse code is {}".format(return_code))
            raise ValueError("An error has occured in the terminate cluster request. The reponse shows and error. Response: {}".format(response))

        print("Response: ", response)

        print("Waiting for cluster to be in terminated state.")
        wait_terminated(conn, cluster_id)
        print("Cluster terminated succesfully. Exiting.")

    except Exception as err:
        # TODO: Send email.
        message = "An exception has occured. {}".format(err)
        print(message)
        raise err


