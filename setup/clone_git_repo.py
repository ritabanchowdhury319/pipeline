import boto3
import subprocess
import json
import sys
import time
import yaml
import datetime

if __name__ == "__main__":

	if len(sys.argv) < 2 or len(sys.argv) > 3:
		raise ValueError("Usage: <run_mode> <optional: use_sample true/false>")

	# Get secrets
	secrets_client = boto3.client('secretsmanager', 'us-east-1')
	secrets_response = secrets_client.get_secret_value(SecretId="pipeline_git_creds")
	secret = json.loads(secrets_response.get('SecretString'))
	git_name = secret.get('git_acct_name')
	git_pw = secret.get('git_acct_pw')

	# Get branch name
	s3_client = boto3.resource('s3', 'us-east-1')
	obj = s3_client.Object('bcgds', 'pipeline/current_branch_name.txt')
	curr_branch_name = obj.get()['Body'].read().decode('utf-8').split("\n")[0].strip()

	# Create https string:
	git_url = "https://" + git_name + ":" + git_pw + "@github.com/AmwayCorp/eap-bcg.git"
	# Clone git repo
	subprocess.call(["git", "clone", "--single-branch", "--branch", curr_branch_name, git_url])

	print("Completed cloning the repository. Updating run.yml.")

	filename = 'eap-bcg/pipeline/config/run.yml'

	with open(filename, 'r') as f:
		run = yaml.load(f)

	run['run_id'] = datetime.datetime.now().strftime('%Y%m%d_%H%M')
	run['run_mode'] = sys.argv[1]

	if len(sys.argv) == 3:
		run['use_sample'] = str(sys.argv[2]).lower() == "true"

	print("Writing run yaml ", run)

	with open(filename, 'w') as f:
		yaml.dump(run, f)

	print("Finished updateing the yaml file.")