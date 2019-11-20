#!/bin/bash

pip=pip-3.6
python=python3

if [[ "$EUID" == "0" ]]; then
	echo "This script should not be run as sudo. It only requires the user running it to have sudo."
	exit 1
fi

run_mode='dev'
if [[ "$#" == "1" ]]; then
	run_mode="$1"
fi


echo "Using user: " `whoami`
echo "In dir: " `pwd`
echo "Changing dir to home."
cd
echo "Now in dir user: " `pwd`
echo "Run mode is : $run_mode"

echo 'alias python=python3' >> ~/.bash_profile

sudo yum install -y git-core

sudo $pip install boto3 spark_df_profiling sklearn shap

echo "" | sudo tee -a  /etc/spark/conf/spark-env.sh
echo "export PYSPARK_PYTHON=/usr/bin/python3" | sudo tee -a /etc/spark/conf/spark-env.sh

aws s3 cp s3://bcgds/pipeline/clone_git_repo.py ~
aws s3 cp s3://bcgds/pipeline/post_cluster_install_trigger_launch.py ~

if [[ "$run_mode" == "test" ]]; then
	$python clone_git_repo.py $run_mode "true"
else
	$python clone_git_repo.py $run_mode
fi
$python post_cluster_install_trigger_launch.py
