import sys
from datetime import datetime
from datetime import datetime, timedelta
import os
import json


print("Adding: ", os.path.dirname(os.path.dirname(os.getcwd())))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.getcwd())))

from pipeline.libraries import util
import pipeline.queries.layer0_queries as queries
from pipeline.tasks.layer0 import task_Layer0

spark = task_Layer0.spark
run = task_Layer0.run
data_paths = task_Layer0.data_paths
configs = task_Layer0.configs
sqlContext = task_Layer0.sqlContext

# If true won't actually run the pipeline but simply use the results from the previously run pipeline.
# Must ensure that the run configuration matches and all files are available.
RUN_PIPELINE = False



def test_ingest_behaviors_data():
	print("In test_ingest_external_data")
	print("Calling ingest_external_data")
	if RUN_PIPELINE:
		task_Layer0.ingest_behaviors_data(None, None)
	print("Reading output...")
	behaviors_combined_data = spark.read.parquet(data_paths["behaviors_combined_data_full"].format(run_mode=run['run_mode'], run_id=run['run_id'])).cache()
	#behaviors_combined_data.show()
	
	with open('behaviours.json', 'r') as f:
		ref_schema = json.load(f)
	
	assert(ref_schema == json.loads(behaviors_combined_data.schema.json())), "The schema of behaviors_combined_data as read from {} does not match that stored for test case.".format(data_paths["behaviors_combined_data_full"].format(run_mode=run['run_mode'], run_id=run['run_id']))
	print("Schema test passed.")
	behaviors_combined_data_count = behaviors_combined_data.count()
	assert(behaviors_combined_data_count > 0), "The behaviours combined dataset as loaded seems to be empty."
	print("test_ingest_behaviors_data passed.")
