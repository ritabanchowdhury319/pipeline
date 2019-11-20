import sys
from datetime import datetime
from datetime import datetime, timedelta
import os
import json
import pytest

print("Adding: ", os.path.dirname(os.path.dirname(os.getcwd())))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.getcwd())))

from pipeline.libraries import util
import pipeline.queries.layer0_queries as queries
from pipeline.tasks.layer1 import task_Layer1


spark = task_Layer1.spark
run = task_Layer1.run
data_paths = task_Layer1.data_paths
configs = task_Layer1.configs
sqlContext = task_Layer1.sqlContext

# If true won't actually run the pipeline but simply use the results from the previously run pipeline.
# Must ensure that the run configuration matches and all files are available.
RUN_PIPELINE = False


test_functions = [
 	('combined_awards',  [data_paths['combined_yr_mo_awards']]), 
    ('behaviors_combined_data',  [data_paths['behaviors_combined_data']]), 
    ('china_promo_imc_order',  [data_paths['china_promo_imc_order']]), 
    ('performance_year',  [data_paths['performance_year']]), 
    ('support_promotions',  [data_paths['support_promotions']]), 
    ('support_first_indicators',  [data_paths['support_first_indicators']]), 
    ('support_repeat_customers_vw',  [data_paths['support_repeat_customers_vw']]), 
    ('support_los_plat',  [data_paths['support_los_plat']]), 
    ('support_los_emerald',  [data_paths['support_los_emerald']]), 
    ('support_los_silver',  [data_paths['support_los_silver']]), 
    ('support_los_gold',  [data_paths['support_los_gold']]), 
    ('support_los_diamond',  [data_paths['support_los_diamond']]), 
    ('abo_dna_downline_indiv_vw',  [data_paths['abo_dna_downline_indiv_vw']]), 
    ('percentile_rank',  [data_paths['percentile_rank']]), 
    ('average_rank',  [data_paths['average_rank']]), 
    ('average_rank_region',  [data_paths['average_rank_region']]), 
    ('average_rank_awd_rnk',  [data_paths['average_rank_awd_rnk']]), 
    ('classroom_data',  [data_paths['classroom_data']]), 
    ('amway_hub',  [data_paths['amwayhub_login']]), 
    ('wechat_cloudcommerce',  [data_paths['wechat_cloudcommerce']]), 
    ('gar_pf_cleaned',  [data_paths['gar_pf_cleaned']])
]


def schema_is_same(schema1, schema2, check_nullable = False):
	if check_nullable:
		return schema1 == schema2

	if schema1['type'] != schema2['type']:
		return False

	cols1 = [x['name'] for x in schema1['fields']]
	cols2 = [x['name'] for x in schema2['fields']]

	if cols1 != cols2:
		return False

	for i in range(len(cols1)):
		if schema1['fields'][i]['type'] != schema2['fields'][i]['type']:
			return False

	return True


@pytest.mark.parametrize("function, names", test_functions)
def test_schema(function, names):
	print("Running schema test for ", function)

	if RUN_PIPELINE:
		print("Running function: ", test_function)
		exec("task_Layer1." + function + '(None, None)')


	for name in names:
		filename = name.format(run_mode=run['run_mode'], run_id=run['run_id'])
		print("checking schema for ", filename)
		
		df = spark.read.parquet(filename)
		
		with open(os.path.basename(filename) + '.json', 'r') as f:
			ref_schema = json.load(f)
		
		assert(schema_is_same(ref_schema, json.loads(df.schema.json()))), "The schema of {} as read from {} does not match that stored for test case.".format(filename, filename + '.json')


