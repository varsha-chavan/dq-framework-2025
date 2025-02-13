<<<<<<< HEAD
<<<<<<< HEAD
# Databricks notebook source

=======
=======
# Databricks notebook source
>>>>>>> 8bc73f8 (changes in constant.py,validation_config .py and validation.py files)
import sys
from awsglue.utils import getResolvedOptions
from common.custom_logger import getlogger
from common.utils import *
>>>>>>> other-repo1/dq-framework-dev-202501
from common.constants import *
from common.validation_config import *
from common.custom_logger import *
from common.spark_config import *
from Utilities.table_loader import *
from Utilities.validation import *
from Utilities.dq__execution import *
logger = getlogger()

def main():
    # get entity id
    args = getResolvedOptions(sys.argv, ['entity_id'])
    entity_id = args['entity_id']
    # load config tables

<<<<<<< HEAD
    """
        #runtime input from glue job.
        Step 1: Get custom logger.
        Step 2: Initialize the spark session.
        Step 3: Load configuration from configuration table in a df => rule_master_df, entity_master_df, execution_plan_df.
        Step 4: Perform metadata validation for configuration table df's.
        Step 5: fetch the entity file path from entity_master_df.
        Step 6: load entity data in df => entity_data_df.
        Step 7: Execute the DQ framework.
    """
    execute_validations(validations)
=======
    # apply validation
    
    execute_validations(validations)
    # fetch entity path

    # load entity data
    
    # apply dq
    execution_plan_with_rule_df = merge_plans_with_rules(execution_plan_df,rule_master_df)
    dq_execution(execution_plan_with_rule_df,entity_data_df,spark)

>>>>>>> other-repo1/dq-framework-dev-202501

if __name__ == "__main__":
    main()


