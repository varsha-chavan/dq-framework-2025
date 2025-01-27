from common.utils import fetch_execution_plan
from utilities.apply_rules import apply_rules

def dq_execution(execution_plan_df, entity_data_df, rule_master_df):
    """
    In this function we will apply the dq on the actual data.

    args:
    execution_plan_df : dataframe that contains plan information for a entity.
    entity_data_df: dataframe on which we have to apply the dq.
    rule_master_df: to fetch names of the rules.

    Steps:
        1. Call the fetch_execution_plan() function.
            pass the execution_plan_df to function.
            This return the list of tuples of plans e.g. execution_plan_list[(rule_id, column_name, paramaters, is_critical, etc.)]
        2. Call the apply_rules() function.
            Pass the entity_data_df, rule_master_df,execution_plan_list to function.
            This function returns the track_list of rules passed/failed on the entity.
        
        3. count the passed and failed rules from track_list.(1= pass , 0= fail)
        
        4. print how many rules passed from total rules in list. Also log the result.
            
    """

