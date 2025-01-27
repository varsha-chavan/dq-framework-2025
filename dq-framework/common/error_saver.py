from common.constants import VAR_ERROR_RECORD_PATH

def save_error_records(error_record_df,entity_id):
    """
    Save the error records to a location.

    args:
    error_record_df: dataframe containing records failed for a rule.
    entity_id: to store the datframe at entity level at path location.

    Steps:
        1. Create the path to store the df
        2. write the df at s3 error records path partioned as Y/M/D/entity_id.
        3: return error_records_path

    Output:
    path: a variable containing the path where error records are stored
    """

