# Databricks notebook source
def check_empty_dataframe(df, logger):
   """
    Validate whether the DataFrame contains any records.

    Args:
    df : The PySpark DataFrame to validate.
    logger : The custom logger instance to log messages.

    Returns:
    bool: True if the DataFrame is not empty, else False.
    
    Steps:
    1. Check if the DataFrame is Empty
       -Use the isEmpty() method to verify if the DataFrame has any records.
    2.Log an error if the DataFrame is empty:
      -Example:
       logger.error(f"Validation failed: The DataFrame {df} is empty.")
       return False
    3.Log a Success Message if the DataFrame is Not Empty:
      -Example:
       logger.debug(f"Validation passed: The dataframe {df} is not empty.")
       return True
    4.Handle Unexpected Errors with a Except Block
      -Example:
       except Exception as e:
       logger.error(f"Error in check_empty_dataframe: {str(e)}")
       return False
    
   """
    
def validate_nullable_constraint(df, column_constraints, logger):
   """
    Validate that columns defined as NOT NULL do not contain NULL values.

    Args:
    df : The PySpark DataFrame to validate.
    column_constraints: A dictionary mapping column names to their nullable status.
    logger : The custom logger instance to log messages.

    Returns:
    bool: True if all constraints are satisfied, else False.
    
    Steps:
    1.Iterate Through the Column Constraints
    2.Check for NOT NULL Columns
    3.Count the Number of NULL Values in the Column
    4.Log an Error and Return False if NULL Values Exist
      eg.logger.error(f"Validation failed: Column '{column}' contains {null_count} NULL values.")
         return False
    5.Log a Success Message and Return True if No NULL Values are Found
      eg.logger.debug("Validation passed: All NOT NULL constraints are satisfied.")
         return True
    6.Handle Unexpected Errors with a Except Block
      eg.except Exception as e:
         logger.error(f"Error in validate_nullable_constraint: {str(e)}")
         return False

   """
    
            
def check_primary_key_uniqueness(df, primary_keys, logger):
   """
    Validate that the primary key column in the DataFrame contains unique values.

    Args:
    df : The PySpark DataFrame to validate.
    primary_keys: The primary key columns to check.
    logger : The custom logger instance to log messages.

    Returns:
    bool: True if the primary key is unique, else False.
    
    Steps:
    1.Iterate Through the Primary Key Columns
    2.For each column listed in primary_keys, perform the uniqueness check.
    3.Count Distinct and Total Values for the Current Key
    4.Compare Distinct Count with Total Count.
      If the distinct_count is less than the total_count, it indicates that the primary key column contains duplicate values.
    5.Log an Error and Return False if Duplicates are Found
      eg:logger.error(f"Validation failed: Primary key '{key}' contains duplicate values.")
         return False
    6.Log a Success Message and Return True if All Keys are Unique
       eg:logger.debug("Validation passed: Primary key uniqueness is satisfied.")
          return True

   """
    
def validate_foreign_key_relationship(child_df, parent_df, column_name, logger):
   """
    Check if all values in the child DataFrame's column are present in the parent DataFrame's column.

    Args:
    child_df (DataFrame): The child DataFrame.eg: execution_plan_df
    parent_df (DataFrame): The parent DataFrame.eg:entity_data_df or rule_master_df
    column_name (str): The column name to check for the foreign key relationship.
    logger : The custom logger instance to log messages.

    Returns:
    bool: True if the foreign key relationship is valid, else False.
    
    Steps:
    1.Perform a Left Anti-Join to Identify Missing Foreign Key Values.
    2.Count the Missing Foreign Key Values.
    3.Log an Error and Return False if Missing Keys Exist.
       eg:logger.error(f"Validation failed: {missing_count} missing foreign key values in column '{column_name}'.")
       return False
    4.Log a Success Message and Return True if All Keys are Valid.
       eg.logger.debug(f"Validation passed: Foreign key relationship for column '{column_name}' is valid.")
       return True
    5.Handle Unexpected Errors with a Except Block.
       eg.except Exception as e:
       logger.error(f"Error in validate_foreign_key_relationship: {str(e)}")
       return False
   """