# Databricks notebook source

# Directory path containing JSON files
DIRECTORY_PATH = "dq-framework/metadata"

# Required table metadata
REQUIRED_TABLE_METADATA = {
    "dq_entity_master": "dq_entity_master.json",
    "df_rule_master": "dq_rule_master.json",
    "dq_execution_plan": "dq_execution_plan.json"
}


# Validation Configuration  required for apply_validation
VALIDATION_STEPS = [
    ("Column data type validation", "validate_column_data_types"),
    ("Nullable constraint validation", "validate_nullable_constraint"),
    ("Primary key uniqueness validation", "validate_primary_key_uniqueness")
]



