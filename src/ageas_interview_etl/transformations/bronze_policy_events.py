"""
Author: Robert Meldrum
Date: 2025-08-11

Description:
Bronze Layer: Raw Policy Data
This module reads raw JSON policy event data.

"""

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
from pyspark.sql import functions as F
from pyspark import pipelines as dp


@dp.table(name="ageas.bronze_policy_event") 
def policy_event():
    
    """
    Description:
    Load raw policy event data from JSON source using Autoloader.
    
    Returns:
    DataFrame: Raw policy events and ingestion metadata.
    
    """
    
    # Define the schema for the policy_type field
    policy_type_schema = StructType([
                    StructField("type", StringType(), True),
                    StructField("brand", StringType(), True),
    ])

    #  Load JSON files with Autoloader to automaticly pick up new files and keep track of shcema changes
    invalid_json_df  = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.schemaLocation", "/Volumes/ageas/bronze/schema") \
        .load("/Volumes/ageas/bronze/files")
    
    # Fix the invalid JSON in the policy_type field by replacing single quotes with double quotes and none with null
    valid_json_df = invalid_json_df.withColumn(
        "policy_type",
        F.regexp_replace(  
            F.regexp_replace(F.col("policy_type"), "'", "\""),
            "None", 
            "\"null\""
        )
    )

    # Replace policy_type string with a valid JSON object
    valid_json_df = valid_json_df.withColumn(
        "policy_type_object",
        F.from_json(F.col("policy_type"), policy_type_schema)
    )

    valid_json_df = valid_json_df.selectExpr(
        "age_of_insured",
        "coverage_amount",
        "customer_id",
        "event_timestamp",
        "event_type",
        "policy_id",
        "policy_type_object.type AS policy_type",
        "policy_type_object.brand AS policy_brand",
        "premium_amount",
        "region"
    )           
                    
    # Add ingestion metadata
    valid_json_df = valid_json_df.withColumn("ingestion_date_time", F.current_timestamp()).withColumn(
        "source_system", F.lit("policy_events_api")
    )
    
    return valid_json_df
