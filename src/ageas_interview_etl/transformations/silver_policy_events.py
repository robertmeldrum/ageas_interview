"""
Author: Robert Meldrum
Date: 2026-08-12

Description:
Silver Layer: Policy Event Transformations
This module reads policy event data from bronze.policy_event and applies
silver-level formatting and enrichment.
"""

from pyspark.sql import functions as F
from pyspark import pipelines as dp


@dp.table(name="ageas.silver_policy_event")
def policy_event():
    """
    Description:
    Read bronze.policy_event and apply silver-layer transformations.

    Returns:
    DataFrame: Policy events with cleaned text fields, decimal amounts,
    and a 10-year age band.
    """

    df = spark.table("ageas.bronze_policy_event")

    # Tidy up data typess and null values for text fields and decimal amounts
    df = df.withColumn(
        "policy_type",
        F.when(F.lower(F.col("policy_type")) == "null", None).otherwise(F.col("policy_type")),
    ).withColumn(
        "policy_band",
        F.when(F.lower(F.col("policy_band")) == "null", None).otherwise(F.col("policy_band")),
    ).withColumn(
        "coverage_amount",
        F.col("coverage_amount").cast("decimal(18,2)"),
    ).withColumn(
        "premium_amount",
        F.col("premium_amount").cast("decimal(18,2)"),
    )

    # Categorise age_of_insured into 10-year bands, with 100+ as a separate band
    age = F.col("age_of_insured").cast("int")
    age_band = F.when(age.isNull(), None).when(
        age >= 100,
        F.lit("100+"),
    ).otherwise(
        F.concat(
            (F.floor(age / 10) * 10).cast("string"),
            F.lit("-"),
            ((F.floor(age / 10) * 10) + 9).cast("string"),
        )
    )

    df = df.withColumn("age_of_insured_band", age_band)

    return df
