"""
Author: Robert Meldrum
Date: 2026-08-12

Description:
Gold Layer: Policy Event Star Schema
This module builds Kimball-style dimensions and facts from ageas.silver_policy_event.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark import pipelines as dp


@dp.table(name="ageas.gold_dim_customer_detail")
def dim_customer_detail():
    df = spark.table("ageas.silver_policy_event")

    df_customers = (
        df.select(
            F.col("customer_id").alias("bk_customer_detail_id"),
            F.col("age_of_insured"),
            F.col("age_of_insured_band"),
            F.col("region"),
        )
        .distinct()
        .withColumn(
            "sk_customer_detail_id",
            F.row_number().over(Window.orderBy("bk_customer_detail_id")),
        )
        .select(
            "sk_customer_detail_id",
            "bk_customer_detail_id",
            "age_of_insured",
            "region",
        )
    )

    return df_customers


@dp.table(name="ageas.gold_dim_policy_detail")
def dim_policy_detail():
    df = spark.table("ageas.silver_policy_event")

    df_policy_details = (
        df.select(
            F.col("policy_id").alias("bk_policy_id"),
            F.col("policy_type").alias("type"),
            F.col("policy_brand").alias("brand"),
        )
        .distinct()
        .withColumn(
            "sk_policy_detail_id",
            F.row_number().over(Window.orderBy("bk_policy_id")),
        )
        .select(
            "sk_policy_detail_id",
            "bk_policy_id",
            "type",
            "brand",
        )
    )

    return df_policy_details


@dp.table(name="ageas.gold_fact_policy")
def fact_policy():
    df = spark.table("ageas.silver_policy_event")
    
    dim_policy = spark.table("ageas.gold_dim_policy_detail")
    dim_customer = spark.table("ageas.gold_dim_customer_detail")

    df_policy_fact = (
        df.groupBy("policy_id")
        .agg(
            F.first("premium_amount").alias("premium_amount"),
            F.first("coverage_amount").alias("coverage_amount"),
            F.lit(1).alias("count"),
        )
        .join(dim_policy, F.col("policy_id") == F.col("bk_policy_id"), "left")
        .join(dim_customer, F.col("customer_id") == F.col("bk_customer_detail_id"), "left")
        .select(
            F.monotonically_increasing_id().alias("sk_policy_id"),
            "sk_policy_detail_id",
            "sk_customer_detail_id",
            "premium_amount",
            "coverage_amount",
            "count",
        )
    )

    return policy_fact


@dp.table(name="ageas.fact_event")
def fact_event():
    df = spark.table("ageas.silver_policy_event")
    dim_policy = spark.table("ageas.dim_policy_detail")
    dim_customer = spark.table("ageas.dim_customer_detail")

    event_fact = (
        df.join(dim_policy, F.col("policy_id") == F.col("bk_policy_id"), "left")
        .join(dim_customer, F.col("customer_id") == F.col("bk_customer_detail_id"), "left")
        .withColumn("event_count", F.lit(1))
        .select(
            F.monotonically_increasing_id().alias("sk_event_id"),
            "sk_policy_detail_id",
            "sk_customer_detail_id",
            F.col("event_timestamp").alias("date_timestamp"),
            "event_type",
            "event_count",
        )
    )

    return event_fact
