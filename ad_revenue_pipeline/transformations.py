from pyspark.sql import Window
import pyspark.sql.functions as F

from ad_revenue_pipeline.config import TOP_N_ADVERTISERS


def build_joined(impressions, clicks):
    """Left join impressions with clicks.

    The click id is renamed to ``click_id`` so we don't end up with two
    different ``id`` columns after the join."""
    clicks_renamed = clicks.withColumnRenamed("id", "click_id")
    return impressions.join(
        clicks_renamed,
        impressions["id"] == clicks_renamed["impression_id"],
        "left",
    )


def compute_app_country_metrics(joined):
    return (
        joined
        .groupBy("app_id", "country_code")
        .agg(
            F.countDistinct("id").alias("total_impressions"),
            F.count("click_id").alias("total_clicks"),
            F.round(
                F.sum(F.coalesce(F.col("revenue"), F.lit(0.0))), 2
            ).alias("total_revenue"),
        )
        .withColumn(
            "click_through_rate",
            F.round(F.col("total_clicks") / F.col("total_impressions"), 4),
        )
        .orderBy("app_id", "country_code")
    )


def compute_top_n_advertisers(joined, n=TOP_N_ADVERTISERS):
    """Pick the top advertisers within each (app, country) by revenue per impression."""
    adv_stats = (
        joined
        .groupBy("app_id", "country_code", "advertiser_id")
        .agg(
            F.countDistinct("id").alias("impressions"),
            F.sum(F.coalesce(F.col("revenue"), F.lit(0.0))).alias("revenue"),
        )
        .withColumn(
            "rev_per_impression",
            F.round(F.col("revenue") / F.col("impressions"), 4),
        )
    )

    w = Window.partitionBy("app_id", "country_code").orderBy(
        F.desc("rev_per_impression"), F.desc("revenue"),
    )

    ranked = (
        adv_stats
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= n)
    )

    return (
        ranked
        .groupBy("app_id", "country_code")
        .agg(
            F.collect_list(
                "advertiser_id"
            ).alias("recommended_advertisers_ids")
        )
        .orderBy("app_id", "country_code")
    )


def compute_median_user_spend(joined):
    """Two-step aggregation for user spend.

    First compute total spend per user, then aggregate again per country
    to get the median and average."""
    user_spend = (
        joined
        .groupBy("country_code", "user_id")
        .agg(
            F.round(
                F.sum(F.coalesce(F.col("revenue"), F.lit(0.0))), 2
            ).alias("total_spend"),
        )
    )

    return (
        user_spend
        .groupBy("country_code")
        .agg(
            F.round(F.median("total_spend"), 2).alias("median_user_spend"),
            F.round(F.avg("total_spend"), 2).alias("avg_user_spend"),
            F.count("user_id").alias("unique_users"),
        )
        .orderBy("country_code")
    )
