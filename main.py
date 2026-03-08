#!/usr/bin/env python3
"""
Entry point for the ad revenue pipeline.

    python3 main.py --impressions imp1.json imp2.json --clicks clicks.json
    spark-submit --master yarn main.py --impressions hdfs:///data/imp/ --clicks hdfs:///data/clicks/
"""

import argparse
import logging
import sys
import time

from pyspark.sql import SparkSession

from ad_revenue_pipeline.config import (
    SPARK_APP_NAME,
    SHUFFLE_PARTITIONS,
    BROADCAST_THRESHOLD_MB,
    IMPRESSIONS_SCHEMA,
    CLICKS_SCHEMA,
)
from ad_revenue_pipeline.reader import read_json, write_json
from ad_revenue_pipeline.quality import run_all_checks, handle_neg_rev, dedup
from ad_revenue_pipeline.transformations import (
    build_joined,
    compute_app_country_metrics,
    compute_top_n_advertisers,
    compute_median_user_spend,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ad_revenue_pipeline")


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Ad Revenue Pipeline")
    p.add_argument("--impressions", required=True, nargs="+",
                    help="Path to impression JSON files")
    p.add_argument("--clicks", required=True, nargs="+",
                    help="Path to click JSON files")
    p.add_argument("--output-dir", default="output", help="Root directory for all outputs")
    return p.parse_args(argv)


def create_spark_session():
    return (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config(
            "spark.sql.autoBroadcastJoinThreshold",
            BROADCAST_THRESHOLD_MB * 1024 * 1024,
        )
        .getOrCreate()
    )


def run(args):
    t0 = time.time()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading impressions from %s", args.impressions)
    impressions = read_json(spark, args.impressions, IMPRESSIONS_SCHEMA, "impressions")
    impressions = dedup(impressions, "id", "impressions")

    logger.info("Reading clicks from %s", args.clicks)
    clicks = read_json(spark, args.clicks, CLICKS_SCHEMA, "clicks")
    clicks = dedup(clicks, "id", "clicks")
    clicks = handle_neg_rev(clicks)

    # run DQ before any transformations
    logger.info("Running data-quality checks")
    errors = run_all_checks(impressions, clicks)

    for e in errors:
        logger.error("DQ ERROR: %s", e)

    if errors:
        logger.error("Aborting: %d data-quality error(s)", len(errors))
        spark.stop()
        sys.exit(1)

    # cache the joined df since all 3 aggregations read from it
    joined = build_joined(impressions, clicks)
    joined.cache()

    metrics = compute_app_country_metrics(joined)
    metrics.show(50, truncate=False)

    top_advertisers = compute_top_n_advertisers(joined)
    top_advertisers.show(50, truncate=False)

    median_spend = compute_median_user_spend(joined)
    median_spend.show(50, truncate=False)

    out = args.output_dir
    write_json(metrics, f"{out}/app_country_metrics")
    write_json(top_advertisers, f"{out}/top_advertisers")
    write_json(median_spend, f"{out}/median_user_spend")

    joined.unpersist()

    logger.info("Done in %.1f s", time.time() - t0)
    spark.stop()


if __name__ == "__main__":
    try:
        run(parse_args())
    except Exception:
        logger.exception("Pipeline failed")
        sys.exit(2)
