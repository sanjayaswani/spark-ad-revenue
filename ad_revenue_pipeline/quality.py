import logging

import pyspark.sql.functions as F

logger = logging.getLogger(__name__)


def check_empty(df, label):
    if len(df.head(1)) == 0:
        return [f"{label}: dataset is empty"]
    return []


def dedup(df, key_col, label):
    before = df.count()
    df = df.dropDuplicates([key_col])
    after = df.count()
    dropped = before - after
    if dropped > 0:
        logger.info("%s: dropped %d duplicate rows on '%s'", label, dropped, key_col)
    return df


def handle_neg_rev(df):
    return df.withColumn(
        "revenue",
        F.when(F.col("revenue") < 0, 0.0).otherwise(F.col("revenue")),
    )


def run_all_checks(impressions, clicks):
    errs = []

    # bail early on empty -- everthing else will just blow up
    errs.extend(check_empty(impressions, "impressions"))
    errs.extend(check_empty(clicks, "clicks"))

    return errs
