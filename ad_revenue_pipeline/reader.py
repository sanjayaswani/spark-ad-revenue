import logging

from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F   

logger = logging.getLogger(__name__)

_CORRUPT_COL = "_corrupt_record"


def _schema_with_corrupt_col(schema):
    return StructType(
        schema.fields + [StructField(_CORRUPT_COL, StringType(), True)]
    )


def read_json(spark, paths, schema, label):
    if isinstance(paths, str):
        paths = [paths]

    enforced_schema = _schema_with_corrupt_col(schema)

    logger.info("%s: reading from %d path(s): %s", label, len(paths), paths)

    raw = (
        spark.read
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", _CORRUPT_COL)
        .option("multiLine", True)
        .schema(enforced_schema)
        .json(paths)
        .cache()
    )

    total = raw.count()
    corrupt_count = raw.filter(F.col(_CORRUPT_COL).isNotNull()).count()

    if corrupt_count > 0:
        logger.warning(
            "%s: %d / %d rows are corrupt and will be excluded",
            label, corrupt_count, total,
        )

    clean = raw.filter(F.col(_CORRUPT_COL).isNull()).drop(_CORRUPT_COL)

    clean = clean.localCheckpoint(eager=True)
    raw.unpersist()

    return clean


def write_json(df, path, mode="overwrite"):
    df.write.mode(mode).json(path)
    logger.info("Wrote json -> %s", path)
