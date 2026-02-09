"""Snowflake Spark connector wrapper for Gold layer read/write."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from g2n.common.config import SnowflakeSettings, get_settings
from g2n.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)

SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"


def _sf_options(settings: SnowflakeSettings | None = None) -> dict[str, str]:
    """Build Snowflake connector options dict from settings."""
    cfg = settings or get_settings().snowflake
    return {
        "sfURL": f"{cfg.account}.snowflakecomputing.com",
        "sfUser": cfg.user,
        "sfWarehouse": cfg.warehouse,
        "sfDatabase": cfg.database,
        "sfSchema": cfg.schema_,
        "sfRole": cfg.role,
    }


def read_table(
    spark: SparkSession,
    table: str,
    *,
    settings: SnowflakeSettings | None = None,
    extra_options: dict[str, Any] | None = None,
) -> DataFrame:
    """Read a Snowflake table into a Spark DataFrame."""
    opts = _sf_options(settings)
    opts["dbtable"] = table
    if extra_options:
        opts.update(extra_options)

    logger.info("snowflake_read", table=table)
    return spark.read.format(SNOWFLAKE_SOURCE).options(**opts).load()


def write_table(
    df: DataFrame,
    table: str,
    *,
    mode: str = "append",
    settings: SnowflakeSettings | None = None,
) -> None:
    """Write a Spark DataFrame to a Snowflake table."""
    opts = _sf_options(settings)
    opts["dbtable"] = table

    logger.info("snowflake_write", table=table, mode=mode)
    df.write.format(SNOWFLAKE_SOURCE).options(**opts).mode(mode).save()
