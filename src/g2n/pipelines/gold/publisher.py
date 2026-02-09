"""Gold layer publisher — writes model outputs and scored results to Snowflake."""

from __future__ import annotations

from typing import TYPE_CHECKING

from g2n.common.audit import add_audit_columns
from g2n.common.connectors.snowflake import write_table
from g2n.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = get_logger(__name__)


def publish_scores(
    df: DataFrame,
    table: str,
    *,
    model_version: str,
    correlation_id: str | None = None,
    mode: str = "append",
) -> None:
    """Publish model scores to a Snowflake Gold table with audit columns.

    Args:
        df: Scored DataFrame to publish.
        table: Target Snowflake table name.
        model_version: MLflow model version for audit traceability.
        correlation_id: Pipeline correlation ID.
        mode: Write mode (append, overwrite).
    """
    audited = add_audit_columns(
        df,
        model_version=model_version,
        correlation_id=correlation_id,
        pipeline_name="gold_publisher",
    )

    write_table(audited, table, mode=mode)
    logger.info(
        "gold_publish_complete",
        table=table,
        model_version=model_version,
        record_count=df.count(),
    )
