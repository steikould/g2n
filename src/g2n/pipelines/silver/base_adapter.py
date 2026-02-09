"""Abstract base class for source-specific Silver layer adapters.

Each data source (EDI 867, EDI 844, ERP, etc.) gets a dedicated adapter that
normalizes its raw schema to the canonical Silver model. This is where
source-specific format differences are resolved.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from g2n.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

logger = get_logger(__name__)


class BaseSilverAdapter(ABC):
    """Abstract adapter for transforming Bronze data to canonical Silver schema."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    @property
    @abstractmethod
    def entity_name(self) -> str:
        """Logical entity name (used for S3 paths and logging)."""

    @property
    @abstractmethod
    def target_schema(self) -> StructType:
        """The canonical Silver schema this adapter normalizes to."""

    @abstractmethod
    def normalize(self, df: DataFrame) -> DataFrame:
        """Normalize source-specific columns to canonical schema.

        This method handles renaming, type casting, and mapping source-specific
        codes to standard values.
        """

    @abstractmethod
    def validate(self, df: DataFrame) -> DataFrame:
        """Apply source-specific validation rules.

        Returns the DataFrame with invalid records flagged (not removed).
        """

    def transform(self, df: DataFrame) -> DataFrame:
        """Full transformation pipeline: normalize → validate.

        Override in subclasses to add source-specific intermediate steps.
        """
        logger.info("silver_transform_start", entity=self.entity_name)
        df = self.normalize(df)
        df = self.validate(df)
        logger.info("silver_transform_complete", entity=self.entity_name)
        return df
