"""Shared enumerations for the G2N platform."""

from __future__ import annotations

from enum import Enum


class DeductionType(str, Enum):
    """Types of deductions in the G2N spread."""

    REBATE = "rebate"
    CHARGEBACK = "chargeback"
    DISTRIBUTION_FEE = "distribution_fee"
    RETURN = "return"
    PROMPT_PAY = "prompt_pay"
    ADMIN_FEE = "admin_fee"
    OTHER = "other"


class QualityDimension(str, Enum):
    """Data quality dimensions monitored in the DQ framework."""

    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    ACCURACY = "accuracy"
    VALIDITY = "validity"


class MedallionLayer(str, Enum):
    """Medallion architecture layers."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    FEATURES = "features"


class ModelStage(str, Enum):
    """MLflow model promotion stages."""

    EXPERIMENTAL = "experimental"
    STAGING = "staging"
    PRODUCTION = "production"
    ARCHIVED = "archived"


class ClaimStatus(str, Enum):
    """Chargeback claim statuses."""

    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    RESUBMITTED = "resubmitted"
    UNDER_REVIEW = "under_review"
