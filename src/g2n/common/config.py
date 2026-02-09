"""Environment-based configuration using Pydantic Settings."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_PROJECT_ROOT = Path(__file__).resolve().parents[3]


class S3Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="G2N_S3_")

    bronze_path: str = "s3a://g2n-data/bronze/"
    silver_path: str = "s3a://g2n-data/silver/"
    quarantine_path: str = "s3a://g2n-data/quarantine/"
    features_path: str = "s3a://g2n-data/features/"


class SnowflakeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="G2N_SNOWFLAKE_")

    account: str = ""
    user: str = ""
    warehouse: str = "G2N_WH"
    database: str = "G2N_GOLD"
    schema_: str = Field("PUBLIC", alias="G2N_SNOWFLAKE_SCHEMA")
    role: str = "G2N_ROLE"


class CollibraSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="G2N_COLLIBRA_")

    base_url: str = ""
    username: str = ""
    community_id: str = ""


class MLflowSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="G2N_MLFLOW_")

    tracking_uri: str = "http://localhost:5000"
    experiment_name: str = "g2n-default"


class ServingSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="G2N_SERVING_")

    host: str = "0.0.0.0"
    port: int = 8000


class G2NSettings(BaseSettings):
    """Top-level settings aggregating all sub-configs."""

    model_config = SettingsConfigDict(
        env_file=str(_PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    env: str = Field("dev", alias="G2N_ENV")

    s3: S3Settings = S3Settings()
    snowflake: SnowflakeSettings = SnowflakeSettings()
    collibra: CollibraSettings = CollibraSettings()
    mlflow: MLflowSettings = MLflowSettings()
    serving: ServingSettings = ServingSettings()


@lru_cache(maxsize=1)
def get_settings() -> G2NSettings:
    """Return cached singleton settings instance."""
    return G2NSettings()
