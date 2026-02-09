# Copilot Instructions for G2N (Gross-to-Net Revenue Optimization)

## Project Overview

This repository contains the architecture and design documentation for the **Gross-to-Net (G2N) Optimization Initiative** — a multi-phase ML/AI effort to improve revenue realization in the Animal Pharmaceutical division by forecasting deductions (rebates, chargebacks, returns) and detecting leakage.

**Key constraint:** On-premises infrastructure only (S3 + Databricks + Snowflake + OpenShift, no cloud-native services).

---

## Architecture at a Glance

```
Source Systems (EDI, ERP, Contracts, Claims)
    ↓
Bronze/Silver (S3) — Data Quality Gates via Great Expectations + Collibra API
    ↓
Gold (Snowflake) & Feature Layer (Databricks Delta on S3) — Parallel paths
    ↓
Databricks ML (model training, MLflow tracking)
    ↓
OpenShift APIs + Snowflake batch outputs + Streamlit/Angular UIs
```

**Critical organizational design:** Gold Data Products team (Snowflake) and AI CoE (Databricks) are separate domains. Gold produces governed datasets for BI. AI CoE builds ML features iteratively on Silver-layer data. Both register with Collibra for governance.

---

## Essential Patterns

### 1. **Data Quality as First-Class Design**

All G2N source data has known quality issues (schema drift, duplicates, timeliness gaps, master data staleness). The architecture enforces quality gates at Stage 2 (Silver ingestion):

- **Great Expectations** runs in Databricks pipelines (via `src/g2n/quality/expectations/`) to validate completeness, uniqueness, consistency, timeliness, accuracy, and validity.
- **Collibra API integration** (`g2n.quality.collibra_sync`) pulls active quality rules at runtime via `pull_active_rules(domain_id)` and pushes results back via `push_check_results()` for governance audit.
- **Quality scoring:** `g2n.quality.scoring` provides composite quality score (0–100) that propagates through features to model training. Models can weight samples by quality score rather than hard-filtering bad data.
- **Quarantine pattern:** Critical failures → S3 quarantine zone (`G2N_S3_QUARANTINE_PATH`); non-critical failures flagged as metadata columns.

**When working on data pipelines:** Import `collibra_sync` to pull rules, use Great Expectations configs in `config/great_expectations/`, and call `g2n.quality.scoring` functions. See [pipelines/silver/](src/g2n/pipelines/silver/) for examples.

### 2. **The Feature Engineering Layer (Not a Full Feature Store)**

No centralized Feature Store; instead, a pragmatic intermediate: shared Delta tables on S3 (Databricks), registered in Collibra.

| Aspect | Responsibility | Implementation |
|--------|--------|--------|
| **Discovery** | Data scientist explores Silver and Gold data in Databricks notebooks | Use `g2n.features.sdk` and Collibra Feature Registry |
| **Development** | Iterative feature logic tested locally; once validated, registered in Collibra | Implement in `src/g2n/features/domains/` |
| **Materialization** | Scheduled Databricks Job → Delta table (S3 Silver layer) | Entry point in `notebooks/pipelines/run_feature_compute.py` calls `g2n.features.store` |
| **Consumption** | Other teams query Feature Registry in Collibra, consume from shared Delta tables | Import from `g2n.features.sdk` |
| **Monitoring** | Streamlit dashboard tracks feature drift, nulls, freshness; alerts on anomalies | See `src/g2n/dashboards/feature_quality.py` |

**Planned feature domains:** Implemented in `src/g2n/features/domains/` (customer_behavior, contract_complexity, rebate_dynamics, seasonal_market, quality_signals).

**When developing features:** (1) Build and test in Databricks notebook, (2) implement logic in `src/g2n/features/domains/`, (3) register via `FeatureRegistry.register()` in `g2n.features.registry`, (4) test with `tests/unit/test_features/test_domains/`, (5) add materialization step to `notebooks/pipelines/run_feature_compute.py`.

### 3. **Gold Data Products vs. Feature Engineering**

This is the key organizational pattern. Two paths, both legitimate:

- **Path A (Gold Data Products):** Need a governed, curated dataset (e.g., complete rebate history)? Submit a formal intake request to the Gold team in Snowflake.
- **Path B (Feature Engineering):** Need to iterate on transformations, test hypotheses, or build ML-specific aggregations? Work directly on Silver data in Databricks, register validated features in the Feature Engineering Layer.

**When to use which:** Gold products for highly curated, stable, audit-trail data (e.g., reconciled transactions, active contracts). Feature layer for ML-specific transformations, experimentation, and iteration.

### 4. **API-First Integration**

No direct database-to-database coupling. All integrations are API-mediated:

- **Databricks ↔ Snowflake:** Spark connector via `g2n.common.connectors.snowflake` (read gold products, write scores)
- **Databricks ↔ Collibra:** REST API via `g2n.common.connectors.collibra.CollibraClient` (read DQ rules, write feature metadata, register lineage)
- **S3 Access:** Via `g2n.common.connectors.s3` for Bronze/Silver/Features paths
- **Model serving:** FastAPI on OpenShift (`src/g2n/serving/app.py`) with REST APIs for real-time predictions
- **Data product requests:** Formal intake (JIRA/ServiceNow ticket to Gold team)

**When integrating:** Use connector clients from `g2n.common.connectors`, not direct SDKs. Configuration is environment-driven (`.env` file matching settings in `g2n.common.config`). Document all external dependencies in the feature/model metadata.

**Key connectors:**
- `CollibraClient().get_dq_rules(domain_id)` — Fetch governance rules
- `CollibraClient().register_feature(...)` — Register feature with lineage
- `SnowflakeConnector().read_table()` / `.write_table()` — Gold data access
- `S3Connector().read_parquet(path)` / `.write_delta(path)` — Feature store access

### 5. **Model Governance & Lineage**

All models are SOX-compliant and fully auditable:

- **MLflow Model Registry** tracks version history, training datasets (with Collibra lineage links), and performance metrics. All models inherit from `BaseG2NModel` in `src/g2n/models/base.py`.
- **Promotion workflow:** staging → production requires documented approval, automated test passing (Bitbucket CI/CD in `infrastructure/ci/bitbucket-pipelines.yml`), and Collibra impact assessment.
- **Output audit columns:** Use `g2n.common.audit.add_audit_columns()` to append model version, scoring timestamp, input feature versions to scored outputs.
- **Collibra lineage chain:** Source system → Bronze → Silver (with DQ metadata) → Gold/Feature → Model (logged in MLflow) → Scored output (Snowflake).

**When building models:** 
- Inherit from `BaseG2NModel` and implement `train()`, `predict()`, `validate()` methods
- Log to MLflow with dataset and feature references via `mlflow.log_*()` 
- Generate a `ModelCard` with use case, training dataset, feature versions, and limitations
- Call `add_audit_columns()` before writing scored outputs to Snowflake
- Write unit tests in `tests/unit/test_models/` and integration tests in `tests/integration/`

---

---

## Project Structure & Key Files

```
src/g2n/
  ├── common/               # Shared utilities
  │   ├── connectors/       # API clients (Collibra, Snowflake, S3)
  │   ├── config.py         # Pydantic Settings for env-based config
  │   ├── audit.py          # SOX audit column helpers
  │   └── logging.py        # Structured logging
  ├── quality/              # Data quality & governance
  │   ├── collibra_sync.py  # Pull DQ rules, push results to Collibra
  │   ├── scoring.py        # Quality score computation
  │   └── expectations/     # Great Expectations configs
  ├── features/             # Feature Engineering Layer
  │   ├── domains/          # Business feature domains
  │   ├── registry.py       # Feature Registry (Collibra interface)
  │   ├── store.py          # Delta materialization
  │   └── sdk.py            # Discovery and consumption API
  ├── models/               # ML models (all inherit BaseG2NModel)
  │   ├── accrual_forecast/
  │   ├── leakage_detection/
  │   ├── deduction_classification/
  │   ├── contract_optimization/
  │   ├── returns_prediction/
  │   └── base.py           # Abstract base with MLflow integration
  ├── pipelines/            # ETL stages
  │   ├── bronze/           # Raw ingestion
  │   ├── silver/           # Cleansing + DQ gates
  │   └── gold/             # Data product preparation
  ├── dashboards/           # Streamlit apps
  │   ├── feature_quality.py
  │   ├── accrual_health.py
  │   ├── leakage_console.py
  │   └── model_governance.py
  └── serving/              # FastAPI on OpenShift
      ├── app.py            # FastAPI application
      ├── routers/          # Route handlers
      └── middleware/       # Audit middleware

notebooks/
  ├── exploration/          # EDA notebooks (thin wrappers)
  ├── pipelines/            # Databricks Job entry points
  └── models/               # Model training entry points

tests/
  ├── unit/                 # Fast, isolated tests
  ├── integration/          # End-to-end pipeline tests
  ├── fixtures/             # Sample data (CSV, JSON)
  └── conftest.py           # Shared pytest fixtures (mock clients)

config/
  ├── environments/         # dev.yaml, staging.yaml, prod.yaml
  ├── collibra/             # Field mapping definitions
  └── great_expectations/   # GX suite configs and expectations

infrastructure/
  ├── ci/                   # bitbucket-pipelines.yml (lint → test → build)
  ├── docker/               # Dockerfiles for dashboards & serving
  └── openshift/            # K8s deployments (base + overlays)
```

### Core Dependencies

**Base:** Pydantic 2.0+ (config validation), PyYAML (YAML parsing), structlog (structured logging), httpx (async HTTP)

**Optional groups** (install as needed):
- `[spark]` — PySpark 3.4+, Delta Lake
- `[quality]` — Great Expectations 0.18+
- `[ml]` — scikit-learn, XGBoost, MLflow, pandas, numpy
- `[serving]` — FastAPI, Uvicorn (OpenShift model endpoints)
- `[dashboards]` — Streamlit, Plotly, Snowflake connector
- `[dev]` — pytest, ruff, mypy, pre-commit

Install all: `pip install -e ".[all]"`

### Environment Configuration

1. Copy `.env.example` to `.env` (never commit `.env`)
2. Fill in credentials:
   - `G2N_S3_*` — S3 bucket paths for medallion layers
   - `G2N_SNOWFLAKE_*` — Snowflake account, user, warehouse, database
   - `G2N_COLLIBRA_*` — Collibra REST endpoint, credentials, community ID
   - `G2N_MLFLOW_*` — MLflow tracking URI and experiment name
   - `G2N_SERVING_*` — API host/port

Settings are loaded at runtime by `g2n.common.config.get_settings()`.

---

## Development Workflows

### Building a G2N Feature

1. **Explore** in Databricks notebook (access Silver via S3, Gold via Snowflake connector).
2. **Validate** for predictive signal using historical data.
3. **Register** feature definition in Collibra API with: business definition, SQL/PySpark logic, source lineage, quality expectations.
4. **Materialize** as scheduled Databricks Job → Delta table (S3 Silver layer).
5. **Monitor** using Streamlit feature quality dashboard; alert on drift.

### Training a G2N Model

1. **Discover features** from Collibra Feature Registry or Streamlit Feature Dashboard.
2. **Build dataset** in Databricks, joining features + gold products + targets. Include data quality scores for optional sample weighting.
3. **Log to MLflow** with dataset artifact, feature versions, and hyperparameters.
4. **Validate** in staging environment; document approval.
5. **Promote** to production via Bitbucket CI/CD, with output audit columns written to Snowflake.

### Pushing Features to Collibra

```python
# Pseudocode – actual SDK varies
from collibra_sdk import AssetAPI

api = AssetAPI(base_url="<collibra_endpoint>")
api.register_feature(
    name="customer_rebate_velocity_90d",
    business_definition="Rolling 90-day sum of rebate claims by customer, normalized by sales",
    computation_sql="SELECT customer_id, DATE_TRUNC('day', claim_date) AS period, ...",
    input_lineage=["silver.rebate_claims", "gold.sales_master"],
    version="1.0.0",
    owner="AI CoE",
    quality_rules=["null_rate < 0.01", "distribution_drift < 0.1"]
)
```

---

## Testing & CI/CD

### Running Tests Locally

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Unit tests only (fast)
pytest tests/unit/ -v --cov=g2n

# Integration tests (requires Spark, S3, Snowflake access)
pytest tests/integration/ -v

# Run specific test file
pytest tests/unit/test_quality/test_scoring.py -v
```

### Linting & Type Checking

```bash
# Lint with Ruff
ruff check src/

# Type checking with mypy
mypy src/g2n/

# Both (Bitbucket CI/CD pipeline)
ruff check src/ && mypy src/g2n/ && pytest tests/unit/ -v --cov=g2n
```

### Bitbucket CI/CD Pipeline

The pipeline in `infrastructure/ci/bitbucket-pipelines.yml` runs on every push:
1. **Lint & Type Check** — Ruff + mypy
2. **Unit Tests** — pytest with coverage report
3. **Build** — Package as Python wheel (on main branch)
4. **Deploy** — To staging/prod via Databricks Jobs API and OpenShift (on tagged releases)

---

## Common Commands

| Task | Command |
|------|---------|
| **Feature Development** | Edit `src/g2n/features/domains/`, test locally, register via `FeatureRegistry.register()` |
| **Model Training** | Inherit from `BaseG2NModel`, train in Databricks notebook, log to MLflow with `ModelCard` |
| **Quality Check** | Pull rules via `collibra_sync.pull_active_rules()`, run Great Expectations suite, push results via `push_check_results()` |
| **Audit Columns** | Call `g2n.common.audit.add_audit_columns()` before writing scored output to Snowflake |
| **S3 Access** | Use `S3Connector` from `g2n.common.connectors.s3`, not direct boto3 |
| **Snowflake Query** | Use `SnowflakeConnector` from `g2n.common.connectors.snowflake` (reads/writes via Spark) |
| **Config Values** | Call `get_settings()` from `g2n.common.config` — all env vars automatically loaded |

---

## Key Files & Directories

- **[DESIGN.md](DESIGN.md)** — Complete architecture, data flows, use cases, roadmap, and risk mitigation. Start here for context.
- **[README.md](README.md)** — Project overview and structure.
- **[pyproject.toml](pyproject.toml)** — Package definition, optional dependency groups (`[spark]`, `[quality]`, `[ml]`, `[serving]`, `[dashboards]`, `[dev]`).
- **[.env.example](.env.example)** — Environment configuration template (copy to `.env`).
- **[infrastructure/ci/bitbucket-pipelines.yml](infrastructure/ci/bitbucket-pipelines.yml)** — CI/CD pipeline: lint → test → build → deploy.
- **[src/g2n/common/config.py](src/g2n/common/config.py)** — Pydantic Settings for all env-based configuration.
- **[src/g2n/quality/collibra_sync.py](src/g2n/quality/collibra_sync.py)** — Pull/push DQ rules to Collibra.
- **[src/g2n/features/registry.py](src/g2n/features/registry.py)** — Feature Registry interface (Collibra API).
- **[src/g2n/common/audit.py](src/g2n/common/audit.py)** — SOX audit column helpers.
- **[src/g2n/models/base.py](src/g2n/models/base.py)** — Abstract `BaseG2NModel` with MLflow integration and `ModelCard`.
- **[src/g2n/serving/app.py](src/g2n/serving/app.py)** — FastAPI application entry point for model serving.
- **[tests/conftest.py](tests/conftest.py)** — Shared pytest fixtures (mock Collibra client, sample data).

---

## Use Cases (Priority Order)

1. **Accrual Forecast Improvement** (Est. $5–20M impact, 3–6 months): ML models to forecast rebate/chargeback/return liabilities at SKU/channel/customer level.
2. **Rebate Leakage Detection** (1–3% deductions recovery, 3–4 months): Anomaly detection comparing contractual terms to actual payments.
3. **Deduction Auto-Classification** (40–60% manual effort reduction, 4–6 months): NLP/classification for auto-categorizing distributor remittance data.
4. **Contract Optimization** (0.5–1% net price improvement, 6–9 months): What-if simulation engine.
5. **Returns Prediction** (10–20% reserve tightening, 3–5 months): Predict return propensity by product/channel/region.

---

## Critical Data Quality Dimensions for G2N

Monitor these dimensions continuously:

| Dimension | G2N Example | Check |
|-----------|------------|-------|
| **Completeness** | All distributor feeds received; no missing periods | Great Expectations suite in Silver ETL |
| **Uniqueness** | No duplicate chargeback claims | Dedup + fuzzy matching logic |
| **Consistency** | Customer IDs resolve across ERP, contracts, distributor feeds | Cross-source referential integrity |
| **Timeliness** | Claims arriving within SLA; staleness scoring | Metadata timestamp profiling |
| **Accuracy** | Contract tiers, discounts within expected ranges | Range checks, outlier detection |
| **Validity** | EDI codes valid; dates logical; amounts positive | Schema & business rule validation |

---

## Frequently Encountered Concepts

- **Medallion Architecture:** Bronze (raw) → Silver (cleansed + quality metadata) → Gold (curated, Snowflake).
- **G2N:** Gross sales minus all deductions (rebates, chargebacks, returns, prompt pay discounts, etc.).
- **Tier reset:** Periodic recalculation of rebate tier eligibility (e.g., quarterly, annually).
- **Claim submission lag:** Delay between qualifying transaction and rebate claim arrival.
- **Distributor error rate:** Feature aggregating duplicate/invalid claims by distributor to identify problem partners.

---

## Common Pitfalls to Avoid

1. **Asking the Gold Data Products team to build ML features.** They own governed datasets; you own feature engineering. Submit intake requests for gold products only.
2. **Building features without Collibra registration.** Creates undiscoverable technical debt. Register all features before materialization.
3. **Hardcoding data quality thresholds.** Pull rules from Collibra API at runtime; allows governance updates without code changes.
4. **Ignoring temporal alignment.** G2N data has complex timeliness issues (rebate claims arrive weeks late). Always validate grain and temporal windows explicitly.
5. **Skipping audit columns on model outputs.** SOX compliance requires full traceability. Always include model version, timestamp, input feature versions in scored tables.

---

## Next Steps for New Contributors

1. Read [DESIGN.md](DESIGN.md) thoroughly (Sections 1–4 for architecture, 6–8 for your specific domain).
2. Review the use case that matches your assigned work (Section 7).
3. Identify data sources and quality rules needed (Section 6).
4. If building features, follow the Feature Engineering Layer lifecycle (Section 8.1).
5. If building models, ensure MLflow logging and audit columns (Section 9.2).
