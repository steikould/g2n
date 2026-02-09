# G2N — Gross-to-Net Revenue Optimization Platform

AI Center of Excellence platform for optimizing the gross-to-net revenue spread
(rebates, chargebacks, distribution fees, returns, discounts) in the Animal
Pharmaceutical division.

## Architecture

See [DESIGN.md](DESIGN.md) for the full architecture and design document.

**Data Flow:** Source Systems → Bronze (S3) → Silver (S3 + DQ Gate) → Gold (Snowflake) + Feature Layer (Databricks Delta) → ML Models → Serving (OpenShift) + Dashboards (Streamlit)

## Project Structure

```
src/g2n/
├── common/         Shared config, logging, audit, connectors (Snowflake, Collibra, S3)
├── pipelines/      Bronze ingestion, Silver adapters (EDI 867/844/849, ERP, contracts, claims), quality gate
├── quality/        Data quality framework (6 dimensions), composite scoring (0-100), Collibra sync
├── features/       Feature Engineering Layer — registry, Delta store, SDK, monitoring, 5 domains
├── models/         ML models — accrual forecast, leakage detection, deduction classification,
│                   contract optimization, returns prediction
├── serving/        FastAPI app for real-time inference on OpenShift
└── dashboards/     Streamlit apps — accrual health, leakage console, feature quality, model governance
```

## Setup

```bash
# Install in development mode with all dependencies
pip install -e ".[all]"

# Or install specific groups
pip install -e ".[spark,ml]"       # For Databricks development
pip install -e ".[serving]"        # For API serving
pip install -e ".[dashboards]"     # For Streamlit dashboards
pip install -e ".[dev]"            # For development tools (pytest, ruff, mypy)
```

## Configuration

1. Copy `.env.example` to `.env` and fill in environment-specific values
2. Environment configs are in `config/environments/{dev,staging,prod}.yaml`
3. Collibra field mappings: `config/collibra/field_mappings.yaml`
4. Great Expectations suites: `config/great_expectations/expectations/`

## Running

```bash
# Run tests
pytest tests/unit/ -v

# Lint
ruff check src/

# Type check
mypy src/g2n/

# Start model serving API
uvicorn g2n.serving.app:app --reload

# Start a Streamlit dashboard
streamlit run src/g2n/dashboards/accrual_health.py
```

## G2N Use Cases

| Use Case | Model Type | Status |
|----------|-----------|--------|
| Accrual Forecast Improvement | Gradient Boosting Regressor | Phase 1 |
| Rebate Leakage Detection | Isolation Forest | Phase 2 |
| Deduction Auto-Classification | TF-IDF + Logistic Regression | Phase 2 |
| Contract Optimization | Simulation Engine | Phase 3 |
| Returns Prediction | Random Forest Classifier | Phase 2 |

## Infrastructure

- **Dockerfiles:** `infrastructure/docker/`
- **OpenShift (Kustomize):** `infrastructure/openshift/`
- **CI/CD (Bitbucket Pipelines):** `infrastructure/ci/bitbucket-pipelines.yml`
