# AI Center of Excellence — Gross-to-Net Revenue Optimization

## Architecture & Design Document

*Addressing Data Quality, Feature Store Gaps, and Organizational Friction*

Animal Pharmaceutical Division | CONFIDENTIAL | February 2026

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Current-State Architecture & Constraints](#2-current-state-architecture--constraints)
3. [The Feature Store Gap & Organizational Friction](#3-the-feature-store-gap--organizational-friction)
4. [Target-State Architecture for G2N](#4-target-state-architecture-for-g2n)
5. [API & Integration Design](#5-api--integration-design)
6. [Data Quality Architecture for G2N](#6-data-quality-architecture-for-g2n)
7. [G2N Use Cases & Prioritization](#7-g2n-use-cases--prioritization)
8. [Feature Engineering Layer: Detailed Design](#8-feature-engineering-layer-detailed-design)
9. [Governance, Compliance & Auditability](#9-governance-compliance--auditability)
10. [Implementation Roadmap](#10-implementation-roadmap)
11. [Key Risks & Mitigations](#11-key-risks--mitigations)
12. [Conclusion](#12-conclusion)

---

## 1. Executive Summary

This document presents the architecture and design for the AI Center of Excellence (CoE) to deliver a firmwide Gross-to-Net (G2N) optimization initiative within the Animal Pharmaceutical division. G2N represents the spread between gross sales and net realized revenue after rebates, chargebacks, distribution fees, returns, prompt pay discounts, and other deductions. Even modest improvements in G2N forecasting accuracy and leakage detection translate to millions in recovered or preserved revenue.

> **⚠ Critical Design Constraints Addressed in This Document**
>
> 1. Multi-source rebate and deduction data with known quality issues requiring upstream profiling and remediation.
> 2. No centralized Feature Store; features are built project-by-project in Databricks with no firm-level reuse.
> 3. Organizational friction where the Gold Data Products team views their curated Snowflake outputs as final for ML/AI, when in reality data science requires iterative feature engineering on Silver-layer data.
> 4. All system integrations must go through APIs (Collibra, Snowflake, etc.) or formal data product request processes.

---

## 2. Current-State Architecture & Constraints

### 2.1 Platform Stack Overview

| Layer | Technology | Role in G2N |
|-------|-----------|-------------|
| Storage – Bronze/Silver | S3 (Medallion Architecture) | Raw and cleansed ingestion of sales, contracts, claims, chargeback, 867/844/849 EDI transaction data |
| Storage – Gold | Snowflake | Curated data products managed by dedicated team; serves as system of record for governed analytics |
| Compute / ML | Databricks | All ML/AI development, feature engineering, model training, batch inference, notebooks |
| Data Governance | Collibra | Data catalog, lineage, quality rules, business glossary, stewardship workflows |
| Deployment | OpenShift | Containerized model serving, microservices, API endpoints |
| Source Control / CI/CD | Bitbucket | Git repos, pipelines for model promotion dev → staging → prod |
| Front End | Angular / Streamlit | Angular for production apps; Streamlit for analyst-facing dashboards and rapid prototyping |

### 2.2 Non-Cloud Constraint

The infrastructure is on-premises with the sole cloud touchpoint being S3 buckets for the medallion architecture. This means all compute (Databricks), orchestration (OpenShift), and serving must operate within the on-premises perimeter. Architecture decisions must account for latency between S3 and on-prem Databricks/Snowflake, and cannot assume cloud-native auto-scaling or managed services.

### 2.3 The Data Quality Problem

G2N data originates from multiple heterogeneous sources: distributor EDI feeds (867/844/849), contract management systems, ERP (SAP/Oracle), claims and chargeback processors, buying group portals, and manual spreadsheet-based agreements. These sources exhibit the following known quality issues:

1. **Schema inconsistency:** Different distributors use different field mappings, codes, and formats for the same logical data (e.g., product identifiers, customer hierarchies).
2. **Timeliness gaps:** Some rebate claims arrive weeks or months after the qualifying transaction, creating temporal misalignment between accruals and actuals.
3. **Duplicate and phantom records:** Chargeback data frequently contains duplicates, re-submissions, and records referencing invalid contracts or expired pricing.
4. **Master data drift:** Customer and product hierarchies change over time (vet clinic acquisitions, buying group membership changes) and are not always reflected across all source systems simultaneously.
5. **Manual entry errors:** Contract terms for smaller accounts are often maintained in spreadsheets, introducing transcription errors in tier thresholds, discount percentages, and effective dates.

> **Why This Matters for ML/AI**
>
> Machine learning models amplify data quality issues. A model trained on incorrectly joined rebate-to-transaction records will learn systematically wrong patterns. Data quality is not a pre-requisite that can be hand-waved; it is the single largest risk to the G2N initiative and must be addressed architecturally, not just procedurally.

---

## 3. The Feature Store Gap & Organizational Friction

### 3.1 Current State: Project-by-Project Feature Engineering

Today, every ML project in Databricks independently engineers its features. A churn prediction model, an accrual forecasting model, and a leakage detection model each build their own version of "customer 90-day purchase velocity" or "contract complexity score" from scratch. This creates several problems:

1. **Redundant computation:** The same transformations are re-derived across projects, wasting compute cycles and data science time.
2. **Inconsistent definitions:** Two teams may define "net sales" differently, leading to conflicting model outputs that undermine business trust.
3. **No discoverability:** A new data scientist joining the G2N initiative has no way to find what features already exist or have been validated.
4. **No versioning or lineage:** When a feature definition changes, downstream models are unaware and may silently degrade.

### 3.2 The Organizational Misunderstanding

The Gold Data Products team, which curates governed datasets into Snowflake, reasonably views their outputs as complete, polished deliverables. From a BI and reporting perspective, they are correct. However, for ML/AI workloads, gold data products are necessary inputs, not final features. The gap between a gold data product and an ML-ready feature includes:

| Gold Data Product | What ML Additionally Needs | Example |
|-------------------|---------------------------|---------|
| Monthly rebate summary by customer | Rolling window aggregations, lag features, rate-of-change calculations | Customer rebate velocity (3/6/12 month rolling), YoY change rate, tier proximity % |
| Contract terms table | Complexity scoring, overlap detection, interaction features | Number of active tiers, multi-product bundle flag, days until tier reset |
| Transaction-level sales data | Seasonality decomposition, statistical profiling, entity embeddings | Parasiticide seasonal index, species-mix entropy, buying pattern cluster ID |
| Chargeback claims history | Anomaly scoring, duplicate probability, submission pattern features | Claim-to-transaction time delta, resubmission count, distributor error rate |

This distinction is critical. The gold team should not be asked to produce ML features (that is not their expertise or mandate), and data scientists should not be expected to work exclusively from gold products without iterating on silver-layer data. The architecture must create a clear, sanctioned path for both.

### 3.3 Proposed Resolution: The Feature Engineering Layer

Rather than asking for a full enterprise Feature Store (which carries significant infrastructure and governance overhead), we propose a pragmatic intermediate approach: a Feature Engineering Layer that sits between the Silver/Gold data and ML model training.

| Component | Location | Owner | Purpose |
|-----------|----------|-------|---------|
| Feature Registry | Collibra (via API) | AI CoE | Catalog of all defined features with business definitions, SQL/PySpark logic, lineage to source data products, version history, and quality metrics |
| Feature Compute Tables | Databricks Delta tables (S3 Silver layer) | AI CoE data engineers | Materialized feature tables computed from Silver + Gold inputs; scheduled refresh via Databricks Jobs; stored as Delta with time-travel for reproducibility |
| Feature Access API | Databricks / OpenShift | AI CoE ML engineers | Python SDK / API that data scientists use to discover, request, and pull features into training and inference pipelines by name and version |
| Feature Quality Dashboard | Streamlit | AI CoE | Monitors feature freshness, null rates, drift, and distribution changes; alerts on anomalies |

> **Key Principle: Features Live in the AI CoE Domain, Not in Gold Data Products**
>
> The Gold Data Products team continues to own governed, curated datasets in Snowflake. The AI CoE owns a separate Feature Engineering Layer in Databricks (Delta on S3). Features consume from both Silver (S3) and Gold (Snowflake) inputs. This avoids overloading the Gold team with ML-specific requests and gives data scientists the iterative freedom they need, while still maintaining governance through Collibra registration.

---

## 4. Target-State Architecture for G2N

### 4.1 End-to-End Data Flow

The following describes the complete data flow from source systems through to G2N model outputs, addressing data quality, feature engineering, and the organizational boundaries between the Gold Data Products team and the AI CoE.

| STAGE 1 | STAGE 2 | STAGE 3 | STAGE 4 | STAGE 5 |
|---------|---------|---------|---------|---------|
| **Source Systems** | **Bronze/Silver (S3)** | **Gold (Snowflake) + Feature Layer (Databricks)** | **ML Training & Inference (Databricks)** | **Serving & Front End** |
| EDI, ERP, Contracts, Claims, Spreadsheets | Ingestion + DQ profiling + cleansing | Curated products + ML features | Model dev, MLflow, batch scoring | OpenShift APIs, Angular, Streamlit |

### 4.2 Stage-by-Stage Design

#### Stage 1 → 2: Source Ingestion with Data Quality Gate

All G2N source data lands in S3 Bronze as raw, immutable copies. The Silver layer applies schema standardization, deduplication, and data quality profiling. The critical addition for the G2N initiative is a formal Data Quality Gate at the Silver layer:

1. **Collibra API integration:** Data quality rules for G2N entities (rebate claims, chargebacks, contracts, transactions) are defined and maintained in Collibra. Databricks pipelines call the Collibra REST API to pull the active rule set at runtime.
2. **Great Expectations in Databricks:** Quality checks (null rates, referential integrity, range validation, cross-source consistency) are executed as part of the Silver layer ETL using Great Expectations, with results pushed back to Collibra via API for governance tracking.
3. **Quarantine pattern:** Records failing critical quality checks are routed to a quarantine zone in S3 with Collibra-tracked remediation workflows. Non-critical failures are flagged but passed through with quality scores attached as metadata columns.
4. **Source-specific adapters:** Each distributor/source gets a dedicated adapter (Databricks notebook or job) that normalizes its schema to the canonical Silver model. This is where EDI format differences are resolved.

#### Stage 2 → 3: Gold Data Products & Feature Engineering Layer (Parallel Paths)

This stage has two parallel, complementary paths:

**Path A – Gold Data Products (Snowflake):** The existing Gold team continues to curate governed, business-ready datasets. For G2N, the AI CoE submits formal data product requests through the established intake process for datasets such as: Rebate Accrual Master (actuals vs. forecast by period, customer, product), Contract Terms Repository (active contracts with tier structures and effective dates), Chargeback Claims Reconciliation (claims matched to qualifying transactions), and Customer/Product Hierarchy (current and historical, point-in-time capable).

**Path B – Feature Engineering Layer (Databricks / S3 Silver):** The AI CoE independently builds and maintains ML features in Databricks Delta tables on S3. These features consume from both Silver (for raw granularity and iterative exploration) and Gold (for governed, curated inputs via Snowflake connector). Features are registered in Collibra via API with full lineage tracing.

> **Formal Process: How Data Scientists Request vs. Build**
>
> Need a governed, curated dataset (e.g., complete rebate history, reconciled transactions)? → Submit a data product request to the Gold team via the intake process. Need to iterate on transformations, test feature hypotheses, or build ML-specific aggregations? → Work directly on Silver-layer data in Databricks. Register validated features in the Feature Engineering Layer. Both paths are legitimate. Neither replaces the other.

#### Stage 3 → 4: Model Development in Databricks

Data scientists access features via the Feature Engineering Layer (Databricks Delta tables) and gold data products (Snowflake via connector). All experimentation is tracked in MLflow on Databricks. Model artifacts, hyperparameters, and evaluation metrics are logged. The MLflow Model Registry manages promotion from experimental → staging → production. Bitbucket stores all pipeline code with CI/CD triggering automated testing on merge.

#### Stage 4 → 5: Model Serving & Front-End Delivery

Batch inference runs as scheduled Databricks Jobs, with scored results written to Snowflake for consumption by downstream systems and dashboards. Real-time inference (e.g., deduction auto-classification) is served via containerized model endpoints on OpenShift, exposed as REST APIs. Streamlit dashboards (for analyst-facing tools like the G2N Accrual Health Monitor and Leakage Investigation Console) connect directly to Snowflake for scored results. The Angular production application (for commercial teams running contract simulations) calls OpenShift APIs for real-time model interaction.

---

## 5. API & Integration Design

All system interactions are API-mediated or go through formal request processes. No direct database-to-database coupling.

| From | To | Mechanism | Purpose | Owner |
|------|----|-----------|---------|-------|
| Databricks pipelines | Collibra | Collibra REST API (read DQ rules, write DQ results, register features) | Governance integration | AI CoE |
| Databricks | Snowflake Gold | Snowflake Spark connector (read gold data products) | Consume curated data | AI CoE / Data Products |
| AI CoE | Gold Data Products team | Formal intake request (JIRA/ServiceNow ticket) | Request new/modified data products | AI CoE submits, Gold team fulfills |
| Databricks (batch scores) | Snowflake | Snowflake Spark connector (write) | Publish model outputs for consumption | AI CoE |
| OpenShift (model APIs) | Angular / Streamlit | REST API (HTTPS) | Real-time predictions | AI CoE ML Eng |
| Streamlit dashboards | Snowflake | Snowflake Python connector / SQLAlchemy | Query scored results for visualization | AI CoE |
| Bitbucket CI/CD | Databricks / OpenShift | Databricks Jobs API / OpenShift CLI deploys | Automated pipeline and model deployment | AI CoE ML Eng |

---

## 6. Data Quality Architecture for G2N

Given the known multi-source data quality challenges, the G2N initiative requires a dedicated data quality architecture that is continuous, automated, and integrated with governance.

### 6.1 Quality Dimensions Monitored

| Dimension | G2N-Specific Check | Implementation |
|-----------|--------------------|----------------|
| Completeness | All expected distributor feeds received; no missing periods in rebate submissions | Great Expectations suite in Databricks Silver ETL; Collibra alerts on missing feeds |
| Uniqueness | No duplicate chargeback claims; unique transaction IDs across distributor submissions | Dedup logic in Silver adapter; fuzzy matching for near-duplicates |
| Consistency | Customer IDs resolve across ERP, contract system, and distributor feeds; product hierarchies align | Cross-source referential integrity checks; master data reconciliation job |
| Timeliness | Claims arriving beyond expected SLA flagged; staleness scoring on rebate accrual inputs | Metadata timestamp profiling; data freshness dashboard in Streamlit |
| Accuracy | Contract tier thresholds validated against source agreements; discount percentages within expected ranges | Range checks, statistical outlier detection, periodic reconciliation against source of truth |
| Validity | EDI transaction codes are valid; dates are logical (effective < expiration); monetary values are positive | Schema validation rules; business rule library in Collibra |

### 6.2 Quality Score Propagation

Every Silver-layer record receives a composite data quality score (0–100) based on the checks it passes. This score propagates through to feature computation and model training. Models can optionally weight training samples by data quality score, effectively down-weighting low-confidence records rather than discarding them. This is a pragmatic approach: rather than waiting for perfect data (which will never arrive), we build quality-awareness into the ML pipeline itself.

---

## 7. G2N Use Cases & Prioritization

| Use Case | Description | Est. Impact | Timeline | Key Data Dependencies |
|----------|-------------|-------------|----------|----------------------|
| Accrual Forecast Improvement | ML models to forecast rebate, chargeback, and return liabilities at SKU/channel/customer level. Reduce over/under accrual variance. | $5–20M+ freed working capital | 3–6 months | Gold: Rebate Accrual Master. Features: Rolling rebate velocity, seasonal indices, contract complexity. |
| Rebate Leakage Detection | Anomaly detection comparing contractual terms to actual payments. Identify overpayments, duplicate claims, tier miscalculations. | 1–3% of total deductions recovered | 3–4 months | Gold: Contract Terms, Chargeback Claims. Features: Claim-to-transaction delta, resubmission patterns, distributor error rate. |
| Deduction Auto-Classification | NLP/classification models to auto-categorize incoming deductions from distributor remittance data. Route for resolution. | 40–60% manual effort reduction | 4–6 months | Silver: Raw remittance text. Gold: Deduction taxonomy. Features: Text embeddings, historical resolution patterns. |
| Contract Optimization | Simulation engine for what-if analysis on rebate structures, tier thresholds, discount strategies by customer segment. | 0.5–1% net price improvement | 6–9 months | Gold: Full contract & sales history. Features: Customer elasticity, competitive positioning, market share trajectories. |
| Returns Prediction | Predict return propensity by product/channel/region to tighten return reserves. Account for animal pharma seasonality. | 10–20% reserve tightening | 3–5 months | Gold: Sales & returns history. Features: Shelf-life proximity, seasonal decomposition, channel return rates. |

---

## 8. Feature Engineering Layer: Detailed Design

### 8.1 Feature Lifecycle

1. **Discovery & hypothesis:** Data scientist identifies a feature need during exploratory analysis in Databricks, working with Silver and/or Gold data.
2. **Iterative development:** Feature logic is developed in Databricks notebooks, tested against historical data, and validated for predictive signal.
3. **Registration:** Once validated, the feature is registered in the Feature Registry (Collibra via API) with its business definition, computation logic (SQL or PySpark), input data sources with lineage, version number, and quality expectations.
4. **Materialization:** Feature computation is promoted to a scheduled Databricks Job that writes to a shared Delta table in the Feature Engineering Layer (S3). CI/CD via Bitbucket ensures code review and testing.
5. **Consumption:** Other data scientists discover features through the Collibra catalog or the Streamlit Feature Dashboard, and consume them from the shared Delta tables.
6. **Monitoring:** Feature quality (drift, nulls, freshness) is continuously monitored. Alerts fire when features degrade, triggering investigation.

### 8.2 G2N Feature Domains

The following feature domains are planned for the initial G2N initiative. Each domain contains multiple individual features:

| Feature Domain | Example Features | Source Layers |
|----------------|-----------------|---------------|
| Customer Behavior | Purchase velocity (30/60/90d rolling), ordering pattern regularity, species mix entropy, buying group loyalty index | Silver (transaction-level) + Gold (customer hierarchy) |
| Contract Complexity | Number of active tiers, multi-product flag, days to tier reset, tier proximity percentage, contract overlap count | Gold (Contract Terms Repository) |
| Rebate Dynamics | Rebate-to-sales ratio trend, claim submission lag distribution, historical over/under payment rate by distributor | Silver (raw claims) + Gold (Rebate Accrual Master) |
| Seasonal & Market | Parasiticide seasonal index, vaccine season indicator, regional livestock cycle, YoY growth rate | Silver (sales) + external market data |
| Data Quality Signals | Source completeness score, cross-reference match confidence, record staleness days, duplicate probability | Silver (DQ metadata from Stage 2) |

### 8.3 Bridging the Organizational Gap

To resolve the friction between the Gold Data Products team and the AI CoE, the following operating agreements are proposed:

1. **Clear boundary:** Gold Data Products team owns curated, governed datasets for BI and reporting in Snowflake. AI CoE owns the Feature Engineering Layer in Databricks for ML/AI workloads. Neither team is asked to do the other's work.
2. **Formal intake for Gold requests:** When the AI CoE needs a new or modified gold data product, they submit a request through the existing intake process with clear requirements (grain, attributes, refresh frequency, historical depth).
3. **Self-service for feature engineering:** Data scientists have sanctioned, direct access to Silver-layer data in S3 via Databricks for exploratory feature work. This is not a workaround; it is by design.
4. **Collibra as the common catalog:** Both gold data products and ML features are registered in Collibra, providing firm-wide discoverability regardless of where the data physically lives.
5. **Quarterly review:** AI CoE and Gold Data Products team meet quarterly to review feature-to-gold-product dependencies, identify features that should be promoted to gold products, and align on roadmap priorities.

---

## 9. Governance, Compliance & Auditability

G2N calculations have direct financial reporting implications and are subject to SOX audit requirements. The architecture must provide complete traceability from model output back to source data.

### 9.1 Lineage Chain

Collibra provides end-to-end lineage: Source system → Bronze (S3) → Silver (S3, with DQ metadata) → Gold Data Product (Snowflake) and/or Feature (Databricks Delta) → Model Training Dataset (MLflow logged) → Model Version (MLflow Registry) → Scored Output (Snowflake). Every link in this chain is registered via API and auditable.

### 9.2 Model Governance

1. All models are registered in the MLflow Model Registry with version history, training dataset references (linked to Collibra lineage), and performance metrics.
2. Model promotion from staging to production requires documented approval, passing automated validation tests (run via Bitbucket CI/CD), and a Collibra-registered impact assessment.
3. Model outputs (accrual forecasts, leakage flags, classifications) are written to dedicated Snowflake tables with full audit columns (model version, scoring timestamp, input feature versions).
4. The Streamlit Model Governance Dashboard provides Finance and Internal Audit with visibility into which models are in production, when they were last retrained, and their current performance metrics.

---

## 10. Implementation Roadmap

| Phase | Timeline | Deliverables | Dependencies | Success Metric |
|-------|----------|-------------|--------------|----------------|
| Phase 0: Foundation | Months 1–2 | G2N data quality profiling complete; DQ rules in Collibra; Silver-layer quality gates operational; Gold data product requests submitted | Collibra API access; Great Expectations setup | DQ scorecard published; <5% critical quality failures in Silver |
| Phase 1: Feature Layer + First Model | Months 2–5 | Feature Engineering Layer operational in Databricks; Feature Registry in Collibra; first G2N features materialized; Accrual Forecast MVP model trained and validated | Gold data products available; Databricks Delta tables provisioned | 20+ features registered; accrual variance reduced by 15%+ vs. current method |
| Phase 2: Leakage + Classification | Months 4–7 | Leakage detection model in production; deduction auto-classification deployed on OpenShift; Streamlit investigation dashboard live | Accrual model validated; OpenShift deployment pipeline | $X identified leakage; 50%+ deductions auto-classified |
| Phase 3: Optimization + Scale | Months 6–12 | Contract optimization simulation engine; returns prediction; Angular commercial app; feature layer expanded to 100+ features with firm-wide adoption | Business stakeholder engagement; Angular dev resources | Net revenue improvement measurable in quarterly financials |

---

## 11. Key Risks & Mitigations

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Data quality issues are worse than expected, requiring extended remediation | Model accuracy degrades; business trust erodes before value is demonstrated | High | Phase 0 profiling will quantify the actual state; quality-weighted training mitigates impact |
| Gold Data Products team capacity constrains G2N data product delivery | Feature engineering blocked on gold inputs; timeline delays | Medium | Dual-path design allows Silver-layer feature work to proceed independently |
| Organizational resistance to Feature Engineering Layer (perceived as shadow IT) | Political friction; governance team blocks feature layer | Medium | Collibra registration ensures governance; quarterly review builds trust; executive sponsorship |
| Feature layer becomes another ungoverned silo over time | Returns to project-by-project chaos | Medium | Mandatory Collibra registration; automated monitoring; CoE stewardship |

---

## 12. Conclusion

The G2N initiative represents a high-impact opportunity for the AI CoE to demonstrate firm-wide value. Success depends not just on building good models, but on solving the foundational challenges of data quality, feature reuse, and organizational alignment that will determine whether those models can be trusted and sustained.

This design addresses those challenges directly: a data quality architecture that makes quality measurable and continuous rather than aspirational, a Feature Engineering Layer that gives data scientists the iterative freedom they need while maintaining governance, and clear organizational boundaries that respect the Gold Data Products team's mandate while enabling ML-specific workflows.

The phased roadmap prioritizes quick wins (accrual forecasting, leakage detection) that generate measurable financial impact within 3–6 months, building the credibility and organizational momentum needed for the more ambitious optimization use cases that follow.
