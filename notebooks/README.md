# Databricks Notebooks

Thin entry-point wrappers for Databricks Jobs. These notebooks call into the
`g2n` package rather than containing business logic directly.

## Structure

- `exploration/` — EDA and ad-hoc analysis notebooks
- `pipelines/` — Entry points for scheduled Silver ETL and feature compute jobs
- `models/` — Entry points for model training Databricks Jobs
