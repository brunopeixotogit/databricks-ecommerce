# Documentation index

| Doc | Read this for |
|---|---|
| [`architecture.md`](./architecture.md)     | End-to-end system design, data flow, principles, tradeoffs |
| [`bronze_layer.md`](./bronze_layer.md)     | Auto Loader configuration, schema enforcement, append-only contract |
| [`silver_layer.md`](./silver_layer.md)     | Dedup, sessionisation, SCD2 by attribute hash, business rules |
| [`gold_layer.md`](./gold_layer.md)         | Marts, KPIs, BI readiness, query examples |
| [`data_model.md`](./data_model.md)         | Pinned schemas, ER relationships, modelling decisions |
| [`data_quality.md`](./data_quality.md)     | Expectation framework, severity levels, failure handling |
| [`ci_cd.md`](./ci_cd.md)                   | GitHub Actions, test strategy, Databricks Asset Bundles, deploy plan |
| [`runbook.md`](./runbook.md)               | Step-by-step run, debugging guide, reprocessing strategy |

Top-level entry points:

- [`../README.md`](../README.md) — portfolio-facing project overview.
- [`../databricks.yml`](../databricks.yml) — Asset Bundle declaring the Workflow.
- [`../.github/workflows/ci.yml`](../.github/workflows/ci.yml) — CI pipeline.
