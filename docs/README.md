# BricksShop — Documentation index

| Doc | Read this for |
|---|---|
| [`architecture.md`](./architecture.md)     | End-to-end system design, data flow, principles, tradeoffs |
| [`bronze_layer.md`](./bronze_layer.md)     | Auto Loader configuration, schema enforcement, append-only contract |
| [`silver_layer.md`](./silver_layer.md)     | Dedup, sessionisation, SCD2 by attribute hash, business rules |
| [`gold_layer.md`](./gold_layer.md)         | Marts, KPIs, BI readiness, query examples |
| [`data_model.md`](./data_model.md)         | Pinned schemas, ER relationships, modelling decisions |
| [`data_quality.md`](./data_quality.md)     | Expectation framework, severity levels, failure handling |
| [`ci_cd.md`](./ci_cd.md)                   | GitHub Actions, deploy targets, **orchestrator modes (`prod`/`simulator`/`full`)** |
| [`dlt_pipeline.md`](./dlt_pipeline.md)     | DLT pipeline: code structure, execution model, deployment |
| [`runbook.md`](./runbook.md)               | Step-by-step run, debugging guide, reprocessing strategy |

Companion entry points:

- [`../README.md`](../README.md) — project overview, mode-controlled execution, PT-BR appendix.
- [`../web/README.md`](../web/README.md) — BricksShop web app: API, schema parity with `EVENT_SCHEMA`, end-to-end test plan.
- [`../databricks.yml`](../databricks.yml) — Asset Bundle (variables, resources, three deploy targets).
- [`../.github/workflows/ci.yml`](../.github/workflows/ci.yml) — CI pipeline.
- [`../.github/workflows/cd.yml`](../.github/workflows/cd.yml) — CD pipeline (DLT-only path-filtered runs).
