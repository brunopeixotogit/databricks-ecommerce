# E-commerce Data Platform on Databricks

A production-style data platform that simulates e-commerce user behavior
(electronics, appliances, furniture) and lands events through a
**Bronze → Silver → Gold** medallion architecture on Delta Lake.

Built for **Databricks Free Edition** with PySpark and Delta Lake. The
architecture is deliberately structured so that swapping the synthetic
producer for Kafka / Kinesis later requires changing only the ingestion
connector — downstream logic stays identical.

## Layers

- **Bronze** — raw, append-only, source-faithful events ingested with Auto Loader.
- **Silver** — deduplicated, type-cleansed, sessionized; SCD2 dimensions.
- **Gold** — star-schema marts, KPIs, ML feature tables.

## Repository layout

```
databricks_ecommerce/
├── conf/             # YAML configuration (pipeline + simulator)
├── src/
│   ├── common/       # schemas, IO helpers, config loader, quality
│   ├── simulator/    # entity + behavior generators, file emitter
│   ├── bronze/       # Auto Loader ingestion jobs
│   ├── silver/       # cleansing, dedup, sessionization, SCD2
│   └── gold/         # aggregates, marts, ML features
├── notebooks/        # Databricks notebooks (thin shells over src/)
└── tests/            # PySpark / pytest unit tests
```

## Running on Databricks Free Edition

1. Import this repo into your workspace via **Repos / Git folders**.
2. Run `notebooks/00_setup.py` once to create schemas and the landing volume.
3. Run `notebooks/10_run_simulator.py` to emit events into the volume.
4. Run the medallion in order: `20_bronze` → `30_silver` → `40_gold`.
5. (Optional) Wire all of the above into a **Databricks Workflow** for continuous operation.

All notebooks accept catalog / schema names as widget parameters, so the
same code runs in dev and prod without edits.

## Design principles

- **Decoupled producer**: the simulator writes files to a Volume; the
  ingestion layer doesn't know it's synthetic.
- **Schema explicit**: schemas are pinned in `src/common/schemas.py` —
  no `inferSchema`, so type drift is impossible to introduce silently.
- **Idempotent**: dedup by `event_id`, SCD2 keyed on attribute hash,
  re-running any job is safe.
- **Append-only Bronze**: Bronze is a replay log. Silver and Gold can
  always be rebuilt from Bronze.
- **Watermarked**: late-arriving events are tolerated up to a bounded
  watermark; rescued data is captured in `_rescued_data`.

## Local development

```bash
pip install -r requirements.txt
pytest tests/
```

The simulator and quality helpers are pure Python, so most logic can be
unit-tested without a Databricks cluster.

## Scaling to production

| Concern | Free Edition (here) | Production |
|---|---|---|
| Ingestion | Auto Loader on a Volume | Kafka / Kinesis / EventHub structured streaming |
| Compute | Single shared cluster | Per-layer job clusters, Photon, autoscaling, spot |
| Orchestration | Databricks Workflows | Same + Delta Live Tables for declarative lineage |
| Governance | Hive metastore / single UC | Full UC: lineage, ABAC, column masks, tagging |
| Quality | Manual `CHECK` + expectations | DLT expectations or Great Expectations in CI |
| Schema evolution | `_rescued_data` column | Schema registry |
| Performance | Manual `OPTIMIZE` + Z-ORDER | Liquid Clustering, Predictive Optimization |
| CI/CD | Manual deploy | Databricks Asset Bundles + GitHub Actions |
| PII | None (synthetic) | Tokenization + UC column masks |
| ML | Notebook training | Feature Store + MLflow + Model Serving |

The architectural shape — medallion, event-first, decoupled producer —
stays identical. Only the substrate changes.
