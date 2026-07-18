# Marketplace Lakehouse Reference

![CI](https://github.com/flipslidersand/marketplace-lakehouse-reference/actions/workflows/ci.yml/badge.svg)
![Tests](https://img.shields.io/badge/tests-31%2F31%20passing-brightgreen)
![Python](https://img.shields.io/badge/python-3.12-blue)
![License](https://img.shields.io/badge/license-Apache%202.0-green)

A working end-to-end Medallion Architecture pipeline (Bronze → Silver → Gold) built with PySpark and Delta Lake — translating a fragmented marketplace data problem into measurable business outcomes, runnable locally in under two minutes.

![Dashboard](docs/assets/dashboard.png)

This repository demonstrates a technical evaluation for a fictional digital-native marketplace whose order, inventory, and pricing data is fragmented across operational systems.

The goal is not to demonstrate PySpark syntax. The goal is to show how a customer problem can be translated into explicit success criteria, a lakehouse architecture, an executable data pipeline, and a consumer-facing application.

---

## Customer Problem

**Acme Marketplace** operates three independent systems that produce data in different formats on different schedules:

- **Order service** — emits JSON events when orders are placed
- **Inventory management** — exports CSV snapshots periodically
- **Pricing service** — emits JSON events when prices change

The operations team cannot answer basic questions without manual spreadsheet reconciliation:

- Which products are at risk of running out of stock?
- Are duplicate orders inflating revenue metrics?
- Have abnormal price changes occurred?
- How much revenue was generated today?

Invalid records — missing IDs, malformed timestamps, zero quantities — have caused incorrect revenue figures in past reporting periods with no systematic way to trace or investigate them.

---

## Success Criteria

| Criterion                                          | How It Is Verified                                             |
| -------------------------------------------------- | -------------------------------------------------------------- |
| New events can be ingested                         | Bronze tables populated from JSONL and CSV                     |
| Source data remains traceable                      | `_ingested_at` and `_source_file` on all Bronze records        |
| Invalid records cannot corrupt curated data        | Silver tests assert no nulls or invalid values pass through    |
| Invalid records remain available for investigation | Quarantine tables with `_quarantine_reason`                    |
| Duplicate orders cannot inflate revenue            | Duplicates quarantined; Gold computed from deduplicated Silver |
| Inventory risk can be calculated                   | `days_of_stock` and `risk_level` per SKU in Gold               |
| Abnormal price changes are visible                 | Price events with `abs(change) >= 30%` flagged in Gold         |
| Gold data is consumable externally                 | Streamlit app queries Gold via Databricks SQL                  |

---

## Architecture

```text
Synthetic Data Generator
         |
         v
Orders (JSONL)  Inventory (CSV)  Price Events (JSONL)
         |              |                 |
         └──────────────┴─────────────────┘
                        |
                        v
              Bronze Delta Tables
              (raw, with metadata)
                        |
                        v
              Silver: Validation + Dedup
              Clean tables + Quarantine tables
                        |
                        v
         ┌──────────────┼──────────────┐
         v              v              v
   daily_revenue  inventory_risk  price_anomalies
         └──────────────┴──────────────┘
                        |
                  Databricks SQL
                        |
                  Streamlit Dashboard
```

### Layer Responsibilities

| Layer  | Responsibility                                                                                    |
| ------ | ------------------------------------------------------------------------------------------------- |
| Bronze | Raw data preservation. No validation. Full traceability. Enables reprocessing.                    |
| Silver | Validation, type normalization, deduplication. Invalid records → quarantine with explicit reason. |
| Gold   | Business metrics from clean data only. Duplicate-safe revenue. Inventory risk. Price anomalies.   |

---

## Data Quality Strategy

Invalid records are **never silently dropped**. Each invalid record is written to a quarantine table with a `_quarantine_reason` field:

| Reason                                    | Meaning                                                   |
| ----------------------------------------- | --------------------------------------------------------- |
| `MISSING_ORDER_ID`                        | `order_id` is null                                        |
| `MISSING_PRODUCT_ID`                      | `product_id` is null                                      |
| `INVALID_QUANTITY`                        | quantity is null, zero, or negative                       |
| `INVALID_PRICE`                           | unit price is null or zero                                |
| `INVALID_TIMESTAMP`                       | timestamp cannot be parsed                                |
| `DUPLICATE_ORDER`                         | `order_id` appears more than once; earlier record removed |
| `MISSING_WAREHOUSE`                       | inventory record has no warehouse                         |
| `INVALID_OLD_PRICE` / `INVALID_NEW_PRICE` | price is null or zero                                     |

Negative inventory quantities are flagged with `negative_stock=true` but are not quarantined — they may represent valid backorder states.

---

## Delta Lake Operations

The layer modules rebuild each table with `overwrite` for reproducibility. Real ingestion is incremental, so `src/marketplace_lakehouse/delta_ops.py` demonstrates the operational primitives Delta Lake adds on top of plain Parquet — the topics that come up once a pipeline is in production:

| Operation           | Function                                    | Why it matters                                                                             |
| ------------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Idempotent upsert   | `merge_upsert(...)`                         | `MERGE` on business keys — replaying a batch after a partial failure never duplicates rows |
| File compaction     | `optimize_table(...)`                       | `OPTIMIZE` (+ optional `ZORDER`) bin-packs small files and clusters for file skipping      |
| Storage reclamation | `vacuum_table(...)`                         | `VACUUM` removes tombstoned files past the retention floor (default 7 days)                |
| Audit trail         | `table_history(...)`                        | `DESCRIBE HISTORY` — every commit's operation, timestamp, and metrics                      |
| Time travel         | `read_version(...)` / `latest_version(...)` | Reproduce last quarter's numbers by reading the table as of a prior version                |

**Idempotent replay** is the headline property. Because `merge_upsert` matches on keys such as `order_id`, running the same batch twice leaves the table unchanged — verified in `tests/test_delta_ops.py::TestMergeUpsert::test_replaying_same_batch_is_idempotent`. The same tests show updates and inserts co-existing, and time travel returning a table's earlier state after an update.

```python
from marketplace_lakehouse.delta_ops import merge_upsert, optimize_table, read_version

# Incremental, safe-to-retry ingestion
merge_upsert(spark, clean_orders, silver_orders_path, key_cols=["order_id"])

# Maintenance
optimize_table(spark, silver_orders_path, zorder_by=["product_id"])

# Reproduce a prior state
snapshot = read_version(spark, silver_orders_path, version=3)
```

All operations are path-based, so they run identically on a laptop and on a Databricks workspace.

---

## Repository Structure

```text
marketplace-lakehouse-reference/
├── README.md
├── LICENSE                         # Apache 2.0
├── pyproject.toml
├── databricks.yml
├── .env.example
├── .gitignore
│
├── docs/
│   ├── architecture.md             # System design and data flow
│   ├── customer-scenario.md        # Customer problem and evaluation scope
│   ├── technical-evaluation.md     # Success criteria and architecture decisions
│   └── workshop.md                 # 60-minute workshop guide
│
├── data/
│   └── samples/                    # Generated synthetic data (gitignored)
│
├── src/
│   ├── generator/
│   │   └── generate_events.py      # Synthetic data generator (intentionally imperfect)
│   └── marketplace_lakehouse/
│       ├── bronze.py               # Raw ingestion
│       ├── silver.py               # Validation, dedup, quarantine
│       ├── gold.py                 # Business metrics
│       ├── quality.py              # Shared quality utilities
│       └── delta_ops.py            # MERGE upsert, OPTIMIZE/ZORDER, VACUUM, time travel
│
├── notebooks/
│   ├── 01_ingestion.py             # Bronze ingestion (Databricks)
│   ├── 02_validation.py            # Silver validation (Databricks)
│   ├── 03_business_metrics.py      # Gold metrics (Databricks)
│   └── 04_technical_evaluation.py  # Acceptance criteria walkthrough
│
├── app/
│   ├── main.py                     # Streamlit operations dashboard
│   └── databricks_client.py        # Databricks SQL connector
│
└── tests/
    ├── conftest.py                  # Shared SparkSession fixture
    ├── test_silver.py               # Silver acceptance tests
    ├── test_gold.py                 # Gold metric tests
    ├── test_quality.py              # Quality utility tests
    └── test_delta_ops.py            # MERGE idempotency & time-travel tests
```

---

## Getting Started

### Prerequisites

- Python 3.10+
- Java 11+ (required by PySpark)

### Quick Start (local, ~2 minutes)

```bash
git clone https://github.com/flipslidersand/marketplace-lakehouse-reference
cd marketplace-lakehouse-reference

python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]" pyarrow

python3 run_pipeline.py          # Bronze → Silver → Gold, exports parquet
streamlit run app/main.py        # Dashboard at http://localhost:8501
```

`run_pipeline.py` generates synthetic data on first run, then executes all three layers and prints a summary of clean vs. quarantined record counts per table.

### Databricks Workspace

Upload the `data/samples/` files to DBFS and run notebooks 01 through 04 in order.

---

## Running the Tests

```bash
pytest tests/ -v
```

Tests cover:

- Null order IDs are quarantined
- Zero and negative quantities are quarantined
- Invalid timestamps are quarantined
- Duplicate order IDs are removed from Silver and quarantined
- Revenue calculation is correct
- Duplicate orders do not inflate revenue
- Inventory risk thresholds (CRITICAL / HIGH / NORMAL)
- Zero sales velocity does not cause a division-by-zero error
- Price anomalies above 30% are flagged
- Price changes below 30% are not flagged

---

## Running the Demo Application

```bash
cp .env.example .env
# Edit .env with your Databricks credentials

streamlit run app/main.py
```

If `DATABRICKS_HOST` is not set, the app loads local sample data from `data/gold_sample/` and renders in offline mode.

The dashboard displays:

- **KPI cards** — Revenue Today, At-Risk SKUs, Price Anomalies
- **Inventory Risk table** — CRITICAL and HIGH SKUs with days-of-stock
- **Price Anomaly table** — Events flagged above the 30% threshold
- **Revenue chart** — Daily gross revenue by channel

---

## Technical Decisions

### Why Medallion Architecture

Three-layer separation maps directly to three distinct problems: traceability (Bronze), data quality (Silver), and business consumption (Gold). Each layer has a single, testable responsibility.

### Why Deterministic Anomaly Rules

Explicit business rules were selected over machine learning for the initial evaluation. Reasons: explainability, simple acceptance criteria, no labeled historical dataset, and operational transparency. ML may be introduced in a later phase after sufficient feedback data is available.

### Why PySpark DataFrame API

Distributed operations are expressed as composable DataFrame transformations. The same code runs locally on the sample dataset and on a Databricks cluster at production scale — no rewrite required when volume increases.

### Why Local Streamlit (No Cloud Deploy)

The demo application is designed to run locally to eliminate infrastructure cost and complexity during the evaluation phase. A production deployment would use Databricks Apps or a container service.

---

## Production Considerations

The following are outside the scope of this prototype and would require explicit design decisions before production deployment:

- Unity Catalog permissions and column-level access controls
- PII classification and data masking
- Schema evolution policy for source system changes
- Pipeline observability and quarantine rate monitoring
- Failure alerting
- Data retention policy
- Disaster recovery
- Workload isolation between pipeline jobs and SQL Warehouse queries
- SQL Warehouse sizing and auto-stop configuration
- Cost monitoring and DBU budgets

See [docs/technical-evaluation.md](docs/technical-evaluation.md) for a full discussion of each topic.

---

## Limitations

- Data is fully synthetic. No real customer, order, or pricing data is included.
- Ingestion is batch-based. Real-time streaming is not demonstrated.
- The sample dataset is small by design. Performance at production scale is not benchmarked.
- The Streamlit application runs locally only. Cloud deployment is not included.
- Unity Catalog integration is not implemented.

---

## License

Apache License 2.0. See [LICENSE](LICENSE).
