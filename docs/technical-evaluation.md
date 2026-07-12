# Technical Evaluation

## Customer Problem

Acme Marketplace operates multiple sales channels and receives operational data from three independent systems: an order service, an inventory management system, and a pricing service. Each system uses a different format and has no shared data contract with the others.

The operational impact of this fragmentation is direct:

- The operations team cannot assess inventory risk without manual spreadsheet reconciliation
- Duplicate orders have caused incorrect revenue figures in past reporting periods
- Price anomalies go undetected until surfaced by customer complaints
- There is no queryable foundation for building operational tooling

This evaluation demonstrates how a lakehouse architecture using Medallion Architecture, Delta Lake, and Apache Spark can address these problems with explicit, verifiable acceptance criteria.

## Success Criteria

The technical evaluation is successful when all of the following are true:

| Criterion                                                | Verification                                                            |
| -------------------------------------------------------- | ----------------------------------------------------------------------- |
| New events can be ingested from all three sources        | Bronze tables populated from JSONL and CSV                              |
| Source data remains traceable                            | `_ingested_at` and `_source_file` present on all Bronze records         |
| Invalid records cannot corrupt curated datasets          | Silver validation tests pass; nulls/bad values absent from Silver       |
| Invalid records remain available for investigation       | Quarantine tables contain records with `_quarantine_reason`             |
| Duplicate orders cannot inflate revenue                  | Duplicate order IDs quarantined; Gold revenue computed from Silver only |
| Inventory risk can be calculated                         | `gold.inventory_risk` contains `days_of_stock` and `risk_level` per SKU |
| Abnormal price changes are visible                       | `gold.price_anomalies` flags events with `abs(change) >= 30%`           |
| Gold datasets can be consumed by an external application | Streamlit app queries Gold via Databricks SQL and renders KPI cards     |

## Architecture Decision: Medallion Architecture

Medallion Architecture was selected because the customer problem maps directly to its three-layer separation.

**Bronze** solves the traceability problem. Raw data from all three source systems is preserved without modification. If a business rule changes next month, the original data can be reprocessed. If an invalid record needs to be investigated, it is available at the source-fidelity level.

**Silver** solves the data quality problem. Validation, type normalization, and deduplication happen in a single layer with explicit rules. Invalid records are written to quarantine tables rather than silently dropped, which means the operations team can inspect and resolve data issues.

**Gold** solves the consumption problem. Business metrics are computed exclusively from clean, validated data. The serving layer always reflects the correct state without requiring consumers to implement their own filtering.

The alternative — a single-layer ELT that combines ingestion and transformation — would make it impossible to distinguish between a missing record and a correctly excluded invalid record. Medallion Architecture makes that distinction explicit by design.

## Why Spark

Transformations in this evaluation are implemented using distributed DataFrame operations (PySpark). The sample dataset is intentionally small for local reproducibility.

The processing model is designed to remain applicable when data volume increases. Filtering, deduplication via window functions, groupBy aggregations, and join operations all scale horizontally in a Spark cluster without changes to the transformation logic. The same Silver and Gold code that runs locally on the sample dataset would run on a Databricks cluster against production-scale data.

This evaluation does not claim to demonstrate actual large-scale production performance. It demonstrates that the processing model is correct and production-appropriate.

## Why Deterministic Anomaly Rules

Price anomaly detection uses a deterministic threshold rule: flag events where the absolute price change ratio is 30% or greater.

This decision was made for four reasons:

1. **Explainability.** The operations team can understand, verify, and override the rule. A machine learning model would require additional tooling to explain individual predictions.

2. **Simple acceptance criteria.** The test suite can verify the rule exactly: a +30% change is flagged, a +29.9% change is not. ML-based detection has no equivalent deterministic test.

3. **Limited historical feedback data.** An ML model for price anomaly detection would require a labeled dataset of known-good and known-anomalous price changes. That dataset does not exist at evaluation time.

4. **Operational transparency.** When the rule flags an event, the operations team knows exactly why. When an ML model flags an event, the reason requires interpretation.

ML-based anomaly detection may be introduced after sufficient historical feedback data is available and after the operations team has built intuition about which events the deterministic rule misses or incorrectly flags.

## Production Considerations

The following topics are intentionally outside the scope of this prototype. Each represents a design decision that would need to be made in a production implementation.

### Unity Catalog Permissions

This evaluation uses unmanaged Delta tables written to a file path. A production implementation would register tables in Unity Catalog with column-level access controls, row filters, and audit logging.

### PII Classification

Order data may contain customer identifiers. This evaluation uses fully synthetic data. A production implementation would require a PII classification scan, appropriate masking or tokenization before Bronze ingestion, and access controls on fields containing personal information.

### Schema Evolution Policy

The Bronze layer uses a permissive schema with StringType for numeric fields to absorb schema drift from source systems. A production implementation would define an explicit schema evolution policy: which changes are additive (new nullable columns), which are breaking (type changes, removed required fields), and how each is handled in Silver.

### Observability

This evaluation has no pipeline monitoring. A production implementation would instrument each layer with row counts, quarantine rates, and processing latency metrics, emitted to an observability platform and surfaced in dashboards.

### Pipeline Failure Alerts

If the Silver layer fails to process a batch, this evaluation produces no alert. A production implementation would require alerting on pipeline failures, quarantine rate spikes, and unexpected drops in record counts.

### Data Quality Monitoring

A production implementation would track quarantine rates over time. A sudden increase in `INVALID_TIMESTAMP` records from the order service would indicate a source system change that needs investigation.

### Cost Monitoring

Databricks SQL Warehouse costs scale with query frequency and cluster size. A production implementation would set per-warehouse spending limits and monitor DBU consumption per Gold table query.

### Data Retention

This evaluation retains all Delta table versions indefinitely. A production implementation would define a retention policy for each layer, balancing reprocessing requirements against storage cost.

### Disaster Recovery

This evaluation has a single copy of each Delta table. A production implementation would define an RTO/RPO target and implement cross-region replication or backup for Gold tables.

### Workload Isolation

This evaluation uses a single Databricks workspace and SQL Warehouse. A production implementation would isolate pipeline workloads from serving workloads to prevent query latency spikes when large pipeline jobs run.

### SQL Warehouse Sizing

The Streamlit application queries Gold tables on a single SQL Warehouse. A production implementation would right-size the warehouse for expected query concurrency and configure auto-stop to avoid idle costs.
