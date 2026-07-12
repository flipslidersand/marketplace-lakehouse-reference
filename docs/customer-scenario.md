# Customer Scenario

## Customer Background

Acme Marketplace is a digital-native commerce company operating multiple sales channels — web, mobile, in-store kiosks, and partner integrations. The company has grown rapidly and now processes thousands of orders daily across its product catalog.

Operational data is produced by three independent systems that were built at different times by different teams:

- An **order service** that emits JSON events when orders are placed
- An **inventory management system** that exports CSV snapshots on a periodic schedule
- A **pricing service** that emits JSON events when product prices are changed

Each system operates independently. There is no shared data contract, no common timestamp format enforcement, and no cross-system deduplication.

## Current Operational Problem

The operations team cannot answer basic business questions without manual intervention:

- Which products are at risk of running out of stock?
- Are duplicate orders inflating reported revenue?
- Have any abnormal price changes occurred that could indicate a configuration error?
- How much revenue was generated today across channels?

Answering these questions today requires an analyst to pull exports from three separate systems, reconcile formats in a spreadsheet, and manually check for duplicates. This process takes hours and produces results that are already stale by the time they are available.

Beyond the time cost, the manual process introduces risk. Invalid records — orders with missing IDs, negative quantities, malformed timestamps — have caused incorrect revenue figures in past reporting periods. There is no systematic way to identify and investigate these records after the fact.

## Business Impact

The current state has three measurable consequences:

1. **Inventory surprises.** Stockout events are discovered only when customers report that items are unavailable. There is no early warning system.

2. **Revenue reporting errors.** Duplicate orders have been counted in gross revenue in at least two reporting periods. The root cause was not identified until the following month.

3. **Price anomaly visibility.** A pricing service misconfiguration that resulted in an 80% price increase on a high-velocity product went undetected for several hours. Detection was customer-reported, not system-detected.

## Evaluation Scope

This technical evaluation addresses the following questions:

- Can order, inventory, and pricing data be unified into a single queryable foundation?
- Can invalid records be isolated without disrupting the pipeline?
- Can duplicate orders be detected and excluded from revenue calculations?
- Can inventory risk be calculated automatically using order history?
- Can abnormal price changes be surfaced in near real-time?
- Can business metrics be consumed by an external operational application?

## Non-Goals

The following are explicitly outside the scope of this evaluation:

- Production-scale performance benchmarking
- Real-time streaming ingestion (this evaluation uses batch ingestion)
- PII classification and data masking
- Unity Catalog permission model
- ML-based anomaly detection (deterministic rules are used for the first evaluation)
- Integration with existing BI tools or reporting infrastructure
- Multi-region or disaster recovery design

## Expected Outcomes

By the end of this evaluation, the following should be demonstrable:

1. Raw source data is preserved and traceable to its origin
2. Invalid records are identified, labeled, and available for investigation
3. Duplicate orders cannot inflate revenue metrics
4. Inventory risk is calculated automatically and available to the operations team
5. Abnormal price changes are detected and surfaced without manual review
6. A simple external application can query and display these metrics
7. Architectural decisions are documented with production considerations identified
