# Workshop Guide

## Hypothetical 60-Minute Customer Workshop

This guide outlines a structured workshop for walking a customer through the technical evaluation. The goal is not a product demo — it is a collaborative discovery session that surfaces the customer's actual data problems and validates whether the proposed architecture addresses them.

---

## Agenda

| Time      | Segment                                   |
| --------- | ----------------------------------------- |
| 0–10 min  | Business problem and current architecture |
| 10–20 min | Data source and quality review            |
| 20–30 min | Lakehouse architecture walkthrough        |
| 30–45 min | PySpark pipeline demo                     |
| 45–55 min | Operations dashboard demo                 |
| 55–60 min | Production considerations and next steps  |

---

## 0–10 min: Business Problem and Current Architecture

**Objective:** Confirm the problem statement and establish shared vocabulary before showing any technology.

Start with the customer's current state, not the solution.

Open questions:

- How does your operations team currently answer "which products are at risk of stockout?"
- What is the typical delay between an inventory event and when operations can act on it?
- Walk me through what happens when a pricing error occurs. How is it detected? How long does it take?

Transition:

> Before we show the architecture, we want to make sure we are solving the right problem. Let us walk through what we heard from your team and confirm the priority order.

---

## 10–20 min: Data Source and Quality Review

**Objective:** Make the data quality problems concrete. Show the synthetic defects before showing the fix.

Open the generator and show examples of intentionally bad data:

```python
# Show a quarantine sample
quarantine.select("order_id", "_quarantine_reason").show(10)
```

Discussion questions:

- Which system currently owns inventory truth? Is the CSV export the authoritative source, or is there a real-time API?
- How are duplicate orders handled today? Are they caught before the order service emits them, or does deduplication happen downstream?
- What is the acceptable delay for inventory risk detection? Hours? Minutes?
- Who investigates invalid data records today? Does that investigation have a defined workflow?

Key point to land:

> Every invalid record we show you here is currently being silently dropped or manually reviewed. The quarantine table makes that invisible problem visible and auditable.

---

## 20–30 min: Lakehouse Architecture Walkthrough

**Objective:** Explain the three-layer design and connect each layer to a specific customer problem.

Walk through `docs/architecture.md` with the Mermaid diagram.

| Layer  | Customer Problem It Solves                                                               |
| ------ | ---------------------------------------------------------------------------------------- |
| Bronze | "We can't reproduce last month's revenue figure because the source data was overwritten" |
| Silver | "Invalid records appear in our revenue dashboard without any warning"                    |
| Gold   | "We can't query inventory risk without building a custom script every time"              |

Discussion questions:

- Which metrics require historical reproducibility? (Revenue? Inventory snapshots? Price history?)
- What data should be classified as sensitive? Customer IDs? Order values?
- What workload growth is expected over the next 12 months? Will order volume double? Ten-x?

---

## 30–45 min: PySpark Pipeline Demo

**Objective:** Run the actual pipeline and show the acceptance criteria being met in real time.

Run in this order:

1. Generate synthetic data

   ```bash
   python3 src/generator/generate_events.py
   ```

2. Show the intentional defects in the raw data

   ```bash
   grep "null\|NOT-A-DATE\|BAD" data/samples/orders.jsonl | head -5
   ```

3. Run the notebooks (or equivalent local pipeline) and show:
   - Bronze row count vs source record count (should match)
   - Quarantine breakdown by reason
   - Silver order count (Bronze minus quarantined minus duplicates)
   - Gold daily revenue total

4. Run the tests
   ```bash
   pytest tests/ -v
   ```

Key point to land:

> These tests are the acceptance criteria written as code. Every test maps directly to a business rule we agreed on at the start of the session. If a test fails, a business rule is broken.

---

## 45–55 min: Operations Dashboard Demo

**Objective:** Show that Gold data is consumable by a real external application without any custom integration work.

```bash
streamlit run app/main.py
```

Walk through:

1. KPI cards — Revenue Today, At-Risk SKUs, Price Anomalies
2. Inventory Risk table — point out CRITICAL and HIGH rows
3. Price Anomaly table — point out the largest percentage changes
4. Revenue chart — show breakdown by channel

Discussion questions:

- Who is the primary consumer of this dashboard? An analyst? An operations manager? An automated alert system?
- What refresh frequency does this data need? Can it be a batch job that runs hourly, or does it need to be sub-minute?
- Are there metrics missing from this view that your team asks for daily?

---

## 55–60 min: Production Considerations and Next Steps

**Objective:** Be honest about what the prototype does not address. Establish trust by naming the gaps yourself rather than waiting for the customer to find them.

Present the production considerations from `docs/technical-evaluation.md`.

Prioritize based on the customer's situation:

| If the customer said...                       | Prioritize...                                      |
| --------------------------------------------- | -------------------------------------------------- |
| "We have GDPR obligations"                    | PII classification, Unity Catalog column masking   |
| "We process millions of orders per day"       | Workload isolation, streaming ingestion design     |
| "Our operations team needs alerts"            | Pipeline observability, quarantine rate monitoring |
| "We need to reproduce last quarter's numbers" | Data retention policy, Delta time travel           |

Close with:

> This evaluation demonstrated that the technical approach is sound for your problem. The next step is defining which of these production considerations are blockers for your first production deployment, and scoping a Phase 2 design for the ones that are.

---

## Facilitator Notes

- Keep the business problem discussion to 10 minutes. The tendency is to spend 30 minutes on the problem and run out of time for the demo.
- If the customer's data quality problems are worse than the synthetic examples, that is a signal to spend more time on the Silver layer and quarantine visibility.
- The pipeline demo should be live, not a pre-recorded screen. Live demos surface real questions. Pre-recorded demos surface none.
- If a test fails during the demo, leave it visible and explain what it means. A failing test that you can explain is more credible than a passing test the customer cannot interpret.
