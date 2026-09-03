# Online Retail Medallion Pipeline (Databricks)
<img width="1187" height="606" alt="image" src="https://github.com/user-attachments/assets/8dd08ff1-26c4-4b3d-a8dd-150b529770fc" />

A Bronze → Silver → Gold data pipeline built on Databricks Free Edition, using the `online-retail` sample dataset from Unity Catalog's `samples` catalog. Built as a focused, fast-turnaround project to get hands-on with Spark, Delta Lake, and Databricks Job scheduling. This is a companion piece to a separate, longer-running Airflow/Docker project (FragTracker).

**Dataset:** ~7 weeks of UK e-commerce transaction data (Dec 1, 2010 - Jan 20, 2011), read from `samples.databricks.online_retail` via Unity Catalog Volumes.

**Stack:** PySpark, Spark SQL, Delta Lake, Databricks Jobs, Databricks SQL Dashboards.

---

## Architecture

```
Bronze (raw) → Silver (cleaned, validated) → Gold (business metrics)
```

Each layer is a persisted Delta table in the `retail_project` catalog, not just notebook variables, so the pipeline is reproducible and can be scheduled to run independently of any single notebook session.

### Bronze - `retail_project.bronze`

Raw ingestion of the source CSV via `spark.read.csv(...)`, written out as-is with minimal transformation. This preserves the original data exactly as received, which matters both as an audit trail and as a fallback if a downstream transformation turns out to be wrong.

### Silver - `retail_project.silver`

Cleaning and validation applied on top of Bronze:

- **`InvoiceDate` cast to `timestamp`.** The raw column was a string in `M/d/yy H:m` format. Getting this right took a few iterations, Spark's datetime pattern language distinguishes `D` (day-of-year) from `d` (day-of-month), and using the wrong one silently produced internally inconsistent parsed dates before erroring out entirely. Also had to swap `"datetime"` (invalid) for the correct Spark type name, `TimestampType`.
- **`InvoiceNo` kept as `string`, not cast to an integer.** The dataset uses a `C` prefix (e.g. `C536379`) to mark cancelled orders, casting to int would have silently destroyed that signal.
- **`Cancelled` flag added**, derived from `Quantity < 0` rather than the `InvoiceNo` prefix directly. Verified that the two signals agree on this dataset, but quantity is the more direct and robust check.
- **Nulls handled selectively.** `dropna()` scoped to `Quantity` and `UnitPrice` only (columns that make a row unusable for revenue math) rather than dropped blanket-wide. `CustomerID` has ~25,000 nulls but was left as-is - those rows are still valid for revenue totals, just excluded from any customer-level aggregation.
- **Duplicates removed** after confirming, by inspection, that the duplicate rows were exact full-row matches rather than legitimate repeat line items.

### Gold - three purpose-built tables

Each answers a specific business question, built from Silver. Non-cancelled rows only, unless the metric is specifically about cancellations.

- **`gold_top_products`** - top 5 products by revenue. Built in both Spark SQL and PySpark (as a deliberate side-by-side exercise). Filtered out non-product line items (postage, manual adjustments, Amazon fees) using a regex check for the presence of a digit in `StockCode`, since real product codes are digit-heavy and operational line items are not.
- **`gold_revenue_by_day`** - daily revenue, cancelled orders excluded. Built and cross-verified in both SQL and PySpark; results matched, which was a useful confirmation that the logic was correct in both.
- **`gold_cancellation_rate`** - cancelled revenue as a percentage of non-cancelled revenue (15.03% on this data slice).

---

## Job Scheduling

A single-task Databricks Job runs the full notebook (Bronze → Silver → Gold) on a **daily** schedule. Chose daily as a realistic simulation of a production batch pipeline processing the previous day's transactions each morning - not because this specific static dataset changes day to day.

**A bug worth documenting:** the initial write mode for Bronze and Silver was `append`. That's the right choice for a source that receives genuinely new data on each run - but this project reads the same static sample file every time, so `append` caused the same rows to be duplicated on every scheduled run. Switched both to `overwrite`, since each run is meant to fully recompute the layer from the current source, not accumulate on top of previous runs.

**A second bug worth documenting:** an early sanity check computed `MIN`/`MAX` on `InvoiceDate` directly against the Bronze table, where the column is still a string, this produced a nonsensical result (the "max" date sorted earlier than the "min" date) because string comparison is lexicographic, not chronological. Re-running the same check against Silver (where `InvoiceDate` is a proper `timestamp`) gave the correct range. A concrete example of why Bronze stays raw and Silver exists to fix exactly this kind of type issue before any analysis happens.

---

## Dashboard

A Databricks SQL Dashboard with three tiles, each reading directly from a Gold table:

- **Cancellation Rate** - single KPI figure
- **Revenue Per Day** - line chart, time series
- **Top 5 Products by Revenue** - bar chart

---

## What I'd do differently in production

- Split Bronze/Silver/Gold into separate notebooks as distinct tasks in a multi-task Job, with explicit dependencies, kept it to a single notebook here to fit the project's timeline, but task-level separation would give better failure isolation and monitoring.
- Use `append` with proper incremental/watermark logic if the source were a genuinely daily-refreshing feed rather than a static sample.
- Add data quality checks as their own step (e.g. row count thresholds, null-rate alerts) rather than one-off manual verification queries.
