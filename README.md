# Online Retail Medallion Pipeline (Databricks)
<img width="1187" height="606" alt="image" src="https://github.com/user-attachments/assets/8dd08ff1-26c4-4b3d-a8dd-150b529770fc" />

A Bronze to Silver to Gold pipeline built on Databricks Free Edition, using the `online-retail` sample dataset from Unity Catalog's `samples` catalog. Built as a focused, hands-on project to get real Spark and Delta Lake experience.

**Dataset:** ~7 weeks of UK e-commerce transactions (Dec 1, 2010 – Jan 20, 2011).
**Stack:** PySpark, Spark SQL, Delta Lake, Databricks Jobs, Databricks SQL Dashboards.

## Architecture

Each layer is a persisted Delta table (`retail_project.bronze/silver/gold_*`), not just an in-notebook DataFrame. That means the pipeline is reproducible and can run as a scheduled Job independently of any one session.

**Bronze**: raw ingestion via `spark.read.csv()`, landed as-is.

**Silver**: cleaned and validated:
- `InvoiceDate` cast to `timestamp` (raw format was `M/d/yy H:m`; had to work through Spark's datetime pattern syntax, including a `D` vs `d` bug, day-of-year vs day-of-month, that produced internally inconsistent parsed dates before failing outright)
- `InvoiceNo` kept as `string`, not cast to int, to preserve the `C`-prefix that marks cancelled orders
- `Cancelled` flag derived from `Quantity < 0`, cross-checked against the `C`-prefix pattern to confirm they agree
- Checked `Quantity`/`UnitPrice` for nulls before trusting them in revenue math; both came back with zero nulls, so no drop was needed there. `CustomerID` has around 25k nulls but was left as-is, since those rows are still valid for revenue totals, just not for customer-level breakdowns
- Duplicates removed after confirming by inspection they were exact full-row matches, not legitimate repeat line items

**Gold**: three tables, each answering a specific business question. Cancelled orders are excluded from revenue math unless the metric is about cancellations:
- `gold_top_products`: top 5 by revenue, in PySpark. Filtered out non-product line items like postage and fees using a digit-presence check on `StockCode`
- `gold_revenue_by_day`: daily revenue, in PySpark
- `gold_cancellation_rate`: cancelled revenue as % of non-cancelled revenue (15.03%), written as a `spark.sql()` query

## Job Scheduling

A single-task Databricks Job runs the notebook end to end on a daily schedule. Daily was chosen as a realistic cadence for a production batch pipeline, even though this specific static dataset doesn't actually change day to day.

**Bug caught along the way:** Bronze/Silver were initially set to `append` mode. Since the source data never changes between runs, this silently duplicated the entire dataset on every scheduled execution. Switched to `overwrite`, since each run should fully recompute the layer from source rather than accumulate on top of prior runs.

**Second bug:** an early `MIN`/`MAX` check on `InvoiceDate` run against Bronze (still a string column) gave a nonsensical result, because string comparison is lexicographic, not chronological. Re-running against Silver, where the column is a proper `timestamp`, gave the correct range. It's a good concrete example of why Bronze stays raw and Silver exists to fix exactly this kind of issue before analysis.

## Dashboard

A Databricks SQL Dashboard with three tiles reading directly from the Gold tables: a cancellation rate KPI card, a daily revenue line chart, and a top-5-products bar chart.

## What I'd do differently in production

- Split Bronze/Silver/Gold into separate notebooks as distinct Job tasks with explicit dependencies, for better failure isolation
- Use `append` with real incremental logic against a genuinely daily-refreshing source, rather than `overwrite`
- Add dedicated data quality checks (row counts, null-rate thresholds) instead of one-off manual verification queries
