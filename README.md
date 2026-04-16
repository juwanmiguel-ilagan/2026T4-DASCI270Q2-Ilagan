# 2026T4-DASCI270Q2-Ilagan

This repository contains the solutions for:

- **Part A – Dagster Data Pipeline**
- **Part B – GitHub Actions Workflow**

---

## Part A – Dagster Sales Analytics Pipeline

This part implements a Dagster asset-based pipeline for daily regional sales analytics.

### What the pipeline does

The pipeline reads daily sales data from a CSV file, cleans and validates the data, produces regional summaries, and generates one final daily KPI output for reporting.

### Assets

The pipeline includes four assets:

- **`raw_sales`**  
  Reads the raw transactional sales CSV file.

- **`clean_sales`**  
  Cleans the raw data by removing duplicates, handling missing or invalid values, and creating a derived `sales_amount` column.

- **`sales_by_region`**  
  Aggregates cleaned sales data by region and computes summary values such as total sales and total orders.

- **`daily_metrics`**  
  Produces one daily summary output containing KPIs such as total sales, total orders, unique customers, and top region by sales.

### Dependency flow

The asset dependency chain is:

- `clean_sales` depends on `raw_sales`
- `sales_by_region` depends on `clean_sales`
- `daily_metrics` uses both `clean_sales` and `sales_by_region`

### Resource

The pipeline defines a configurable Dagster resource named **`sales_io`**.

This resource is used for:

- reading the sales CSV input
- writing output files such as cleaned data, regional summaries, and daily metrics

### Asset checks

Two asset checks are included:

- **`clean_sales_quality_check`**  
  Verifies data quality in the cleaned sales dataset.

- **`daily_metrics_business_rule_check`**  
  Verifies that the final KPI output is valid and follows business rules.

### Automation

A **daily schedule** is included for pipeline automation.

A schedule was chosen because the pipeline is designed for recurring daily sales reporting with a fixed cadence.

### Input data

The input CSV file is located at:

```text
src/data/sales_2026_04_14.csv
