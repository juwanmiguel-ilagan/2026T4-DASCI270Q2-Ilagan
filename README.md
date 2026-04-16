# 2026T4-DASCI270Q2-Ilagan

This repository contains my solutions for both parts of the assignment:

- **Part A – Dagster Data Pipeline**
- **Part B – GitHub Actions CI/CD Workflow**

The project is based on a daily sales analytics scenario. The goal is to make the pipeline more reliable in two ways: first, by improving the pipeline itself using Dagster assets, checks, and scheduling; and second, by adding a CI/CD workflow in GitHub Actions to catch problems before changes are deployed.

---

## Part A – Dagster Data Pipeline

For Part A, I built a Dagster asset-based pipeline that reads daily sales data from a CSV file, cleans and validates the data, generates regional summaries, and produces one final daily KPI report.

### Input Data

The input CSV file used by the pipeline is:

```text
src/data/sales_2026_04_14.csv
