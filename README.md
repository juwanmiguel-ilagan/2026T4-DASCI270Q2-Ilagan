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

### Assets

The pipeline contains four assets:

raw_sales – reads the raw transactional sales data from the CSV source
clean_sales – removes duplicates, handles missing or invalid values, standardizes fields, and creates the derived sales_amount column
sales_by_region – aggregates the cleaned sales data by region and computes summary values such as total sales, total orders, and unique customers
daily_metrics – produces one final daily summary output containing KPIs such as total sales, total orders, unique customers, and top region by sales
Asset Dependency Flow

### The dependency chain is:

clean_sales depends on raw_sales
sales_by_region depends on clean_sales
daily_metrics uses both clean_sales and sales_by_region
Configurable Resource

The pipeline defines a configurable resource named sales_io.

This resource is used for:

reading the input CSV
writing output files such as cleaned sales data, regional summaries, and daily metrics
Asset Checks

### Two asset checks are included:

clean_sales_quality_check – validates the quality of the cleaned data
daily_metrics_business_rule_check – validates that the final KPI output is logically correct
Automation

### A daily schedule is used for pipeline automation.
A schedule was chosen because this is a daily reporting pipeline with a fixed cadence, so a time-based trigger is more appropriate than a sensor.

### Main Part A Files
src/dagster_tutorial/defs/assets.py
src/dagster_tutorial/defs/resources.py
src/dagster_tutorial/defs/schedules.py
src/dagster_tutorial/definitions.py


Part B – GitHub Actions CI/CD Workflow

For Part B, I created a GitHub Actions workflow to make the ML pipeline safer and more reliable. The workflow is designed to reduce the risk of schema changes breaking the nightly batch job and bad artifacts being promoted to staging without enough validation.

### Workflow File

The workflow file is located at:
.github/workflows/mlops-ci.yaml
Workflow Triggers

The workflow is configured to run in the following situations:

Pull requests to main when files under pipelines/**, assets/**, or config/** change
Nightly schedule at 2:00 AM
Manual execution from the GitHub Actions UI using workflow_dispatch
Workflow Jobs

### The workflow includes the following jobs:

static_checks – runs fast checks before deeper validation
test_and_validate – runs unit tests, schema/data validation, and a lightweight smoke test
package_artifact – builds and uploads one trusted artifact for later deployment
deploy_staging – deploys only after packaging succeeds, only on push to main, and uses environment: staging; it also includes a placeholder deploy step and a post-deployment smoke test
Guardrails

### The workflow includes CI/CD guardrails such as:

concurrency control to avoid overlapping runs
minimal permissions by default

### These help reduce common CI/CD risks and make the workflow safer.

### Part B Support Files
.github/workflows/mlops-ci.yaml
config/expected_sales_schema.json
pipelines/README.md
assets/README.md
tests/test_placeholder.py
