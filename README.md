## Part A – Dagster Sales Pipeline

Main files:
- `src/dagster_tutorial/defs/assets.py`
- `src/dagster_tutorial/defs/resources.py`
- `src/dagster_tutorial/defs/schedules.py`
- `src/dagster_tutorial/definitions.py`

Input data:
- `src/data/sales_2026_04_14.csv`

How to run:
1. Activate the virtual environment
2. Run `dg check defs`
3. Run `dg dev`
4. Open `http://127.0.0.1:3000`
5. Verify assets in Catalog and Lineage
6. Run checks and enable the schedule