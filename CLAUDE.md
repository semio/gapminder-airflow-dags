# CLAUDE.md

## Project Overview

This repository contains Apache Airflow DAGs for automatically building and updating DDF datasets in the open-numbers GitHub organization. The DAGs manage ETL pipelines for Gapminder's data processing workflows.

## Repository Structure

```
├── dags/                    # Airflow DAG definitions
│   ├── datasets/            # Auto-generated DAGs for individual datasets
│   ├── update_all_datasets.py   # Main daily DAG: clones repos, generates dataset DAGs
│   ├── refresh_dags.py      # Manual DAG refresh (simpler version)
│   ├── airflow-log-cleanup.py   # Log cleanup utility DAG
│   └── server-tmp-cleanup.py    # Temp file cleanup utility DAG
├── templates/               # Jinja2 templates for generating dataset DAGs
│   ├── etl_recipe.py        # Standard ETL with recipe/python processing
│   ├── etl_recipe_auto.py   # Auto datasets with git merge/push
│   ├── etl_recipe_production.py  # Production deployment (merge + push)
│   ├── check_source_only.py # Source checking only (no ETL)
│   ├── manual_update.py     # Manually maintained datasets
│   └── etl_recipe_variable_filled.py  # Reference with filled values
└── plugins/                 # Custom Airflow operators
    └── ddf_operators.py     # DDF-specific operators and sensors
```

## Airflow Version

This project uses **Apache Airflow 3.x**. Key import patterns:

```python
from airflow.sdk import DAG, Variable, TaskGroup
from airflow.hooks.base import BaseHook
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
```

## Templates

Templates in `templates/` are **Jinja2 templates**, not valid Python files. They contain `{{ variable }}` placeholders that get rendered by the DAG generation code. Do not run ruff or other linters directly on template files.

Template variables include:
- `{{ name }}` - Dataset name (e.g., "open-numbers/ddf--gapminder--example")
- `{{ datetime }}` - Start date as `datetime(year, month, day)` constructor call (requires `datetime` import)
- `{{ schedule }}` - Cron schedule string
- `{{ priority }}` - Task priority weight
- `{{ etl_type }}` - ETL type ("recipe", "python", or "manual")
- `{{ dependencies }}` - Dict of dependency datasets

**Important**: Since `{{ datetime }}` expands to `datetime(...)`, templates must import `datetime` from the datetime module: `from datetime import datetime, timedelta`

## Custom Operators (plugins/ddf_operators.py)

Key operators:
- `DependencyDatasetSensor` - Waits for external DAG task using XCom
- `GitPullOperator`, `GitPushOperator`, `GitCommitOperator`, `GitMergeOperator`
- `GitCheckoutOperator`, `GitResetOperator`, `GitResetAndGoMasterOperator`
- `RunETLOperator` - Runs etl.py script
- `UpdateSourceOperator` - Runs update_source.py if present
- `GenerateDatapackageOperator` - Generates datapackage.json
- `ValidateDatasetOperator` - Validates DDF dataset

## Cross-DAG Dependencies

DAGs emit XCom `last_task_run_time` at the start of each run. `DependencyDatasetSensor` uses this to wait for the most recent successful run of dependency DAGs.

## Required Airflow Variables

- `airflow_home` - Path to AIRFLOW_HOME
- `datasets_dir` - Path where datasets are stored
- `with_production` - Newline-separated list of datasets with production DAGs
- `automatic_datasets` - Newline-separated list of auto-push datasets
- `check_source_datasets` - Newline-separated list of source-check-only datasets
- `custom_schedule` - JSON dict of custom schedules per dataset
- `GSPREAD_PANDAS_CONFIG_DIR` - Config directory for Google Sheets access

## Development

### Setup
```bash
./install-dev-dependencies.sh
```

### Code Quality
- Use `ruff check` and `ruff format` for Python files in `dags/` and `plugins/`
- Do NOT run ruff on template files (they are Jinja2, not valid Python)

### Package Management
- Uses `uv` for package management
- Uses `pyenv` for Python version management
