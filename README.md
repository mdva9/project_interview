# 🧪 HHM Product Validation Pipeline with Dagster
This project showcases a local implementation of a data validation pipeline using Dagster, Polars, and uv as the package manager. The pipeline ingests raw product data from a Parquet file, applies a series of business validation rules, and writes the filtered datasets to disk.
This project was developed as part of a technical interview task. The goal is to demonstrate clean orchestration logic, modular asset design, and robust testing using Dagster in a fully local environment.
---

## 📂 Project Structure
```
project_interview/
│
├── hhm_interview/
│   ├── data/
│   │   ├── products_for_one_day.parquet        # Raw input data
│   │   └── output/                             # Validated outputs
│   │       ├── extract_raw_products.parquet
│   │       ├── no_out_of_stock_products.parquet
│   │       ├── wrong_date_type_products.parquet
│   │       └── incorrect_pricing_products.parquet
│   │
│   ├── src/
│   │   └── hhm_interview/
│   │       ├── defs/
│   │       │   ├── assets.py                   # Dagster asset definitions
│   │       │   ├── resources.py                # IO manager config
│   │       │   └── definitions.py              # Dagster Definitions object
│   │       └── __init__.py
│   │
│   └── tests/
│       ├── test_assets.py                      # Unit tests for asset logic
│       └── test_materialization.py             # Dagster materialization tests
│
├── README.md
├── pyproject.toml                              # uv project configuration
├── uv.lock                                     # uv lockfile
└── .gitignore
```
---

## 🚀 Pipeline Architecture

**Steps:**
1. **Extract** raw product data from a Parquet file
2. **Validate** products:
    - Filter out products that are out of stock
    - Detect incorrect date types based on product category
    - Identify incorrect pricing based on business rules
3. **Store** each validated dataset as a separate Parquet file in **data/output/**
---

## ⚙️ Local Setup

### 1. Prerequisites
- Python 3.12+
- **uv** installed (package manager)

### 2. Clone the repository

```bash
git clone <your-repo-url>
cd project_interview/
```
### 3. Install dependencies with uv

This command will :

- create a lightweight virtual environment

- install Dagster, Dagit, Polars, PyArrow

- configure the packaged project structure defined in pyproject.toml

```bash
uv sync
```

### Testing

## Unit tests
Run tests for asset logic:
```bash
uv run pytest hhm_interview/tests/tests_assets.py
```
## Dagster materialization tests
Run Dagster tests to validate full pipeline execution:
```bash
uv run pytest hhm_interview/tests/test_materialization.py

## Pipeline Execution
These commands allow you to execute the pipeline in two different ways.
```bash
cd hhm_interview/
```

### Dagit UI
Start the Dagster development server:
```bash
uv run dagster dev
```
Then open:
 http://127.0.0.1:3000

- Click on Materialize an Asset (top-right)
- Click on Materialize all
- Click on view to see the pipeline run details (bottom-right)

