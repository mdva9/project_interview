"""
Module: resources.py
=================

This module defines the IO manager used by Dagster to handle product data
storage and retrieval in Parquet format with Polars.

Purpose:
--------
Dagster assets often produce or consume data tables. Instead of manually
reading and writing files, an IO manager centralizes this logic. Here we
use `PolarsParquetIOManager` to automatically save asset outputs as
Parquet files and reload them when needed.

Configuration:
--------------
- `base_dir="data/output"` specifies the directory where all Parquet
  files will be stored.
- Each asset that uses this IO manager will have its output written
  into this folder, named according to the asset key.
- When another asset depends on it, the IO manager will read the
  corresponding Parquet file back into a Polars DataFrame.
"""
from dagster_polars import PolarsParquetIOManager

# IO manager that saves and loads asset data as Parquet files in data/output
io_manager = PolarsParquetIOManager(base_dir="data/output")