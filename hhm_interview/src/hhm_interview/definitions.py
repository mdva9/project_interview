"""
Module : definitions.py
=================

This module defines the Dagster project definitions, including assets and resources.

Purpose:
--------
Dagster requires a central place to declare which assets and resources are part of
the project. This file serves as that entry point.

Steps:
1. `load_from_defs_folder` loads all asset definitions from the current folder.
2. These assets are collected into a `Definitions` object.
3. The `io_manager` resource is registered so that all assets can use it for
   reading and writing Parquet files via Polars.

Usage:
------
- Place this file at the root of your Dagster project (inside `src/hhm_interview`).
- Dagster will automatically discover and use the definitions when running jobs
  or materializing assets.
- The `io_manager` ensures consistent data handling across all assets.

Summary:
--------
This file acts as the configuration hub for Dagster, connecting:
- Assets (business logic for data transformations).
- Resources (IO manager for data persistence).
"""


from pathlib import Path

from dagster import definitions, Definitions,load_from_defs_folder

from hhm_interview.defs.resources import io_manager


@definitions
def defs():
    load_defs = load_from_defs_folder(path_within_project=Path(__file__).parent)

    return Definitions(
        load_defs.assets,
        resources={"io_manager":io_manager}
    )
