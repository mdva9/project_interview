"""
Module: assets.py
=================

This module defines several Dagster assets for product data extraction and validation.
The assets use Polars to process DataFrames and enforce business rules regarding stock,
date types, and pricing consistency.

Overall workflow:
1. Extract raw products from a Parquet file.
2. Filter products that are in stock.
3. Identify products with incorrect date types.
4. Detect products with incoherent pricing.
"""


from dagster import asset
import polars as pl
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_PATH = PROJECT_ROOT /'data'/ 'products_for_one_day.parquet'



@asset(
    description="""Extracts raw product data from the Parquet file located in the `data/` directory.
                    Returns a Polars DataFrame with all products.""",
    io_manager_key="io_manager"
)
def extract_raw_products() -> pl.DataFrame:
    return pl.read_parquet(DATA_PATH)


@asset(
    description="""Filters products that are not out of stock.
                    Keeps only rows where `quantity > 0`.""",
    io_manager_key="io_manager"
)
def no_out_of_stock_products(extract_raw_products: pl.DataFrame) -> pl.DataFrame:
    return extract_raw_products.filter(pl.col("quantity") > 0)


@asset(
    description="""Identifies products with incorrect date types based on `boxNumber`.
                   Fresh products (boxNumber starting with "F") must have `date_type = DLC`.
                   Dry products (boxNumber starting with "S") must have `date_type = DDM`.""",
    io_manager_key="io_manager"
)
def wrong_date_type_products(extract_raw_products: pl.DataFrame) -> pl.DataFrame:
    df = extract_raw_products.filter(
        (pl.col("boxNumber").str.starts_with("F") & (pl.col("date_type") != "DLC")) |
        (pl.col("boxNumber").str.starts_with("S") & (pl.col("date_type") != "DDM"))
    )
    return df


@asset(
    description="""  Detects products with incoherent pricing.
                     Rules:
                          * `oldPrice` should equal `unit_price × unitQuantity` (within tolerance 0.01).
                          * `newPrice` should equal `oldPrice × (1 - discount)` (within tolerance 0.01).
                     Returns a DataFrame of products failing these checks.""",
    io_manager_key="io_manager"
)
def incorrect_pricing_products(extract_raw_products: pl.DataFrame) -> pl.DataFrame:

    df = extract_raw_products.with_columns(
        pl.col("unit_price").cast(pl.Float64)
    )

    return df.filter(
        (
                ((pl.col("oldPrice") - (pl.col("unit_price") * pl.col("unitQuantity"))).abs() > 0.01) |
                ((pl.col("newPrice") - (pl.col("oldPrice") * (1 - pl.col("discount")))).abs() > 0.01)

        )
    )
