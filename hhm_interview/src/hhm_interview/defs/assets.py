from dagster import asset
import polars as pl
from pathlib import Path

FILE_PATH = Path(__file__).parent / 'data' / 'products_for_one_day.parquet'

@asset(
    descritpion="This assets allow to extract the products from the parquet file"
)
def extract_raw_products():
    return pl.read_parquet(FILE_PATH)


