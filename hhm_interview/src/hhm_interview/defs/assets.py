from dagster import asset
import polars as pl
from pathlib import Path

FILE_PATH = Path(__file__).parent / 'data' / 'products_for_one_day.parquet'

@asset(
    descritpion="This assets allow to extract the products from the parquet file"
)
def extract_raw_products() -> pl.DataFrame:
    return pl.read_parquet(FILE_PATH)


@asset(
    descritpion="This assets allow to get the products that are not out of the stock"
)
def no_out_of_stock_products(extract_raw_products:pl.DataFrame) -> pl.DataFrame:
    return extract_raw_products.filter(pl.col("quantity") > 0)

@asset(
    description="This assets allow to get products where date_type are not good"
                "Fresh products (F*) should have DLC, dry products (S*) should have DDM."
)
def wrong_date_type_products(extract_raw_products:pl.DataFrame) -> pl.DataFrame:
    return extract_raw_products.filter(
        (pl.col("boxNumber").str.stars_with("F")& (pl.col("date_type") != "DLC")) |
        (pl.col("boxNumber").str.stars_with("S") & (pl.col("date_type") != "DDM"))
    )