from dagster import asset
import polars as pl
from pathlib import Path

FILE_PATH = Path(__file__).parent / 'data' / 'products_for_one_day.parquet'

@asset(
    description="This assets allow to extract the products from the parquet file",
    io_manager_key="io_manager"
)
def extract_raw_products() -> pl.DataFrame:
    return pl.read_parquet(FILE_PATH)


@asset(
    description="This assets allow to get the products that are not out of the stock",
    io_manager_key="io_manager"
)
def no_out_of_stock_products(extract_raw_products:pl.DataFrame) -> pl.DataFrame:
    return extract_raw_products.filter(pl.col("quantity") > 0)

@asset(
    description="This assets allow to get products where date_type are not good"
                "Fresh products (F*) should have DLC, dry products (S*) should have DDM.",
    io_manager_key="io_manager"
)
def wrong_date_type_products(extract_raw_products:pl.DataFrame) -> pl.DataFrame:
    return extract_raw_products.filter(
        (pl.col("boxNumber").str.starts_with("F")& (pl.col("date_type") != "DLC")) |
        (pl.col("boxNumber").str.starts_with("S") & (pl.col("date_type") != "DDM"))
    )

@asset(
    description="This assets allow to get products with incoherent pricing based on "
                "unit_quantity, oldPrice and newPrice",
    io_manager_key="io_manager"
)
def incorrect_pricing_products(extract_raw_products:pl.DataFrame) -> pl.DataFrame:
    return extract_raw_products.filter(
        (pl.col("newPrice") <= 0) |
        (pl.col("newPrice") > pl.col("oldPrice")) |
        ((pl.col("oldPrice") - pl.col("newPrice")) < 0) |
        (pl.col("unitQuantity") <= 0)
    )
