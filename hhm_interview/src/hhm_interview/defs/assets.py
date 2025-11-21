from dagster import asset
import polars as pl
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_PATH = PROJECT_ROOT /'data'/ 'products_for_one_day.parquet'



@asset(
    description="This assets allow to extract the products from the parquet file",
    io_manager_key="io_manager"
)
def extract_raw_products() -> pl.DataFrame:
    return pl.read_parquet(DATA_PATH)


@asset(
    description="This assets allow to get the products that are not out of the stock",
    io_manager_key="io_manager"
)
def no_out_of_stock_products(extract_raw_products: pl.DataFrame) -> pl.DataFrame:
    return extract_raw_products.filter(pl.col("quantity") > 0)


@asset(
    description="This assets allow to get products where date_type are not good"
                "Fresh products (F*) should have DLC, dry products (S*) should have DDM.",
    io_manager_key="io_manager"
)
def wrong_date_type_products(extract_raw_products: pl.DataFrame) -> pl.DataFrame:
    df = extract_raw_products.filter(
        (pl.col("boxNumber").str.starts_with("F") & (pl.col("date_type") != "DLC")) |
        (pl.col("boxNumber").str.starts_with("S") & (pl.col("date_type") != "DDM"))
    )
    return df


@asset(
    description="Products with incoherent pricing based on unit_price, oldPrice and newPrice",
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
