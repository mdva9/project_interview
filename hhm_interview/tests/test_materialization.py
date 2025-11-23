import polars as pl
from dagster import materialize
from dagster import mem_io_manager
from hhm_interview.defs import assets


def test_materialize_all_assets():

    result = materialize(
        [
            assets.extract_raw_products,
            assets.no_out_of_stock_products,
            assets.wrong_date_type_products,
            assets.incorrect_pricing_products,
        ],
        resources={"io_manager": mem_io_manager},
    )

    # Assert
    assert result.success

    raw = result.output_for_node("extract_raw_products")
    assert isinstance(raw, pl.DataFrame)

    no_out = result.output_for_node("no_out_of_stock_products")
    assert isinstance(no_out, pl.DataFrame)

    wrong_date = result.output_for_node("wrong_date_type_products")
    assert isinstance(wrong_date, pl.DataFrame)

    incorrect_price = result.output_for_node("incorrect_pricing_products")
    assert isinstance(incorrect_price, pl.DataFrame)
