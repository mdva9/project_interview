import polars as pl
from dagster import build_asset_context
from hhm_interview.defs.assets import (
    no_out_of_stock_products,
    wrong_date_type_products,
    incorrect_pricing_products
)


def test_no_out_of_stock_products():
    # Arrange
    df = pl.DataFrame({"product": ["A", "B"], "quantity": [10, 0]})
    context = build_asset_context()

    # Act
    result = no_out_of_stock_products(context, df)

    # Assert
    assert result.shape[0] == 1
    assert result["product"][0] == "A"

############################################################################################

def test_wrong_date_type_products():
    # Arrange
    df = pl.DataFrame({
        "boxNumber": ["F123", "S456", "F789"],
        "date_type": ["DDM", "DDM", "DLC"]
    })
    context = build_asset_context()
    # Act
    result = wrong_date_type_products(context, df)

    #  F123 incorrect product (must be DLC)
    assert result.shape[0] == 1
    assert result["boxNumber"][0] == "F123"

############################################################################################
"""incorrect_pricing_products tests"""

def test_incorrect_pricing_products():
    # Arrange
    df = pl.DataFrame({
        "product": ["A", "B"],
        "unit_price": [10.0, 20.0],
        "unitQuantity": [1, 1],
        "oldPrice": [10.0, 20.0],
        "newPrice": [12.0, 20.0],  # Product A incorrect pricing
        "discount": [0.0, 0.0]
    })
    context = build_asset_context()

    # Act
    result = incorrect_pricing_products(context, df)

    # Assert
    assert result.shape[0] == 1
    assert result["product"][0] == "A"

