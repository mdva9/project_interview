from pathlib import Path
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = PROJECT_ROOT /'data'/ 'products_for_one_day.parquet'
OUTPUT_ONE = PROJECT_ROOT /'data'/ 'output'/ 'extract_raw_products.parquet'
OUTPUT_TWO = PROJECT_ROOT/'data'/ 'output'/ 'incorrect_pricing_products.parquet'
OUTPUT_THREE = PROJECT_ROOT/'data'/ 'output'/ 'no_out_of_stock_products.parquet'
OUTPUT_FOR = PROJECT_ROOT / 'data' / 'output'/ 'wrong_date_type_products.parquet'

def extract_output(path:Path) -> pl.DataFrame:
    return pl.read_parquet(path)


if __name__ == '__main__':
    print(extract_output(DATA_PATH).shape)
    print(extract_output(OUTPUT_ONE).shape)
    print(extract_output(OUTPUT_TWO).shape)
    print(extract_output(OUTPUT_THREE).shape)
    print(extract_output(OUTPUT_FOR).shape)

