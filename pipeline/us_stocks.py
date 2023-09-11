import dlt
from os import getenv

EODHD_API_KEY = getenv("EODHD_API_KEY")

pipeline = dlt.pipeline(
    pipeline_name="us_stocks_pipeline",
    destination="duckdb",
    dataset_name="us_stocks",
)
