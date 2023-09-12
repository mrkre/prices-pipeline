import dlt

from pipelines.eod_source import eod_source


pipeline = dlt.pipeline(
    pipeline_name="us_stocks_pipeline",
    destination="duckdb",
    dataset_name="us_stocks",
)


def update_us_stocks_pipeline():
    pipeline.run(eod_source(), loader_file_format="parquet")
