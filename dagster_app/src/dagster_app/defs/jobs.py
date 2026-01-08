import dagster as dg
from .assets import (
    fake_meteo_data,
)
from .geoparquet import(
    geoparquet_assets,
)

# Fake meteo job
fake_meteo_data_job = dg.define_asset_job(
    name="fake_meteo_data_job", 
    selection=[fake_meteo_data]
)

# Postgres to Minio job
pg_to_minio_job = dg.define_asset_job(
    name="pg_to_minio_job",
    selection=dg.load_assets_from_modules(modules=[geoparquet_assets]),
)


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        jobs=[
            fake_meteo_data_job,
            pg_to_minio_job,
        ]
    )