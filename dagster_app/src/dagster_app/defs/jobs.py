import dagster as dg
from .assets import (
    fake_meteo_data
)

# Fake meteo job
fake_meteo_data_job = dg.define_asset_job(
    name="fake_meteo_data_job", 
    selection=[fake_meteo_data]
)

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        jobs=[
            fake_meteo_data_job,
        ]
    )