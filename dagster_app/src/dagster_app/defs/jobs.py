import dagster as dg
from .assets import (
    fake_meteo_data
)

# Hello world job
fake_meteo_data_job = dg.define_asset_job(
    name="fake_meteo_data_job", 
    selection=[fake_meteo_data]
)