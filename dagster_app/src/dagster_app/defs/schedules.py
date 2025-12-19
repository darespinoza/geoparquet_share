import dagster as dg
from .jobs import (
    fake_meteo_data_job,
    pg_to_minio_job,
)

# Campbell daily partition Schedule
fake_meteo_data_schedule = dg.build_schedule_from_partitioned_job(
    job=fake_meteo_data_job,
    hour_of_day=0,
)

# Postgres to Minio Private Schedule
pg_to_minio_schedule = dg.build_schedule_from_partitioned_job(
    job=pg_to_minio_job,
    hour_of_day=1,
)

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        schedules=[
            fake_meteo_data_schedule,
            pg_to_minio_schedule,
        ]
    )