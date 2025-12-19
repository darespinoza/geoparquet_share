import dagster as dg
from .jobs import (
    fake_meteo_data_job,
)

# Raw DB Spatio-temp DB schedule
# mssql_pg_utlils_schedule = dg.ScheduleDefinition(
#     job=fake_meteo_data_job,
#     cron_schedule="0 0 * * *", # At 08:00 GMT-5
#     execution_timezone="America/Guayaquil",
# )

# Campbell daily partition Schedule
fake_meteo_data_schedule = dg.build_schedule_from_partitioned_job(
    job=fake_meteo_data_job,
    hour_of_day=0,
)

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        schedules=[
            fake_meteo_data_schedule,
        ]
    )