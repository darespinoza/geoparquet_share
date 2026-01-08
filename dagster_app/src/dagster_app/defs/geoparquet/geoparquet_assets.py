import yaml
import dagster as dg

from jinja2 import Template
from dagster_aws.s3 import S3Resource
from ..resources import (
    PostgresResource,
)
from ..constants import (
    BUCKET_NAME,
    OBJECT_TEMPLATE,
    CURSOR_CHUNK_SIZE,
)
from ..tools import (
    pg_to_minio_geoparquet,
)

# Minio partition
minio_daily_partition = dg.DailyPartitionsDefinition(
    start_date="2025-12-01",
    timezone="America/Guayaquil",
)

# Dynamic asset creation
def create_pg_to_minio_asset(asset_name: str,
                            sql_query: str,
                            prefix_schema: str,
                            prefix_table: str,
                        ):
    
    # Dynamic asset definition
    @dg.asset(name=f"{asset_name}_to_minio",
            group_name='pg_to_minio',
            partitions_def=minio_daily_partition,
    )
    def dynamic_asset(context: dg.AssetExecutionContext,
                    postgres_res: PostgresResource,
                    minio_res: S3Resource,
                ) -> None:
        
        try:
            # Get partition time wiwndow start
            current_window = context.partition_time_window
            partition_start = current_window.start
            query_timestamp = partition_start.strftime("%Y-%m-%d %H:%M:%S")
            
            # Get prefix parts from current partition
            prefix_year = partition_start.strftime("%Y")
            prefix_month = partition_start.strftime("%m")
            prefix_day = partition_start.strftime("%d")
            
            # Get postgres read engine
            pg_engine = postgres_res.get_engine()
            
            # Prepare object key and prefix
            sql_template = Template(sql_query).render(partition=query_timestamp)
            object_prefix = f"{prefix_schema}/{prefix_table}/year={prefix_year}/month={prefix_month}/day={prefix_day}"
            
            # Query to database, create geoparquet and upload to MinIO
            pg_to_minio_geoparquet(sql_query=sql_template,
                                object_prefix=object_prefix,
                                bucket_name=BUCKET_NAME,
                                object_template=OBJECT_TEMPLATE,
                                pg_engine=pg_engine,
                                minio_res=minio_res,
                                chunk_size=CURSOR_CHUNK_SIZE)
            
            # Finish asset execution
            pass
        
        except Exception as exc:
            context.log.error(f"Error while loading to MinIO'{asset_name}'.\n{str(exc)}")
            return None
    
    # Return new asset
    return dynamic_asset

# Read Postgres to Minio Public Pipelines configuration file
config_path = dg.file_relative_path(__file__, "pipelines.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# Create assets dynamically
assets = []
for pipeline in config["pipelines"]:
    assets.append(create_pg_to_minio_asset(asset_name=pipeline['asset_name'],
                    sql_query=pipeline['sql_query'],
                    prefix_schema=pipeline['prefix_schema'],
                    prefix_table=pipeline['prefix_table'],
                ))