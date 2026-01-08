import dagster as dg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from sqlalchemy import create_engine
from .resources import PostgresResource
from .tools import (
    generate_fake_meteo_data,
)

# Fake meteo data DailyPartition
fake_meteo_daily_partition = dg.DailyPartitionsDefinition(
    start_date="2025-01-01",
    timezone="America/Guayaquil",
)

@dg.asset(
    partitions_def=fake_meteo_daily_partition,
)
def fake_meteo_data(context: dg.AssetExecutionContext,
                    postgres_res: PostgresResource) -> None:
    """
    Write a DataFrame to PostgreSQL.
    
    Handling exceptions in a very simplified manner.
    """ 
    
    # Engine variable
    engine = None
    
    try:
        # Get partition bounds
        start, end = context.partition_time_window
        partition_start = start.replace(tzinfo=None)
        
        # Get SQLAlchemy engine
        engine = postgres_res.get_engine()
        
        # Generate fake meteo data for three stations located in Cuenca Ecuador
        table_name = 'meteo_data'
        
        # Station 1, located in Chola Cuencana 
        ir_1 = generate_fake_meteo_data(timestamp=partition_start,
                                station_id="CUE-001",
                                latitude=-2.8953,
                                longitude=-78.9963,
                                table_name=table_name,
                                engine=engine,
                                schema_name='public'
                            )
        context.log.info(f"Successfully wrote {ir_1} rows to {table_name} table")
        
        # Station 2, located in Ictocruz Park
        ir_2 = generate_fake_meteo_data(timestamp=partition_start,
                                station_id="CUE-002",
                                latitude=-2.9303,
                                longitude=78.9997,
                                table_name=table_name,
                                engine=engine,
                                schema_name='public'
                            )
        context.log.info(f"Successfully wrote {ir_2} rows to {table_name} table")
        
        # Station 3, located in Cebollar Park
        ir_3 = generate_fake_meteo_data(timestamp=partition_start,
                                station_id="CUE-003",
                                latitude=-2.8805,
                                longitude=-79.0268,
                                table_name=table_name,
                                engine=engine,
                                schema_name='public'
                            )
        context.log.info(f"Successfully wrote {ir_3} rows to {table_name} table")
        
    except Exception as exc:
        context.log.error(f"While creating fake meteo data\n{str(exc)}")
    finally:
        # Dipose engine
        if engine:
            engine.dispose()
