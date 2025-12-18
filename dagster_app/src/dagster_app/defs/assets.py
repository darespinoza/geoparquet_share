import dagster as dg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from sqlalchemy import create_engine
from .resources import PostgresResource
from .tools import (
    generate_fake_meteo_data,
)



@dg.asset
def fake_meteo_data(context: dg.AssetExecutionContext,
                    postgres_res: PostgresResource) -> None:
    """
    Write a DataFrame to PostgreSQL.
    Handling exceptions in a very simplified manner.
    """
    
    try:
        # Generate fake meteo data for a station located in Cuenca Ecuador
        df = generate_fake_meteo_data(station_id="UAZ001",
                                latitude=-2.8953,
                                longitude=-78.9963)
        
        # Get engine and create 
        engine = postgres_res.get_engine()
        
        # Write DataFrame to PostgreSQL
        table_name = 'fake_meteo_data'
        df.to_sql(
                name=table_name,
                con=engine,
                schema='public',
                if_exists='append',
                index=False
            )
        
        # Dipose engine after use
        engine.dispose()
        context.log.info(f"Successfully wrote {len(df)} rows to {table_name} table")
    except Exception as exc:
        context.log.error(f"While creating fake meteo data\n{str(exc)}")
