import os, tempfile
import pandas as pd
import numpy as np
import geopandas as gpd
import shapely.wkb as wkb

from datetime import (
    datetime, 
    timedelta,
)
from dagster import get_dagster_logger
from dagster_aws.s3 import S3Resource
from sqlalchemy.engine import Engine
from jinja2 import Template

# Get Dagster Logger
logger = get_dagster_logger()

def generate_fake_meteo_data(station_id="ST001",
                            latitude=0,
                            longitude=0,
                            ) -> pd.DataFrame:
    """
    Generate fake meteorological data for the current day in 10-minute intervals.

    Returns a pandas DataFrame with columns:
    timestamp, station_id, temperature, humidity, pressure
    """
    try:
        # Current date (midnight to next midnight)
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        timestamps = pd.date_range(start=today, end=today + timedelta(days=1), freq="10min", inclusive="left")

        # Fake data
        n = len(timestamps)
        temperature = np.random.normal(loc=20, scale=5, size=n).round(1)  # °C
        humidity = np.random.uniform(low=40, high=90, size=n).round(1)    # %
        pressure = np.random.normal(loc=1013, scale=5, size=n).round(1)   # hPa

        df = pd.DataFrame({
            "timestamp": timestamps,
            "station_id": station_id,
            "latitude": latitude,
            "longitude": longitude,
            "temperature": temperature,
            "humidity": humidity,
            "pressure": pressure
        })

        return df
    except Exception as exc:
        raise
    

def pg_to_minio_geoparquet(sql_query: str,
                        object_prefix: str,
                        bucket_name: str,
                        object_template: str,
                        pg_engine: Engine = None,
                        s3_resource: S3Resource = None,
                        chunk_size: int = 100_000,
                        ) -> None:
    """
    Streams rows from PostGIS using a server-side cursor, converts 
    them to GeoParquet using GeoPandas, uploads each result file to 
    MinIO, and deletes the local file after its use.
    
    Managing exceptions a little bit better here.
    """

    def df_convert_n_upload(input_df: pd.DataFrame,
                        geom_col:str ,
                        input_crs: str,
                        s3_object_key: str, 
                        s3_bucket: str,
                        minio_s3_client) -> bool:
        """
        Converts input DataFrame to GeoDatframe, using the provided CRS.
        DataFrame must have its geometry columns in HexWKB format.
        Uploads a GeoDataFrame to Minio, using a temporary file thats deleted when the upload is finished.
        """
        if len(input_df) > 0:
            if geom_col in df.columns:
                # Convert DataFrame geom, from Hex String
                try:
                    input_df['geom'] = input_df[geom_col].apply(lambda x: wkb.loads(x, hex=True))
                except Exception as exc:
                    logger.warning(f"Error converting HexWKB geometry values from {geom_col}.\n{str(exc)}")
                    pass
                
                # Convert to GeoDataFrame
                result_gdf = None
                try:
                    result_gdf = gpd.GeoDataFrame(input_df, geometry='geom', crs=input_crs)
                    pass
                except Exception as exc:
                    logger.warning(f"Error converting to GeoDataFrame using {input_crs}.\n{str(exc)}")
                    pass
                
                # Upload to Minio
                tmp_file = None
                try:
                    # Create temp file, get path
                    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
                    logger.info(f"Using {tmp_file.name} temp file")
                    
                    # Save Geoparquet
                    result_gdf.to_parquet(tmp_file.name, 
                                index=False, 
                                engine='pyarrow',
                                schema_version='1.1.0',
                                compression='gzip',
                            )
                    tmp_file.seek(0)
                    data_stream = open(tmp_file.name, "rb")

                    # Upload temp file to minio
                    minio_s3_client.upload_file(
                        Filename=tmp_file.name,
                        Bucket=s3_bucket,
                        Key=s3_object_key,
                    )
                    logger.info(f"Uploaded {s3_object_key}")
                    
                    # Cleanup used temp file
                    data_stream.close()
                    pass
                except Exception as exc:
                    logger.warning(f"Error while uploading {tmp_file} to Minio.\n{str(exc)}")
                    pass
                finally:
                    # Cleanup temp file
                    try:
                        os.remove(tmp_file.name)
                    except Exception:
                        pass
            else:
                logger.warning(f"Geometry column {geom_col} not found in DataFrame.")
        else:
            logger.warning(f"No rows found in DataFrame.")
    
    # Init DB connection vars
    conn = None
    cursor = None
     # Init S3 client
    s3_client = s3_resource.get_client()

    try:
        # Create raw psycopg2 connection from SQLAlchemy engine
        conn = pg_engine.raw_connection()
        cursor = conn.cursor(name="stream_cursor")  # server-side cursor
        cursor.itersize = chunk_size
        cursor.execute(sql_query)

        # Current batch number, process while batches exist
        batch = 1
        while True:
            # Fetch rows using db cursor
            rows = cursor.fetchmany(chunk_size)
            
            # Check for remainging rows, break loop if there are not
            if not rows:
                logger.info(f"Finished processing {batch - 1} batch(es).")
                break
            
            # Convert fetched rows to DataFrame
            df = pd.DataFrame(rows, columns=[col.name for col in cursor.description])
            
            # Convert DataFrame to GeoDataFrame, then to Geoparquet and upload to Minio with CRS 32717
            object_template_4326 = Template(object_template).render(
                batch_id=batch,
                crs='4326')
            object_key_4326 = f"{object_prefix}/{object_template_4326}"
            df_convert_n_upload(input_df=df,
                                geom_col='geom_4326',
                                input_crs='EPSG:4326',
                                s3_object_key=object_key_4326,
                                s3_bucket=bucket_name,
                                minio_s3_client=s3_client
                            )
            
            # Increase batch number
            batch += 1
    except Exception as exc:
        logger.error(f"Error while processing data from query:\n{sql_query}\n{str(exc)}")    
    finally:
        # Clean up connections
        try:
            if cursor: cursor.close()
        except Exception:
            pass

        try:
            if conn: conn.close()
        except Exception:
            pass
        
        try:
            pg_engine.dispose()
        except Exception:
            pass