# GeoParquet Share

A small but powerful geospatial ETL solution that uses Dagster to create pipelines in a dynamic way, by declarating pipelines using a **YAML** file in a human-readable form.

I've created an example using a PostGIS database as source, and MinIO object storage as destination. However the data source, destination and configuration (pipelines) can be changed.

![GeoParquet Share](geoparquet_share_draw.png)

## To run this project:

1. Create a `.env` file in the project root directory, like so:

```sh
# Sample DB
POSTGRES_HOST='postgis-source'
POSTGRES_PORT=5432
POSTGRES_DATABASE='sample_db'
POSTGRES_USER='postgres'
POSTGRES_PASSWORD='db_strongpassword'

# MinIO
MINIO_API_URL='http://minio-target:9000'
MINIO_ROOT_USER='minio_admin'
MINIO_ROOT_PASSWORD='minio_strongpassword'

# Get MinIO access and secret keys
MINIO_ACCESS_KEY='a_minio_access_key'
MINIO_SECRET_KEY='a_minio_secret_key'
```

2. Execute the `docker compose up -d` command to create the three containers.
   - A Postgis example **source**
   - A Minio **target** container
   - Dagster **dynamic pipeline creation** project

3. Navigate to Minio Web Console at `http://localhost:9001` and create a new pair keys. Then place them on the `.env` file.

4. Navigate to Dagster's Web Interface at `http://localhost:3000` and execute:
   - Fake meteo data Job
   - Postgis to Minio Job

## Real world application

I've created a function that produces fake data for three imaginary stations located in the city of Cuenca-Ecuador, and then created three pipelines that get that data in a aggregated form. Query result is tranformed to a GeoParquet format and uploaded to MinIO partionating the result in man GeoParquet files if needed, by setting a `CURSOR_CHUNK_SIZE` for each file.

### Important notes

- Pipelines are declared in the `dagster_app/defs/geoparquet/pipelines.yaml` file.
- Once a pipeline is added to the YAML file, the Dagster Code location must be reloaded to create the correspondant Dagster asset.
- The GeoParquet transformation expects `geometry` data type expressed in `EPSG:4326`.
- GeoParquet transformation is made using a temp file.
- A chunk size for the GeoParquet result files can be set in the `constants.py` file, so you can decide partitioning.
