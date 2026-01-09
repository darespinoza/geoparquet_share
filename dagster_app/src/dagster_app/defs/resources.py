import os
import dagster as dg

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from dagster_aws.s3 import S3Resource


# Customized ConfigurableResource for Postgres resource
class PostgresResource(dg.ConfigurableResource):
    hostname: str
    port: int = 5432
    database: str
    username: str
    password: str
    
    def get_engine(self) -> Engine:
        connection_uri = f"postgresql://{self.username}:{self.password}@{self.hostname}:{self.port}/{self.database}"
        return create_engine(connection_uri)

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "postgres_res": PostgresResource(
                hostname=os.getenv("POSTGRES_HOST"),
                port=int(os.getenv("POSTGRES_PORT")),
                database=os.getenv("POSTGRES_DATABASE"),
                username=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD")
            ),
            "minio_res" :S3Resource(
                endpoint_url=os.getenv("MINIO_API_URL"),
                aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
                aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
                region_name="us-east-1",
                use_ssl=False,
            )
        }
    )
