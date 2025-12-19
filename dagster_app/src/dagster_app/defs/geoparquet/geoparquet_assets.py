import dagster as dg


@dg.asset(
    group_name='pg2minio_geoparquet',
)
def geoparquet_hello(context: dg.AssetExecutionContext) -> None:
    """
    Say hello
    """
    
    try:
        context.log.warning("Hello GeoParquet")
    except Exception as exc:
        context.log.error(f"While creating fake meteo data\n{str(exc)}")
