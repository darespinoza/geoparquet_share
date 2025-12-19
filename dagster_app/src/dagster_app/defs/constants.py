# Object template name
OBJECT_TEMPLATE = ("part-{{ \"%03d\"|format(batch_id) }}.parquet")

# MinIO bucket name
BUCKET_NAME = 'aggregations-data'
CURSOR_CHUNK_SIZE = 100_000