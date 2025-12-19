from pathlib import Path
import dagster as dg
from .defs.geoparquet import geoparquet_assets


@dg.definitions
def defs():
    return dg.Definitions.merge(
        dg.load_from_defs_folder(path_within_project=Path(__file__).parent)   
    )
