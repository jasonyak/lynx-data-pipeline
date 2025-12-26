from dagster import Definitions, load_assets_from_modules

from src.assets.sources import initial_source
from src.assets.transforms import basic_transform
from src.assets.sinks import local_sink

# Load all assets from the modules
all_assets = load_assets_from_modules([initial_source, basic_transform, local_sink])

defs = Definitions(
    assets=all_assets,
)
