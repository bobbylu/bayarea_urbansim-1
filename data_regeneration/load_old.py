import logging

from spandex import TableLoader
from spandex.io import logger
from spandex.spatialtoolz import conform_srids


logger.setLevel(logging.INFO)


shapefiles = {'staging.old_cnc': 'contra_costa.shp'}


# Load shapefiles specified above to the project database.
loader = TableLoader()
loader.load_shp_map(shapefiles)

# Fix invalid geometries and reproject.
staging = loader.tables.staging
conform_srids(loader.srid, schema=staging, fix=True)
