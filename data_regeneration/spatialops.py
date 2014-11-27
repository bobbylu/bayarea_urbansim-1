from spandex import TableLoader
from spandex.spatialtoolz import tag


loader = TableLoader()
t = loader.tables

tag(t.public.parcels, 'taz', t.staging.taz, 'taz_key')
