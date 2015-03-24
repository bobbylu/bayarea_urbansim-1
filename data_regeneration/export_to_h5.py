import pandas as pd
from spandex import TableLoader
import pandas.io.sql as sql

loader = TableLoader()

def db_to_df(query):
    """Executes SQL query and returns DataFrame."""
    conn = loader.database._connection
    return sql.read_frame(query, conn)

## Export to HDF5
h5_path = loader.get_path('out/regeneration/summaries/bayarea.h5')  ## Path to the output file
buildings = db_to_df('select * from building').set_index('building_id')
parcels = db_to_df('select * from parcel').set_index('parcel_id')
jobs = db_to_df('select * from jobs').set_index('job_id')
hh = db_to_df('select * from households').set_index('household_id')

store = pd.HDFStore(h5_path)
store['parcels'] = parcels
store['buildings'] = buildings
store['households'] = hh
store['jobs'] = jobs
store.close()