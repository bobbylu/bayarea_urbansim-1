import os

import pandas as pd
from spandex import TableLoader, TableFrame

# Build parcels TableFrame.
loader = TableLoader()
table = loader.database.tables.public.parcels
tf = TableFrame(table, index_col='id')

# Load TAZ residential unit control totals.
taz_controls_csv = loader.get_path('hh/taz2010_imputation.csv')
targetunits = pd.read_csv(taz_controls_csv, index_col='taz1454')['targetunits']

# Get CSV output file directory.
output_dir = loader.get_path('out/regeneration/summaries')

# Generate summary CSV by county and TAZ.
for grouper in ['county_id', 'taz']:
    df = tf[[grouper, 'non_residential_sqft', 'residential_units']]
    df.dropna(subset=[grouper], inplace=True)

    if grouper == 'taz':
        df[grouper] = df[grouper].astype(int)

    df['count'] = 1
    summary = df.groupby(grouper).sum()

    if grouper == 'taz':
        summary['residential_units_target'] = targetunits

    output_filename = os.path.join(output_dir,
                                   'summary_{}.csv'.format(grouper))
    summary.to_csv(output_filename)
