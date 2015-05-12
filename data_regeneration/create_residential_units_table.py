import numpy as np
import pandas as pd
from urbansim.utils import misc
from spandex import TableLoader
import pandas.io.sql as sql
from spandex.io import df_to_db

###Database connection
loader = TableLoader()
t = loader.tables

def db_to_df(query):
    """Executes SQL query and returns DataFrame."""
    conn = loader.database._connection
    return sql.read_frame(query, conn)

#Prepare census fields on parcel table
parcels = db_to_df('select * from parcel')
parcels['tract_id'] = parcels.block_id.str.slice(5,11)
parcels['block_id_short'] = parcels.block_id.str.slice(11,15)
parcels.county_id = parcels.county_id.astype('int')
parcels.tract_id[parcels.tract_id == ''] = 0
parcels.tract_id = parcels.tract_id.fillna(0).astype('int')
parcels.block_id_short[parcels.block_id_short == ''] = 0
parcels.block_id_short = parcels.block_id_short.fillna(0).astype('int')

#Load SF1 tenure table (h4) and calculation proportion rental
tenure_inputs = pd.read_csv(loader.get_path('census/block_tenure/block_tenure.csv'))
tenure_inputs['proportion_rental'] = tenure_inputs.h0040004 / tenure_inputs.h0040001
tenure_inputs.proportion_rental[tenure_inputs.proportion_rental  == np.inf] = 0.0

#Merge SF1 block data to parcels
p_rental_merged = pd.merge(parcels, tenure_inputs, left_on = ['county_id', 'tract_id', 'block_id_short'], right_on = ['county', 'tract', 'block'], how = 'left')
p_rental_merged.proportion_rental = p_rental_merged.proportion_rental.fillna(0.0)
p_rental_merged = p_rental_merged.set_index('parcel_id')

#Attach proportion_rental field to buildings
buildings = db_to_df('select * from building')
buildings = buildings.set_index('building_id')
buildings['proportion_rental'] = misc.reindex(p_rental_merged.proportion_rental, buildings.parcel_id)
buildings.proportion_rental[buildings.proportion_rental.isnull()] = 0.0
print buildings.proportion_rental.describe()

# Next:  Grab acs data (b25032), adjust proportions up and down based on the structure size

# Prepare units table and assign tenure (1: own, 2: rent)
units = buildings[buildings.residential_units > 0].residential_units
units =  units[units>0].order(ascending=False)
units = buildings.ix[np.repeat(units.index.values, units.values)]
units = units[['parcel_id', 'sqft_per_unit', 'proportion_rental']].reset_index()
units.index = units.index.values + 1
units.index.name = 'residential_unit_id'
units['randnum'] = np.random.rand(len(units))
units['tenure'] = 1
units.tenure[units.randnum <= units.proportion_rental] = 2

num_rentals = len(units[units.tenure == 2])
num_owners = len(units[units.tenure == 1])
print 'Number of rental units: %s' % num_rentals
print 'Number of owned units: %s' % num_owners
print 'Proportion rental: %s' % (num_rentals*1.0 / (num_rentals + num_owners))

del units['randnum']
del units['proportion_rental']
del units['parcel_id']
units['rent'] = 0.0
units['sale_price'] = 0.0
units['bedrooms'] = 0

# Load 'residential_units' table to database
df_to_db(units, 'residential_units', schema=loader.tables.public)