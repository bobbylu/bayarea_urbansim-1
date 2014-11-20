import numpy as np
import pandas as pd
import urbansim.sim.simulation as sim

from spandex import TableLoader, TableFrame
from spandex.io import df_to_db


loader = TableLoader()
staging = loader.tables.staging


## Assumptions.


# Use codes were classified manually because the assessor classifications
# are meant for property tax purposes. These classifications should be
# reviewed and revised.
res_codes = {}
exempt_codes = []


# Assume that each residential unit in a mixed-used parcel occupies
# 1500 sqft, since residential vs. non-residential sqft is not known.
sqft_per_res_unit = 1500.


## Register input tables.


tf = TableFrame(staging.FIXME, index_col=FIXME)
sim.add_table('parcels_in', tf, copy=False)


## Register output table.


@sim.table(cache=True)
def parcels_out(parcels_in):
    index = pd.Series(parcels_in.index).dropna().unique()
    df = pd.DataFrame(index=index)
    df.index.name = 'apn'
    return df


## Register output columns.


out = sim.column('parcels_out', cache=True)


@out
def county_id():



@out
def apn():



@out
def parcel_id_local():



@out
def land_use_type_id():



@out
def res_type(land_use_type_id='parcels_out.land_use_type_id'):
    lu = pd.Series(index=land_use_type_id.index)
    for name, codes in res_codes.items():
        lu[land_use_type_id.isin(codes)] = name
    return lu


@out
def land_value():



@out
def improvement_value():



@out
def year_assessed():



@out
def year_built():



@out
def building_sqft():



@out
def non_residential_sqft():



@out
def residential_units():



@out
def sqft_per_unit():



@out
def stories():



@out
def tax_exempt():



## Export back to database.


@sim.model()
def export(parcels_out):
    df = parcels_out.to_frame()
    assert df.index.is_unique
    assert not df.index.hasnans()
    df_to_db(df, 'attributes_FIXME', schema=staging)

sim.run(['export'])
