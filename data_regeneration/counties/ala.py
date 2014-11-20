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
res_codes = {'single': ([1100] + range(1120, 1151) + range(1200, 1500) +
                        range(1900, 2000)),
             'multi': (range(600, 1100) + [1700] + range(2000, 3000) +
                       range(5000, 5300) + range(7000, 7701) + [7800]),
             'mixed': (range(3900, 4000) + [4101] + [4191] + [4240] +
                       [9401] + [9491])}
exempt_codes = range(1, 1000)

# Assume that each residential unit in a mixed-used parcel occupies
# 1500 sqft, since residential vs. non-residential sqft is not known.
sqft_per_res_unit = 1500.


## Register input tables.


tf = TableFrame(staging.parcels_ala, index_col='apn_sort')
sim.add_table('parcels_in', tf, copy=False)


@sim.table_source()
def ie670():
    filepath = \
        loader.get_path('built/parcel/2010/ala/assessor_nov10/IE670c.txt')
    df = pd.read_table(filepath, sep='\t', index_col=False)
    df.set_index("Assessor's Parcel Number (APN) sort format", inplace=True)
    assert df.index.is_unique
    assert not df.index.hasnans()
    return df


@sim.table_source()
def ie673():
    filepath = \
        loader.get_path('built/parcel/2010/ala/assessor_nov10/IE673c.txt')
    df = pd.read_table(filepath, sep='\t', index_col=False)
    df.set_index('APNsort', inplace=True)
    assert df.index.is_unique
    assert not df.index.hasnans()
    return df


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
    return '001'


@out
def parcel_id_local():
    pass


@out
def land_use_type_id(parcels_out, code='ie673.UseCode'):
    # Alternate inputs:
    # - "Use Code": from IE670, values are identical
    return code.reindex(parcels_out.index, copy=False).fillna(0).astype(int)


@out
def res_type(land_use_type_id='parcels_out.land_use_type_id'):
    lu = pd.Series(index=land_use_type_id.index, dtype=object)
    for name, codes in res_codes.items():
        lu[land_use_type_id.isin(codes)] = name
    return lu


@out
def land_value(value='ie670.Land value'):
    # Alternate inputs:
    #  - "CLCA land value": less data available
    return value


@out
def improvement_value(value='ie670.Improvements value'):
    # Alternate inputs:
    #  - "CLCA improvements value": less data available
    return value


@out
def year_assessed(date='ie670.Last document date (CCYYMMDD)'):
    # Alternate inputs:
    # - "Last document prefix": not always numeric
    # - "Last document input date (CCYYMMDD)"
    # - "Property characteristic change date (CCYYMMDD)": from IE673
    return date.floordiv(10000)


@out
def year_built(year='ie673.YearBuilt'):
    return year.str.strip().replace('', np.nan).astype(float)


@out
def building_sqft(sqft='ie673.BldgArea'):
    return sqft


@out
def non_residential_sqft(building_sqft='parcels_out.building_sqft',
                         res_type='parcels_out.res_type',
                         residential_units='parcels_out.residential_units'):
    sqft = pd.Series(index=res_type.index)
    building_sqft = building_sqft.reindex(sqft.index, copy=False)

    # If not residential, assume all area is non-residential.
    sqft[res_type.isnull()] = building_sqft

    # If residential, assume zero non-residential area.
    sqft[(res_type == 'single') | (res_type == 'multi')] = 0

    # If mixed-use, assume residential units occupy some area.
    sqft[res_type == 'mixed'] = (building_sqft -
                                 sqft_per_res_unit * residential_units)

    # Non-residential area must not be negative.
    sqft[(sqft.notnull()) & (sqft < 0)] = 0

    return sqft


@out
def residential_units(tot_units='ie673.Units',
                      res_type='parcels_out.res_type'):
    units = pd.Series(index=res_type.index)
    tot_units = tot_units.reindex(units.index, copy=False)

    # If not residential, assume zero residential units.
    units[res_type.isnull()] = 0

    # If single family residential, assume one residential unit.
    units[res_type == 'single'] = 1

    # If non-single residential, assume all units are all residential,
    # even if mixed-use.
    units[res_type == 'multi'] = tot_units
    units[res_type == 'mixed'] = tot_units

    return units


@out
def sqft_per_unit(building_sqft='parcels_out.building_sqft',
                  non_residential_sqft='parcels_out.non_residential_sqft',
                  residential_units='parcels_out.residential_units'):
    return (1. * building_sqft - non_residential_sqft) / residential_units


@out
def stories(stories='ie673.Stories'):
    # 1 story = 10 Alameda County stories.
    return 0.1 * stories


@out
def tax_exempt(land_use_type_id='parcels_out.land_use_type_id'):
    exempt = pd.Series(index=land_use_type_id.index, dtype=int)
    exempt[land_use_type_id.isin(exempt_codes)] = 1
    exempt[~land_use_type_id.isin(exempt_codes)] = 0
    return exempt


## Export back to database.


@sim.model()
def export(parcels_out):
    df = parcels_out.to_frame()
    assert df.index.is_unique
    assert not df.index.hasnans()
    df_to_db(df, 'attributes_ala', schema=staging)

sim.run(['export'])
