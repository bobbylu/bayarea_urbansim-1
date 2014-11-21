import numpy as np
import pandas as pd
from spandex import TableLoader, TableFrame
from spandex.io import df_to_db
import urbansim.sim.simulation as sim

import utils


loader = TableLoader()
staging = loader.tables.staging


## Assumptions.


# Use codes were classified manually because the assessor classifications
# are meant for property tax purposes. These classifications should be
# reviewed and revised.
res_codes = {'single': ['RSFR'],
             'multi': ['RAPT', 'RCON', 'RDUP', 'RMFD', 'RMOB', 'RMSC',
                       'RCOO', 'RQUA', 'RTIM', 'RTRI', 'VRES'],
             'mixed': []}
exempt_codes = []


## Register input tables.


tf = TableFrame(staging.parcels_scl, index_col='parcel')
sim.add_table('parcels_in', tf, copy=False)


@sim.table_source()
def scvta():
    # Will need to group by the site address fields later to aggregate
    # condos with multiple parcels but only a single polygon. Currently,
    # only one condo will join to the geometries in the shapefile.
    df = loader.get_attributes('built/parcel/2010/scl/Scvta031210.dbf')

    # Strip non-numeric characters in parcel numbers. Affects three records.
    df['PARCEL_NUM'] = df.PARCEL_NUM.str.replace('[^0-9]', '')

    # There are only 11 duplicated parcel numbers. From manual inspection,
    # it appears that the "ASSESSED_V" field is zero for one of the entries
    # in each pair of duplicates. Keep the entry with ASSESSED_V > 0.
    df = df[~df.PARCEL_NUM.isin(df.PARCEL_NUM[df.PARCEL_NUM.duplicated()]) |
            (df.ASSESSED_V > 0)]

    df.set_index('PARCEL_NUM', inplace=True)
    assert df.index.is_unique
    assert not df.index.hasnans()
    return df


## Register output table.


@sim.table(cache=True)
def parcels_out(scvta):
    index = pd.Series(scvta.index).dropna().unique()
    df = pd.DataFrame(index=index)
    df.index.name = 'apn'
    return df


## Register output columns.


out = sim.column('parcels_out', cache=True)


@out
def county_id():
    return '085'


@out
def parcel_id_local():
    pass


@out
def land_use_type_id(code='scvta.STD_USE_CO'):
    code[code == ''] = None
    return code


@out
def res_type(land_use_type_id='parcels_out.land_use_type_id'):
    return utils.get_res_type(land_use_type_id, res_codes)


@out
def land_value(value='scvta.ASSESSED_V', percent_im='scvta.PERCENT_IM'):
    # Alternate inputs:
    # - "SALE_AMOUN"
    return 0.0001 * (10000 - percent_im) * value


@out
def improvement_value(value='scvta.ASSESSED_V',
                      percent_im='scvta.PERCENT_IM'):
    # Alternate inputs:
    # - "SALE_AMOUN"
    return 0.0001 * percent_im * value


@out
def year_assessed(date='scvta.SALE_DATE'):
    # Alternate inputs:
    # - "YEAR_SOLD_": less data available and inconsistent with "SALE_DATE"
    #
    # A better approach may be needed. For example, could use the max of
    # "SALE_DATE" and "YEAR_SOLD_" when both are available.
    date.replace(0, np.nan, inplace=True)
    return date.floordiv(10000)


@out
def year_built(year='scvta.YEAR_BUILT'):
    # Alternate inputs:
    # - "EFF_YEAR_B"
    year.replace(0, np.nan, inplace=True)
    return year


@out
def building_sqft(sqft='scvta.SQ_FT'):
    return sqft


@out
def non_residential_sqft(building_sqft='parcels_out.building_sqft',
                         res_type='parcels_out.res_type',
                         residential_units='parcels_out.residential_units'):
    return utils.get_nonresidential_sqft(building_sqft, res_type,
                                         residential_units)


@out
def residential_units(tot_units='scvta.NUMBER_OF_',
                      res_type='parcels_out.res_type'):
    # Alternate inputs:
    # - NUMBER_O_1
    # - NUMBER_O_2
    # - NUMBER_O_3
    # We assume "NUMBER_OF_" is number of units, but are not certain.
    # Some values are unreasonably high (e.g., 9100).
    return utils.get_residential_units(tot_units, res_type)


@out
def sqft_per_unit(building_sqft='parcels_out.building_sqft',
                  non_residential_sqft='parcels_out.non_residential_sqft',
                  residential_units='parcels_out.residential_units'):
    return utils.get_sqft_per_unit(building_sqft, non_residential_sqft,
                                   residential_units)


@out
def stories(stories='scvta.NUMBER_OF1'):
    # Field name confirmed by inspecting the Sobrato Office Tower
    # (APN 26428171), at 488 Almaden Blvd, San Jose, which is a
    # single parcel with a 17-story building.
    return stories


@out
def tax_exempt(land_use_type_id='parcels_out.land_use_type_id'):
    #return utils.get_tax_exempt(land_use_type_id, exempt_codes)
    # Field not present, but could infer from land_use_type_id.
    pass


## Export back to database.


@sim.model()
def export(parcels_out):
    df = parcels_out.to_frame()
    assert df.index.is_unique
    assert not df.index.hasnans()
    df_to_db(df, 'attributes_scl', schema=staging)

sim.run(['export'])
