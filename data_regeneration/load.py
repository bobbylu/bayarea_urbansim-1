import logging

from spandex import TableLoader
from spandex.io import logger
from spandex.spatialtoolz import conform_srids


logger.setLevel(logging.INFO)


shapefiles = {
    #'staging.controls_blocks':
    #'hh/control_sm/block10_gba.shp',

    #'staging.controls_blockgroups':
    #'hh/control_sm/blockgroup10_gba.shp',

    #'staging.nat_farms':
    #'nat/farm/williamson_act.shp',

    #'staging.nat_slopes_gt6':
    #'nat/slope/gt6pctslope_1km.shp',

    #'staging.nat_slopes_gt12':
    #'nat/slope/gt12pctslope_1km',

    #'staging.nat_water':
    #'nat/water/bayarea_allwater.shp',

    #'staging.nat_water_wetlands':
    #'nat/wetlands/wetlands.shp',

    'staging.parcels_ala':
    'built/parcel/2010/ala/parcelsAlaCo2010/asr_parcel.shp',

    'staging.parcels_cnc_poly':
    'built/parcel/2010/cnc/raw10/CAD_AO_ParcelPoly_0410.shp',

    'staging.parcels_cnc_pt':
    'built/parcel/2010/cnc/raw10/CAD_AO_ParcelPoints_int0410.shp',

    'staging.parcels_nap':
    'built/parcel/2010/nap/Napa_Parcels.shp',

    'staging.parcels_nap_tract':
    'built/parcel/2010/nap/Napa_Census_tract.shp',

    # Missing shx file.
    #'staging.parcels_mar':
    #'built/parcel/2005/parcels2005_mar.shp',

    'staging.parcels_scl':
    'built/parcel/2010/scl/parcels2010_scl.shp',

    'staging.parcels_sfr':
    'built/parcel/2010/sfr/parcels2010_sfr.shp',

    'staging.parcels_smt':
    'built/parcel/2010/smt/shapefiles/ACTIVE_PARCELS_APN.shp',

    'staging.parcels_sol':
    'built/parcel/2010/sol/Parcels.shp',

    'staging.parcels_sol_zoning':
    'built/parcel/2010/sol/zoning.shp',

    'staging.parcels_son':
    'built/parcel/2010/son/PAR_PARCELS.shp',

    # Geometry type is MultiPolygonZM.
    #'staging.parcels_son_exlu':
    #'built/parcel/2010/son/parcels2010_son/Final2010exlu.shp',

    #'staging.taz':
    #'juris/reg/zones/taz1454.shp',
}


# Load shapefiles specified above to the project database.
loader = TableLoader()
loader.load_shp_map(shapefiles)

# Fix invalid geometries and reproject.
staging = loader.tables.staging
conform_srids(loader.srid, schema=staging, fix=True)