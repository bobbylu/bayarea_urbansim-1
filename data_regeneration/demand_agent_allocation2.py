import pandas as pd, numpy as np
from spandex import TableLoader
from spandex.io import exec_sql,  df_to_db
import utils

hh_choice_fn = utils.unit_choice
emp_choice_fn = utils.unit_choice

#Connect to the database
loader = TableLoader()

##Get the buildings, the alternatives we will be allocating to.  Buildings must have field name zone_id_field_name (e.g. taz)
buildings = utils.db_to_df('select * from buildings;').set_index('building_id')


################
#####HOUSEHOLDS#
################

# Load TAZ-level synthetic population
relative_path_to_households_csv = 'hh/synth/hhFile.p2011s3a1.2010.csv'
households_index_column_name = 'HHID'
hh_zone_id_field_name = 'taz'

hh = pd.read_csv(loader.get_path(relative_path_to_households_csv)).set_index(households_index_column_name)
hh.index.name = 'household_id'
hh['building_id'] = -1

# Any household table formatting goes here
hh = hh.rename(columns = {'TAZ':'taz'})

# Get the taz-level dwelling unit controls just for reference (to compare number of units with number of households)
taz_controls_csv = loader.get_path('hh/taz2010_imputation.csv')
targetunits = pd.read_csv(taz_controls_csv, index_col='taz1454')

# Occupancy rate diagnostic
targetunits['hh'] = hh.groupby(hh_zone_id_field_name).size()
df = targetunits[['targetunits', 'hh']]
df['occupancy'] = df.hh*1.0/df.targetunits
print 'Number of zones over-occupied with households will be: %s' % (df.occupancy>1).sum()

##Universe of alternatives is residential units
empty_units = buildings[buildings.residential_units > 0].residential_units.order(ascending=False)
alternatives = buildings[['development_type_id', 'parcel_id', hh_zone_id_field_name]]
alternatives = alternatives.ix[np.repeat(empty_units.index.values,empty_units.values.astype('int'))]

taz_hh_counts = hh.groupby(hh_zone_id_field_name).size()

for taz in np.unique(hh[hh_zone_id_field_name]):
    num_hh = taz_hh_counts[taz_hh_counts.index.values==taz].values[0]
    chooser_ids = hh.index[hh[hh_zone_id_field_name]==taz].values
    print 'There are %s households in TAZ %s' % (num_hh, taz)
    alts = alternatives[alternatives[hh_zone_id_field_name]==taz]
    alternative_ids = alts.index.values
    probabilities = np.ones(len(alternative_ids)) #Each resunit has equal probability.  Change if alternative weights desired.
    num_resunits = len(alts)
    print 'There are %s residential units in TAZ %s' % (num_resunits, taz)
    choices = hh_choice_fn(chooser_ids,alternative_ids,probabilities)
    hh.loc[chooser_ids,'building_id'] = choices
    if num_hh > num_resunits:
        print 'Warning:  number of households exceeds number of resunits in TAZ %s' % taz
        
##Allocated household diagnostics
targetunits['hh_allocated'] = pd.merge(hh, buildings, left_on = 'building_id', right_index = True).groupby('taz_x').size()
df = targetunits[['targetunits', 'hh', 'hh_allocated']]
df['occupancy'] = df.hh*1.0/df.targetunits
print df.head()

##Export household allocation summary
summary_output_path = loader.get_path('out/regeneration/summaries/hh_summary.csv')
df.to_csv(summary_output_path)



################
#####JOBS#######
################

# Load aggregate employment totals by sector/zone.
# We already have this from above (taz2010_imputation.csv -> targetunits)

# Translate job totals to job records
sector_columns = []
for col in targetunits.columns:
    if col.startswith('e'):
        if col.endswith('_10'):
            sector_columns.append(col)
emp_targets = targetunits[sector_columns]

total_jobs = int(emp_targets.etot_10.sum())
job_id = np.int32(np.arange(total_jobs) + 1)
taz_id = np.int64(np.zeros(total_jobs))
sector_id = np.int32(np.zeros(total_jobs))

## Prepare jobs table
i = 0
#regional_data = regional_data[regional_data.block11.notnull()].fillna(0)
if 'etot_10' in sector_columns: sector_columns.remove('etot_10')
for taz in emp_targets.index.values:
    for sector in sector_columns:
        num_jobs = int(emp_targets.loc[taz, sector])
        if num_jobs > 0:
            j = i + num_jobs
            taz_id[i:j]=taz
            sector_num = int(sector.split('_')[0].split('e')[1])        
            sector_id[i:j]=sector_num
            i = j
            
jobs_table = {'job_id':job_id,'taz':taz_id,'sector_id':sector_id}
jobs_table = pd.DataFrame(jobs_table)
jobs_table = jobs_table.set_index('job_id')
jobs_table['building_id'] = -1

taz_job_counts = jobs_table.groupby('taz').size()

#buildiing_sqft_per_job assumptions for calculating job spaces in the initial allocation
building_sqft_per_job = {'BR':355,
 'GV':355,
 'HO':1161,
 'HP':355,
 'IH':661,
 'IL':661,
 'IW':661,
 'MF':400,
 'MR':383,
 'OF':355,
 'RT':445,
 'SC':470,
 'SF':400,
 'LD':1000,
 'VP':1000,
 'other':1000}

##Calculate job spaces per building
buildings['sqft_per_job'] = buildings.development_type_id.map(building_sqft_per_job)
buildings['job_spaces'] = (buildings.non_residential_sqft / buildings.sqft_per_job).fillna(0).astype('int')

##Universe of job space alternatives
empty_units = buildings[buildings.job_spaces > 0].job_spaces.order(ascending=False)
alternatives = buildings[['development_type_id','parcel_id','taz']]
alternatives = alternatives.ix[np.repeat(empty_units.index.values,empty_units.values.astype('int'))]

jobs = jobs_table

##Allocate jobs from TAZ to building
for taz in np.unique(jobs.taz):
    num_jobs = taz_job_counts[taz_job_counts.index.values==taz].values[0]
    chooser_ids = jobs.index[jobs.taz==taz].values
    print 'There are %s jobs in TAZ %s' % (num_jobs, taz)
    alts = alternatives[alternatives.taz==taz]
    alternative_ids = alts.index.values
    probabilities = np.ones(len(alternative_ids))
    num_jobspaces = len(alts)
    print 'There are %s job spaces in TAZ %s' % (num_jobspaces, taz)
    choices = emp_choice_fn(chooser_ids,alternative_ids,probabilities)
    jobs.loc[chooser_ids,'building_id'] = choices
    if num_jobs > num_jobspaces:
        print 'Warning:  number of jobs exceeds number of job spaces in TAZ %s' % taz

targetunits['jobs_allocated'] = pd.merge(jobs, buildings, left_on = 'building_id', right_index = True).groupby('taz_x').size()
targetunits['jobs'] = jobs.groupby('taz').size()
targetunits['job_spaces'] = buildings.groupby('taz').job_spaces.sum()

df = targetunits[['job_spaces', 'jobs', 'jobs_allocated']]
df['occupancy'] = df.jobs_allocated*1.0/df.job_spaces
df['diff'] = df.jobs - df.job_spaces
summary_output_path = loader.get_path('out/regeneration/summaries/emp_summary.csv')
df.to_csv(summary_output_path)

print df.head(50)

print jobs.building_id.isnull().sum()

print hh.building_id.isnull().sum()

targetunits['sqft_per_job'] = targetunits.targetnonressqft/targetunits.etot_10

print targetunits['sqft_per_job'].describe()

jobs.building_id[jobs.building_id.isnull()] = -1
hh.building_id[hh.building_id.isnull()] = -1

#EXPORT DEMAND AGENTS TO DB
df_to_db(jobs, 'jobs', schema=loader.tables.public)
df_to_db(hh, 'households', schema=loader.tables.public)

#EXPORT BUILDING TABLE BACK TO DB
buildings['residential_sqft'] = buildings.residential_units * buildings.sqft_per_unit
buildings2 = buildings[['parcel_id', 'development_type_id', 'improvement_value', 'residential_units', 
                        'residential_sqft', 'sqft_per_unit', 'non_residential_sqft', 'nonres_rent_per_sqft', 'res_price_per_sqft', 'stories', 'year_built',
                        'redfin_sale_price', 'redfin_sale_year', 'redfin_home_type', 'costar_property_type', 'costar_rent']]
devtype_devid_xref = {'SF':1, 'MF':2, 'MFS':3, 'MH':4, 'MR':5, 'GQ':6, 'RT':7, 'BR':8, 'HO':9, 'OF':10, 'OR':11, 'HP':12, 'IW':13, 
                      'IL':14, 'IH':15, 'VY':16, 'SC':17, 'SH':18, 'GV':19, 'VP':20, 'PG':21, 'PL':22, 'AP':23, 'LD':24, 'other':-1}
for dev in devtype_devid_xref.keys():
    buildings2.development_type_id[buildings2.development_type_id == dev] = devtype_devid_xref[dev]
buildings2.development_type_id = buildings2.development_type_id.astype('int')
buildings2.residential_units = buildings2.residential_units.astype('int')
buildings2.residential_sqft = buildings2.residential_sqft.astype('int')
buildings2.non_residential_sqft = np.round(buildings2.non_residential_sqft).astype('int')
buildings2.stories = np.ceil(buildings2.stories).astype('int')
buildings2.year_built = np.round(buildings2.year_built).astype('int')
df_to_db(buildings2, 'building', schema=loader.tables.public)