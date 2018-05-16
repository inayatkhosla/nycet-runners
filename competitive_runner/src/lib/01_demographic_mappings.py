
# coding: utf-8

# # Based on Angela's work


from packages import *
import db_puller as dbp
import sys

#get_ipython().run_line_magic('matplotlib', 'inline')


import os


FIPS_PATH = '../DATA/Fips_Files/'
CENSUS_MAPS_PATH = '../DATA/census_maps.csv'


# # Data

# ## CountyED Maps
password = sys.argv[1]
dbpuller = dbp.DBPuller('ny', password)
dbpuller.connect()

ced_maps = dbpuller.pull('censusblock')
tostr_cols = ['county', 'countyed','countyfp10','tractce10','blockce10','geoid10']
ced_maps[tostr_cols] = ced_maps[tostr_cols].astype(str)

#print(ced_maps['countyed'].nunique())
#print(ced_maps['county'].unique())
#ced_maps.head()


# ## FIPS Maps


fips_list = [FIPS_PATH+elem for elem in os.listdir(FIPS_PATH)]
fips_all = []
for elem in fips_list:
    df = pd.read_csv(elem, header=None, dtype=str)
    df.columns = ['state','fips1','fips2','county','H']
    fips_all.append(df)
fips_ny = pd.concat(fips_all)



fips_ny['county'] = fips_ny.county.str.upper().str.extract('(.*) COUNTY')
fips_ny['fips2'] = fips_ny['fips2'].str.pad(width=3, side='left', fillchar='0')
fips_ny['fips'] = fips_ny['fips1'] + fips_ny['fips2']
fips_ny.head()


# # Method
# - geoids are a concatenation of the federal FIPs codes for state/county + tract + block
# - pulled in the FIPs codes
# - padded the fips, tract and block columns with 00 so that all counties have 3 digits, all tracts have 6 digits, and all blocks have 4 (left padding)
# - made two new columns, one for complete, correct geoid (called geoid2), and one at the tract level (this matches the Census data) (called geoid_tract)
# - have not tested with merge, yet

# ## CountyED Maps

maps = ced_maps.merge(fips_ny, how='left', on='county')
maps['tract2'] = maps['tractce10'].str.pad(width=6, side='left', fillchar='0')
maps['block2'] = maps['blockce10'].str.pad(width=4, side='left', fillchar='0')
maps['blockgroup'] = maps['block2'].apply(lambda x: x[0])
maps['geoid_tract'] = maps['fips'] + maps['tract2']
maps['geoid_blockgroup'] = maps['fips'] + maps['tract2'] + maps['blockgroup']
maps['geoid2'] = maps['fips'] + maps['tract2'] + maps['block2']


#missing_census_data.to_csv('missing_census_data.csv',index=False)
maps.to_csv(CENSUS_MAPS_PATH,index=False)
