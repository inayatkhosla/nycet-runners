
# coding: utf-8

import pandas as pd
import os
import re
import numpy as np


#Geographical Division = Length of GEO ID

COUNTY = 5
COUNTY_SUB = 10
TRACT = 11
BLKGRP = 12
BLK = 15


# ## Define Input Paths

## Census Maps
ed_tract_df = pd.read_csv('data/census_maps.csv')


ACS_FILES_PATH = 'data/AFF/acs/'
CENSUS_FILES_PATH = 'data/AFF/census/'
CITIZENSHIP_FILES_PATH = 'data/AFF/citizenship/'


# ## Define Standard Functions

def clean_int(text):
    '''
    Supporting Function used to convert GeoID to numberic values.
    '''
    li = re.findall('[0-9]+', str(text))
    num = 0
    if len(li) > 0:
        num = np.ceil(int(li[0]))
    return num


def create_tract_factor(census_maps, geo_id='geoid_blockgroup'):
    return census_maps.groupby([geo_id]).countyed.nunique().sort_values(ascending=False).to_frame().rename(columns={'countyed':'factor'})


def census_cleanup(census, tract_factor, geo_id = 'GEO.id2', cols_to_drop=['GEO.id','GEO.display-label']):
    '''
    census: input dataframe from the ACS/Census Files. Important Criterion is the need for a GEO id in the file.

    '''
    census[geo_id] = census[geo_id].apply(lambda id: int(id))
    temp = census.drop(cols_to_drop,axis=1).set_index(geo_id).applymap(lambda x: clean_int(x))
    #.value_counts(dropna=False)
    for ind in temp.index.tolist():
        try:
            temp.loc[ind] = (temp.loc[ind]/tract_factor.loc[ind][0])
        except KeyError:
            pass
    temp = temp.applymap(lambda x: int(np.ceil(x)) )
    return temp


def combine_data_fields(input_df, census_maps, geo_id = 'GEO.id2', cols_to_drop=['GEO.id','GEO.display-label']):

    choice_of_geo_id = 'geoid2'
    l = input_df[geo_id].apply(lambda x: len(str(x))).median()
    if l == TRACT:
        choice_of_geo_id = 'geoid_tract'
    elif l == BLKGRP:
        choice_of_geo_id = 'geoid_blockgroup'


    tract_df = create_tract_factor(census_maps, geo_id=choice_of_geo_id)
    ed_tract_df = census_maps.set_index(choice_of_geo_id)

    mod_census = census_cleanup(census, tract_factor=ed_tract_df.groupby([choice_of_geo_id]).countyed.nunique().sort_values(ascending=False).to_frame().rename(columns={'countyed':'factor'})
    )
    t = pd.merge(
        left = mod_census,
        right = ed_tract_df[['countyed']],
        left_index=True, right_index=True
    ).drop_duplicates()

    ed_census_df = t.groupby('countyed').sum()

    return ed_census_df


# ## Execution Section

INPUT_FILE_PATH_LIST = [ACS_FILES_PATH, CENSUS_FILES_PATH, CITIZENSHIP_FILES_PATH]
for file_path in INPUT_FILE_PATH_LIST:
    OUTPUT_FILE_PATH_LIST = 'data/ED_AFF/ED_'+'/'.join(file_path.split('/')[-2:])
    #print (OUTPUT_FILE_PATH_LIST)
    if not os.path.exists(OUTPUT_FILE_PATH_LIST):
        os.mkdir(OUTPUT_FILE_PATH_LIST)


    '''
    The section generates a list of relevant input files.

    '''

    in_files = []

    for fil in os.listdir(file_path):
        if 'with_ann' in fil:
            in_files.append(file_path+fil)

        if os.path.isdir(file_path+fil):
            for fil_in_fil in os.listdir(file_path+fil):
                if 'with_ann' in fil_in_fil:
                    in_files.append(file_path+fil+'/'+fil_in_fil)



    '''
    The section below generates the relevant output files.
    '''

    for fil in in_files:
        if 'census' in fil.lower():
            out_fil = OUTPUT_FILE_PATH_LIST+'ED_'+'_'.join(fil.split('/')[-2:])
        else:
            out_fil = OUTPUT_FILE_PATH_LIST+'ED_'+'_'.join(fil.split('/')[-1:])
        census = pd.read_csv(fil).iloc[1:]
        #print (fil)
        ed_census_df = combine_data_fields(census, ed_tract_df)
        #print (out_fil+'\n')
        ed_census_df.to_csv(out_fil)
