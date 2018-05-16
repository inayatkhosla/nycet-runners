import datetime
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO
from io import BytesIO
import os
pd.options.display.precision = 3
import sys

class GenerateVoterFile():
    '''
    directory = directory where voter file is stored
    years = List of years this voter file will cover. Input in the format of ['15', '16', '17', etc...]
    '''
    def __init__(self, years):
        self.years = years
        self.read_data()
        self.generate_demos()
        self.generate_turnout()
        self.delete_cols()
        self.get_pcts()

    def read_data(self):
        self.vfile = pd.read_table('../DATA/VOTER_FILE/' + os.listdir('../DATA/VOTER_FILE/')[1])
        self.vfile = self.vfile.rename(columns = {'Voter File VANID': 'VANID'})
        self.vfile = self.vfile[self.vfile['RegistrationStatusName'].apply(lambda x: x in ['Registered Active', 'Registered Inactive'])]

    def generate_demos(self):
        self.vfile['countyed'] = self.vfile['CountyName'] + self.vfile['PrecinctName']
        self.vfile['Female'] = self.vfile['Sex']== 'F'
        self.vfile['Male'] = self.vfile['Sex'] == 'M'
        self.vfile['Undefined_Sex'] = self.vfile['Sex'] == 'U'
        self.vfile['Democrat'] = self.vfile['Party'] == 'D'
        self.vfile['Republican'] = self.vfile['Party'] == 'R'
        self.vfile['Independent'] = self.vfile['Party'] == 'I'
        self.vfile['Unaffiliated'] = self.vfile['Party'] == 'U'
        self.vfile['Hispanic'] = self.vfile['Race'] == 'H'
        self.vfile['Asian'] = self.vfile['Race'] == 'A'
        self.vfile['White'] = self.vfile['Race'] == 'W'
        self.vfile['Black'] = self.vfile['Race'] == 'B'
        self.vfile['Other_Party_Affiliation'] = self.vfile['Party'].apply(lambda x: x not in ['U', 'R', 'D', 'I'])
        self.vfile['DOB'] = pd.to_datetime(self.vfile['DOB'])
        self.vfile['age'] = datetime.datetime.now().year - self.vfile['DOB'].dt.year

        for year in self.years:
            self.vfile['youth_' + year] = self.vfile['age'].apply(lambda x: 1 if x <= (2035 + 18 - datetime.datetime.now().year) else 0)
            for election_type in ['General', 'Primary', 'Special']:
                self.vfile[election_type + year] = self.vfile[election_type + year].apply(lambda x: 1 if x in ['D', 'Y', 'R', 'A', 'P'] else 0)

    def generate_turnout(self):
        turnout_demos = {'Dem':['Party', 'D'], 'Rep': ['Party', 'R'], 'White': ['Race', 'W'], 'Black':['Race', 'B'], 'Hispanic':['Race', 'H'], 'Asian':['Race', 'A'], 'Male': ['Sex', 'M'], 'Female':['Sex', 'F'], 'Unaff':['Party', 'U'], 'Youth':['youth',True]}
        for cat in turnout_demos:
            print ('Generating ' + cat)
            for year in years:
                # Generating necesary values
                new_col_name = cat + '_General_' + year + '_Turnout'
                year_filter_col = 'General' + year
                demo_filter_col = turnout_demos[cat][0]
                demo_filter = turnout_demos[cat][1]
                if demo_filter_col == 'youth':
                    demo_filter_col = demo_filter_col + '_' + year

                # Creating New Column
                self.vfile[new_col_name] = self.vfile[[year_filter_col, demo_filter_col]].apply(lambda x: 1 if ((x[year_filter_col] == 1) and (x[demo_filter_col] == demo_filter)) else 0, axis = 1)

        # self.vfile.to_csv('./vfile_temp.csv')

    def delete_cols(self):
        # self.vfile = pd.read_csv('./vfile_temp.csv')
        for year in self.years:
            self.vfile['youth_' + year] = self.vfile['age'].apply(lambda x: 1 if x <= (2035 + 18 - datetime.datetime.now().year) else 0)

    def get_pcts(self):
        self.vfile = pd.read_csv('./vfile_temp.csv')
        test_2 = self.vfile[['countyed', 'VANID']].groupby('countyed').count()['VANID'].reset_index()
        self.test = self.vfile.groupby(['countyed', 'CD', 'SD', 'HD']).sum().reset_index()
        self.test = self.test.merge(test_2, on = 'countyed').rename(columns = {'VANID_y': 'ed_count'})
        turnout_demos = {'Dem':['Democrat'], 'Rep': ['Republican'], 'White': ['White'], 'Black':['Black'], 'Hispanic':['Hispanic'], 'Asian':['Asian'], 'Male': ['Male'], 'Female':['Female'], 'Unaff':['Unaffiliated'], 'Youth':['youth']}

        for year in self.years:
            for cat in turnout_demos:
                old_col_name = cat + '_General_' + year + '_Turnout'
                to_divide = turnout_demos[cat][0]
                new_col_name = cat.lower() + '_turnout_' + year + '_pct'

                if cat == 'Youth':
                    to_divide = 'youth_' + year

                self.test[new_col_name] = self.test[old_col_name]/self.test[to_divide]
            self.test['turnout_' + year] = self.test['General' + year]/self.test['ed_count']
        self.test = self.test.fillna(-1)
        del self.test['VANID_x']

class GenerateDemoMetrics():
    def __init__(self, vfile, directory, years, acs = True):
        '''
        vfile: GenerateVoterFile object
        directory: file directory where the demo files are
        acs: T/F indicating whether to generate census or ACS
        '''
        self.directory = directory
        self.vfile = vfile.test
        self.years = years
        if acs is True:
            self.gen_acs_df()
        else:
            self.gen_census_df()

    def gen_acs_df(self):
        ## Creating File Mappings
        self.file_mapping = {'B11003': 'households', 'B01001': 'sex', 'B15002': 'education', 'C16002': 'language', 'B19001': 'income', 'G001': 'geo', 'B03002': 'ethnicity_by_race', 'B05003': 'citizenship', 'B23025': 'employment'} #'B02001': 'race',
        self.column_mapping = {'race' : {'HD01_VD01': 'race_total', 'HD01_VD02': 'white_only', 'HD01_VD03': 'black_only', 'HD01_VD04':'native_american_only', \
                                    'HD01_VD05': 'asian_only', 'HD01_VD06': 'pacific_only', 'HD01_VD07': 'other_race_only', \
                                    'HD01_VD08': 'two_or_more_races'},
                          'education': {('HD01_VD10', 'HD01_VD09', 'HD01_VD08', 'HD01_VD27', 'HD01_VD26', 'HD01_VD25', 'HD01_VD04', 'HD01_VD03', 'HD01_VD24', 'HD01_VD23', 'HD01_VD22', 'HD01_VD07', 'HD01_VD06', 'HD01_VD05', 'HD01_VD21'): 'less_than_high_school', \
                                       ('HD01_VD11', 'HD01_VD28'): 'high_school_grad', \
                                       ('HD01_VD12', 'HD01_VD13', 'HD01_VD29', 'HD01_VD30'): 'some_college', \
                                       ('HD01_VD17', 'HD01_VD14', 'HD01_VD34', 'HD01_VD31'): 'non_bachelors_college', \
                                       ('HD01_VD15', 'HD01_VD32'): 'bachelors_grad', \
                                       ('HD01_VD16', 'HD01_VD33'): 'masters', \
                                       ('HD01_VD18', 'HD01_VD35'): 'doctorate', \
                                       ('HD01_VD10', 'HD01_VD09', 'HD01_VD08', 'HD01_VD27', 'HD01_VD26', 'HD01_VD25', 'HD01_VD04', 'HD01_VD03', 'HD01_VD24', 'HD01_VD23', 'HD01_VD22', 'HD01_VD07', 'HD01_VD06', 'HD01_VD05', 'HD01_VD21','HD01_VD11', 'HD01_VD28', 'HD01_VD12', 'HD01_VD13', 'HD01_VD29', 'HD01_VD30', 'HD01_VD17', 'HD01_VD14', 'HD01_VD34', 'HD01_VD31', 'HD01_VD15', 'HD01_VD32','HD01_VD16', 'HD01_VD33', 'HD01_VD18', 'HD01_VD35'): 'education_total'},
                          'language': {'HD01_VD02': 'english_only', 'HD01_VD04': 'spanish_only', 'HD01_VD10': 'asia_pacific_only'},
                          'income': {('HD01_VD02', 'HD01_VD03', 'HD01_VD04', 'HD01_VD05', 'HD01_VD06', 'HD01_VD07'): 'lower', \
                                     ('HD01_VD08', 'HD01_VD09', 'HD01_VD10'): 'lower_middle', \
                                     ('HD01_VD11', 'HD01_VD12'): 'middle', \
                                     ('HD01_VD13',): 'upper_middle', \
                                     ('HD01_VD14','HD01_VD15'): 'upper', \
                                     ('HD01_VD16', 'HD01_VD17'): 'upper_high',\
                                    ('HD01_VD02', 'HD01_VD03', 'HD01_VD04', 'HD01_VD05', 'HD01_VD06', 'HD01_VD07', 'HD01_VD08', 'HD01_VD09', 'HD01_VD10', 'HD01_VD11', 'HD01_VD12', 'HD01_VD13', 'HD01_VD14','HD01_VD15', 'HD01_VD16', 'HD01_VD17'): 'income_total'},
                          'employment': {'HD01_VD02': 'labor_force', 'HD01_VD04': 'employed', 'HD01_VD05': 'unemployed'},
                          'households': {'HD01_VD01': 'all_households', 'HD01_VD03': 'married_with_children', 'HD01_VD10':'male_household_children', 'HD01_VD16': 'female_household_children'},
                          'sex': {'HD01_VD26': 'female', 'HD01_VD02': 'male', 'HD01_VD01': 'total', ('HD01_VD03','HD01_VD04','HD01_VD05','HD01_VD06','HD01_VD27','HD01_VD28','HD01_VD29','HD01_VD30'): 'to_subtract'},
                          'ethnicity_by_race': {'HD01_VD01': 'race_total', 'HD01_VD02': 'total_non_hispanic', 'HD01_VD03': 'white_only_non_hispanic', 'HD01_VD04': 'black_only_non_hispanic', 'HD01_VD05': 'native_american_non_hispanic', \
                                               'HD01_VD06': 'asian_non_hispanic', 'HD01_VD07': 'pacific_islander_non_hispanic', 'HD01_VD08': 'other_race_non_hispanic', 'HD01_VD09': 'two_races_non_hispanic', \
                                                'HD01_VD12': 'total_hispanic'},
                          'geo': {'VD054': 'cong_district', 'VD055': 'ss_district', 'VD056': 'ad_district', 'VD057': 'ed_district', 'VD058': 'ed_indicator'},
                          'citizenship': {'HD01_VD02': 'native', 'HD01_VD05': 'naturalized_citizen', 'HD01_VD06': 'not_citizen',\
                                         ('HD01_VD02', 'HD01_VD05', 'HD01_VD06'): 'citizen_total'}
            }

        self.acs_demo_df = pd.DataFrame()
        for path in os.listdir(self.directory):
            try:
                acs_name = path.split('YR_')[1].split('_with')[0]
            except IndexError:
                print (path + ' ignored!')
                continue

            if acs_name in self.file_mapping:
                temp = pd.read_csv(self.directory + path, sep = ",")

                acs_type = self.file_mapping[acs_name]
                acs_columns = self.column_mapping[acs_type]
                temp = temp.sort_values(by = 'countyed')
                self.acs_demo_df['countyed'] = temp['countyed']

                for key in acs_columns.keys():
                    if isinstance(key, basestring):
                        self.acs_demo_df[acs_columns[key]] = temp[key]
                    else:
                        i = 0

                        for thing in key:
                            if i == 0:
                                self.acs_demo_df[acs_columns[key]] = temp[thing]
                            else:
                                self.acs_demo_df[acs_columns[key]] = self.acs_demo_df[acs_columns[key]] + temp[thing]

                            i += 1

        self.acs_demo_df['voting_age_pop'] = self.acs_demo_df['total'] - self.acs_demo_df['to_subtract']

        # Generating Turnout Metrics
        self.acs_demo_df['countyed'] = self.acs_demo_df['countyed'].apply(lambda x: x.replace('BRONX', 'Bronx').replace('NEW YORK', 'New York').replace('QUEENS', 'Queens').replace('KINGS', 'Kings').replace('RICHMOND', 'Richmond').replace('AD 0', 'Ad ').replace('ED', 'Ed'))
        self.vfile['registered_voters'] = self.vfile['Male'] + self.vfile['Female']
        self.acs_demo_df['voting_age_pop'] = self.acs_demo_df['total'] - self.acs_demo_df['to_subtract']

        turnouts = ['countyed', 'registered_voters']
        years = []

        for year in self.years:
            years.append('turnout_' + str(year))

        turnouts = turnouts + years

        self.acs_demo_df = self.vfile[turnouts].merge(self.acs_demo_df, on = 'countyed')
        self.acs_demo_df['registered_pct'] = self.acs_demo_df['registered_voters']/self.acs_demo_df['voting_age_pop']
        self.acs_demo_df['median_turnout'] = self.acs_demo_df[years].apply(lambda x: x.median(axis = 0), axis = 1)

        # Turning total metrics into proportion
        self.acs_demo_df['male_pct'] = self.acs_demo_df['male']/self.acs_demo_df['total']
        self.acs_demo_df['female_pct'] = self.acs_demo_df['female']/self.acs_demo_df['total']
        self.acs_demo_df['other_race_only_pct'] = self.acs_demo_df['other_race_non_hispanic']/self.acs_demo_df['total']
        self.acs_demo_df['pacific_only_pct'] = self.acs_demo_df['pacific_islander_non_hispanic']/self.acs_demo_df['total']
        self.acs_demo_df['asian_only_pct'] = self.acs_demo_df['asian_non_hispanic']/self.acs_demo_df['total']
        self.acs_demo_df['native_american_only_pct'] = self.acs_demo_df['native_american_non_hispanic']/self.acs_demo_df['total']
        self.acs_demo_df['black_only_pct'] = self.acs_demo_df['black_only_non_hispanic']/self.acs_demo_df['total']
        self.acs_demo_df['white_only_pct'] = self.acs_demo_df['white_only_non_hispanic']/self.acs_demo_df['total']
        self.acs_demo_df['hispanic_pct'] = self.acs_demo_df['total_hispanic']/self.acs_demo_df['total']
        self.acs_demo_df['two_or_more_races_pct'] = self.acs_demo_df['two_races_non_hispanic']/self.acs_demo_df['total']
        self.acs_demo_df['male_household_children_pct'] = self.acs_demo_df['male_household_children']/self.acs_demo_df['all_households']
        self.acs_demo_df['married_with_children_pct'] = self.acs_demo_df['married_with_children']/self.acs_demo_df['all_households']
        self.acs_demo_df['female_household_children_pct'] = self.acs_demo_df['female_household_children']/self.acs_demo_df['all_households']
        self.acs_demo_df['some_college_pct'] = self.acs_demo_df['some_college']/self.acs_demo_df['education_total']
        self.acs_demo_df['masters_pct'] = self.acs_demo_df['masters']/self.acs_demo_df['education_total']
        self.acs_demo_df['non_bachelors_college_pct'] = self.acs_demo_df['non_bachelors_college']/self.acs_demo_df['education_total']
        self.acs_demo_df['bachelors_grad_pct'] = self.acs_demo_df['bachelors_grad']/self.acs_demo_df['education_total']
        self.acs_demo_df['less_than_high_school_pct'] = self.acs_demo_df['less_than_high_school']/self.acs_demo_df['education_total']
        self.acs_demo_df['high_school_grad_pct'] = self.acs_demo_df['high_school_grad']/self.acs_demo_df['education_total']
        self.acs_demo_df['doctorate_pct'] = self.acs_demo_df['doctorate']/self.acs_demo_df['education_total']
        self.acs_demo_df['middle_pct'] = self.acs_demo_df['middle']/self.acs_demo_df['income_total']
        self.acs_demo_df['lower_pct'] = self.acs_demo_df['lower']/self.acs_demo_df['income_total']
        self.acs_demo_df['lower_middle_pct'] = self.acs_demo_df['lower_middle']/self.acs_demo_df['income_total']
        self.acs_demo_df['upper_pct'] = self.acs_demo_df['upper']/self.acs_demo_df['income_total']
        self.acs_demo_df['upper_middle_pct'] = self.acs_demo_df['upper_middle']/self.acs_demo_df['income_total']
        self.acs_demo_df['upper_high_pct'] = self.acs_demo_df['upper_high']/self.acs_demo_df['income_total']
        self.acs_demo_df['asia_pacific_only_pct'] = self.acs_demo_df['asia_pacific_only']/self.acs_demo_df['total']
        self.acs_demo_df['spanish_only_pct'] = self.acs_demo_df['spanish_only']/self.acs_demo_df['total']
        self.acs_demo_df['english_only_pct'] = self.acs_demo_df['english_only']/self.acs_demo_df['total']
        self.acs_demo_df['naturalized_citizen_pct'] = self.acs_demo_df['naturalized_citizen']/self.acs_demo_df['citizen_total']
        self.acs_demo_df['native_pct'] = self.acs_demo_df['native']/self.acs_demo_df['citizen_total']
        self.acs_demo_df['not_citizen_pct'] = self.acs_demo_df['not_citizen']/self.acs_demo_df['citizen_total']
        self.acs_demo_df['employed_pct'] = self.acs_demo_df['employed']/self.acs_demo_df['labor_force']
        self.acs_demo_df['unemployed_pct'] = self.acs_demo_df['unemployed']/self.acs_demo_df['labor_force']
        self.acs_demo_df['countyed'] = self.acs_demo_df['countyed'].apply(lambda x: x.replace('BRONX', 'Bronx').replace('NEW YORK', 'New York').replace('QUEENS', 'Queens').replace('KINGS', 'Kings').replace('RICHMOND', 'Richmond'))
        self.acs_demo_df = self.acs_demo_df.fillna(-1)

    def gen_census_df(self):
        # Creating File Mapping
        self.file_mapping = {'G001': 'geo', 'P2': 'urban_rural', 'P5': 'ethnicity_by_race', 'P38': 'family_type', 'P12': 'sex'}
        self.column_mapping = {'family_type': {'D002': 'all_families', 'D003': 'all_families_w_children', 'D009':'male_household_children', 'D016': 'female_household_children'},
                          'ethnicity_by_race': {'D003': 'white_only', 'D004': 'black_only', 'D005': 'native_american', \
                                               'D006': 'asian', 'D007': 'pacific_islander', 'D008': 'other_race', 'D009': 'two_races', \
                                                'D010': 'total_hispanic'},
                          'geo': {'VD054': 'cong_district', 'VD055': 'ss_district', 'VD056': 'ad_district', 'VD057': 'ed_district', 'VD058': 'ed_indicator'},
                          'urban_rural': {'D002': 'urban', 'D003': 'urbanized_area', 'D004': 'urbanized_cluster', 'D005': 'rural'},
                          'sex': {'D002': 'male', 'D026': 'female'}
            }

        # Reading in DataFrames
        self.census_df = pd.DataFrame()
        for path in self.file_mapping:
            temp_df = pd.DataFrame()
            for new_path in os.listdir('../DATA/ED_AFF/ED_census/'):
                if path in new_path:
                    temp_df = temp_df.append(pd.read_csv('../DATA/ED_AFF/ED_census/' + new_path))
            temp_df = temp_df.reset_index()

            acs_type = self.file_mapping[path]
            acs_columns = self.column_mapping[acs_type]
            temp_df = temp_df.sort_values(by = 'countyed')
            temp_df['countyed'] = temp_df['countyed'].apply(lambda x: x.replace('BRONX', 'Bronx').replace('NEW YORK', 'New York').replace('QUEENS', 'Queens').replace('KINGS', 'Kings').replace('RICHMOND', 'Richmond').replace('AD 0', 'Ad ').replace('ED', 'Ed'))

            if len(self.census_df) == 0:
                self.census_df['countyed'] = temp_df['countyed']

            for key in acs_columns.keys():
                if isinstance(key, basestring):
                    self.census_df = self.census_df.merge(temp_df[['countyed', key]], on = 'countyed', how = 'outer').rename(columns = {key: acs_columns[key]})

                else:
                    i = 0
                    for thing in key:
                        if i == 0:
                            self.census_df = self.census_df.merge(temp_df[['countyed', key]], on = 'countyed', how = 'outer').rename(columns = {key: acs_columns[key]})

                        else:
                            self.census_df = self.census_df.merge(temp_df[['countyed', thing]], on = 'countyed', how = 'outer')
                            self.census_df[acs_columns[key]] = self.census_df[acs_columns[key]] + self.census_df[thing]

                        i += 1


        # Generating Turnout Metrics
        self.vfile['registered_voters'] = self.vfile['Male'] + self.vfile['Female']

        turnouts = ['countyed', 'registered_voters']
        years = []

        for year in self.years:
            years.append('turnout_' + str(year))
        turnouts = turnouts + years

        self.census_df = self.vfile[turnouts].merge(self.census_df, on = 'countyed')
        self.census_df['median_turnout'] = self.census_df[years].apply(lambda x: x.median(axis = 0), axis = 1)
        self.census_df['total'] = self.census_df['female'] + self.census_df['male']

        self.census_df['urban_pct'] = self.census_df['urban']/self.census_df['total']
        self.census_df['urbanized_area_pct'] = self.census_df['urbanized_area']/self.census_df['total']
        self.census_df['urbanized_cluster_pct'] = self.census_df['urbanized_cluster']/self.census_df['total']
        self.census_df['rural_pct'] = self.census_df['rural']/self.census_df['total']
        self.census_df['male_household_children_pct'] = self.census_df['male_household_children']/self.census_df['all_families']
        self.census_df['all_families_w_children_pct'] = self.census_df['all_families_w_children']/self.census_df['all_families']
        self.census_df['female_household_children_pct'] = self.census_df['female_household_children']/self.census_df['all_families']
        self.census_df['female_pct'] = self.census_df['female']/self.census_df['total']
        self.census_df['male_pct'] = self.census_df['male']/self.census_df['total']
        self.census_df['other_race_pct'] = self.census_df['other_race']/self.census_df['total']
        self.census_df['two_races_pct'] = self.census_df['two_races']/self.census_df['total']
        self.census_df['total_hispanic_pct'] = self.census_df['total_hispanic']/self.census_df['total']
        self.census_df['white_only_pct'] = self.census_df['white_only']/self.census_df['total']
        self.census_df['black_only_pct'] = self.census_df['black_only']/self.census_df['total']
        self.census_df['native_american_pct'] = self.census_df['native_american']/self.census_df['total']
        self.census_df['asian_pct'] = self.census_df['asian']/self.census_df['total']
        self.census_df['pacific_islander_pct'] = self.census_df['pacific_islander']/self.census_df['total']
        self.census_df = self.census_df.fillna(-1)

class RunDBMappings():
    def __init__(self, years, password):
        self.years = years
        self.password = password
        self.vfile = GenerateVoterFile(self.years)
        self.acs = GenerateDemoMetrics(self.vfile, '../DATA/ED_AFF/ED_acs/', years, acs = True)
        self.census = GenerateDemoMetrics(self.vfile, '../DATA/ED_AFF/ED_census/', years, acs = False)
        self.upload_to_db()

    def to_pg(self, df, table_name, con):
        data = BytesIO() #create file in memory/as string
        df.to_csv(data, header=False, index=False, sep = '|') #write csv to that StringIO
        data.seek(0) #start at beginning of "csv"
        raw = con.raw_connection() #create raw connection
        curs = raw.cursor() #cursor
        curs.execute("DROP TABLE " + table_name) #drop table if exists
        empty_table = pd.io.sql.get_schema(df, table_name, con = con) #create schema from df with table name
        empty_table = empty_table.replace('"', '').replace('Unnamed: 0', 'df_index') #???? who knows
        curs.execute(empty_table)   #create table in db from schema
        curs.copy_from(data, table_name, sep = '|') #copy StringIO
        curs.connection.commit() #close connection

    def upload_to_db(self):
        engine = create_engine('postgresql+psycopg2://apps_user:{}@nycet-postgres.c1swnd7n2f4l.us-east-1.rds.amazonaws.com:5432/apps'.format(self.password))
        engine_conn = engine.connect()
        self.to_pg(self.acs.acs_demo_df, 'acs_ed_demographics', engine)
        self.to_pg(self.census.census_df, 'census_ed_demographics', engine)
        self.to_pg(self.vfile.test, 'ed_agg_voter_file', engine)

password = sys.argv[1]
years = ['12', '13', '14', '15', '16', '17']
RunDBMappings(years, password)

# Instructions to use:

# 1. Open "03_Generate_Metrics.py" in a text editor.
# 2. Change "years" to include any additional years you now use.
