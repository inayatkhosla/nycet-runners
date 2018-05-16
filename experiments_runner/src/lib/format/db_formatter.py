from lib.packages import *

class DBFormatter():

    def __init__(self, cace_results):
        self.cace_df = pd.concat(cace_results.values()).reset_index(drop=True)

    def format_for_db(self):

        def transition_to_db(x, dems):
            to_return = x.copy()
            dem_num = 1
            for d in dems:
                if type(to_return[d]) == str:
                    to_return['dem'+str(dem_num)] = d
                    dem_num +=1
            if dem_num == 2:
                to_return['dem2'] = None
            if to_return.quantiles is not None:
                to_return['ci_low'] = to_return.quantiles[.025]
                to_return['Q1'] = to_return.quantiles[.25]
                to_return['Q3'] = to_return.quantiles[.75]
                to_return['ci_high'] = to_return.quantiles[.975]
                to_return['median'] = to_return.quantiles['median']
                to_return['control_pop'] = to_return.quantiles['control_pop']
                to_return['treatment_pop'] = to_return.quantiles['treat_pop']
                to_return['total_pop'] = to_return['control_pop'] + to_return['treatment_pop']
            to_return.drop('quantiles', inplace=True)
            to_return['dem1_value'] = to_return[to_return['dem1']]
            if to_return['dem2'] is not None:
                to_return['dem2_value'] = to_return[to_return['dem2']]
            else:
                to_return['dem2_value'] = None
            for d in dems:
                to_return.drop(d,inplace=True)
            return to_return

        self.cace_db = self.cace_df.apply(lambda x: transition_to_db(x, ['org','race','sex']), axis=1)#['org','party','race','regstatus','sex','age_bracket']), axis=1)

    def create_inverse_demo(self):
        cace_2 = self.cace_db.copy()
        cace_2['demtemp'] = cace_2['dem1']
        cace_2['demval'] = cace_2['dem1_value']
        cace_2 = cace_2.drop(columns=['dem1','dem1_value'])
        cace_2 = cace_2.rename(columns={'dem2':'dem1','dem2_value':'dem1_value','demtemp':'dem2','demval':'dem2_value'})
        self.cace_db = self.cace_db.append(cace_2)

    def spring_cleaning(self):
        cace = self.cace_db.copy()
        cace = cace[(cace.contact_rate != 0) & (cace.cace != 0) & (cace.cace.notnull()) & (cace.treatment_pop.notnull())]
        cace = cace.fillna('')
        self.cace_db = cace

    def lookup_cleaning(self):

        lookup = {'sex':{r'^M':'Male',
                        r'^F':'Female',
                        r'^U':'Unspecified'},
                  'race':{r'^A':'Asian',
                         r'^B':'Black',
                         r'^H':'Hispanic',
                         r'^U':'Unspecified',
                         r'^W':'White'},
                  'party':{r'^D':'Democratic',
                          r'^G':'Green',
                          r'^I':'Independent',
                          r'^R':'Republican',
                          r'^L':'Libertarian',
                          r'^N':'Unaffiliated',
                          r'^O':'Other',
                          r'^W':'Working Families',
                          r'^U':'Unknown'},
                  'age_bracket':{r'^60 - 300':'60+'},
                  'regstatus':{r'^APPLICANT':'Applicant',
                          r'^DROPPED':'Dropped',
                          r'^MULTIPLE APPEARANCES':'Multiple Appearances',
                          r'^REGISTERED ACTIVE':'Registered Active',
                          r'^REGISTERED INACTIVE':'Registered Inactive',
                          r'^UNREGISTERED':'Unregistered'}
                 }

        cace = self.cace_db.copy()

        def clean_cols(df,dem,dem_col,dem_val_col,old_val,new_val):
            regex_pat = re.compile(old_val)
            df.loc[df[dem_col] == dem, dem_val_col] = df.loc[df[dem_col] == dem, dem_val_col].str.replace(regex_pat,new_val)
            return df

        for dem,lu in lookup.items():
            for old,new in lu.items():
                df = clean_cols(cace,dem,'dem1','dem1_value',old,new)
                df = clean_cols(cace,dem,'dem2','dem2_value',old,new)

        cace.loc[cace.dem2_value == '','dem2'] = 'all'
        cace.loc[cace.dem1_value == '','dem1'] = 'all'
        self.cace_db = cace

    def convert_from_proportion(self):
        for col in ['Q1','Q3','cace','ci_high','ci_low','median']:
            self.cace_db[col] = self.cace_db[col].apply(lambda x: round(x * 100,1))

    def add_election_and_year_cols(self):
        self.cace_db[['election','year']] = self.cace_db['election_w_year'].apply(lambda x: pd.Series(x.split(' - ')))
        self.cace_db.drop(columns=['election_w_year'], inplace=True)

    def run(self):
        self.format_for_db()
        self.create_inverse_demo()
        self.spring_cleaning()
        self.lookup_cleaning()
        self.convert_from_proportion()
        self.add_election_and_year_cols()
