from lib.packages import *

class ContactRateCalculator():
    def __init__(self, exp_by_voter_df, dem_slices, rel_contact_types):
        self.voter_df = self.process_voter_df(exp_by_voter_df)
        self.dem_slices = dem_slices
        self.contact_types = rel_contact_types
        self.contact_rates = self.get_contact_rates()

    def process_voter_df(self, voter_df):
        def get_age(dob):
            try:
                days_alive = datetime.datetime(2017,12,31) - dob
                return round(days_alive.days/365.0)
            except:
                return None

        def get_age_bracket(age):
            age_brackets = [[18, 29], [30, 44], [45, 59], [60, 300]]

            def stringify(bracket):
                bracket = [str(b) for b in bracket]
                return ' - '.join(bracket)

            bracket_opts = [stringify(bracket) for bracket in age_brackets if age in range(bracket[0], bracket[1]+1)]
            return bracket_opts[0] if len(bracket_opts) > 0 else None

        voter_df['dob'] = pd.to_datetime(voter_df['dob'], errors='coerce')
        future = voter_df['dob'] > datetime.datetime(2010,1,1)
        voter_df.loc[future, 'dob'] -= pd.Timedelta(days=365.25*100)
        voter_df['age'] = voter_df['dob'].apply(lambda x: get_age(x))
        voter_df['age_bracket'] = voter_df['age'].apply(get_age_bracket)

        cols_to_strip = ['sex', 'race', 'ethnicity', 'regstatus', 'county', 'party']
        for col in cols_to_strip:
            voter_df.loc[pd.isnull(voter_df[col]), col] = ''
        voter_df[cols_to_strip] = voter_df[cols_to_strip].applymap(lambda x: x.strip())

        return voter_df

    def get_contact_rates(self):
        def calculate_contact_rate(grp, contact_types):
            grp = grp[grp['universe']=='Treatment']

            if len(grp) > 0:
                cols = [ct + '_org' for ct in contact_types if ct + '_org' in grp.columns]
                canvassed = grp[cols].sum(axis=1)
                no_canvassed = grp[[bool(val) for val in canvassed]]['vanid'].nunique()
                no_contacted = grp['vanid'].nunique()

                contact_rate = round(no_canvassed / float(no_contacted), 2)
            else:  contact_rate = 0

            return contact_rate

        contact_rates_df = self.voter_df.groupby(self.dem_slices) \
               .apply(lambda grp: calculate_contact_rate(grp, self.contact_types)).reset_index()

        contact_rates_df.rename(index=str, columns={0: 'contact_rate'})

        return contact_rates_df
