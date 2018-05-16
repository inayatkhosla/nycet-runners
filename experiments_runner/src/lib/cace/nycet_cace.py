from lib.packages import *

'''
Example Usage:
import contact_rate_calculator as cr
import nycet_cace as cace
licet_cr = cr.ContactRateCalculator(voter_df[voter_df.org=='LICET'],['election'],
                                    rel_contact_types = ['PHONE', 'WALK', 'TEXT', 'ROBOCALL'])
licet_cace = cace.OnTheCace(licet_cr.contact_rates,'election',voter_df[voter_df.org=='LICET'])
licet_cace.dem_cace(ci=True, iterations = 1000)

Now, licet_cace.contact_rates will have all the relevant cace metrics in its columns
'''

class OnTheCace:

    def __init__(self, contact_rates, voter_df):

        self.voters = voter_df
        self.voters.vanid = self.voters.vanid.apply(lambda x: str(x))
        self.contact_rates = contact_rates.dropna() # should be a DataFrame


    def dem_cace(self, ci=False, iterations=1000):
        '''
        dem should be a demographic column name
        '''
        def calc_cace2(tv, cv, cr):
            pattern = re.compile(r'[a-zA-Z]')
            treatment_votes = sum(tv.result.replace(pattern,1).astype(int))
            control_votes = sum(cv.result.replace(pattern,1).astype(int))

            n1 = len(tv)
            n2 = len(cv)

            try:
                return (treatment_votes/float(n1) - control_votes/float(n2))/cr
            except ZeroDivisionError:
                return None

        def dem_cace_ci(tv, cv, cr, i=1000):

            if (len(tv) * cr > 200) & (len(cv) > 200):
                pattern = re.compile(r'[a-zA-Z]')
                tv = tv.result.replace(pattern,1).astype(int)
                cv = cv.result.replace(pattern,1).astype(int)

                ts = np.random.choice(tv, size=(i, len(tv)), replace=True)
                cs = np.random.choice(cv, size=(i, len(cv)), replace=True)
                bootstrap_samples = zip(ts,cs)

                def get_cace(samples):
                    treatment = samples[0]
                    control = samples[1]
                    return ((sum(treatment) / float(len(treatment))) - (sum(control)/float(len(control)))) \
                        / float(cr)

                cace_dist = map(get_cace, bootstrap_samples)
                cace_dist = pd.Series(cace_dist)

                return {.025: cace_dist.quantile(.025),
                        .25: cace_dist.quantile(.25),
                        'median': cace_dist.quantile(.50),
                        .75: cace_dist.quantile(.75),
                        .975: cace_dist.quantile(.975),
                        'treat_pop': len(tv),
                        'control_pop': len(cv)}
            else:
                return None

        def slice_cace(row):
            slice = True
            for col in row.index.tolist()[:-1]:
                if col == 'contact_rate':
                    break
                slice = slice & (self.voters[col] == row[col])
            return calc_cace2(self.voters[(self.voters.universe == 'Treatment') & slice],
                              self.voters[(self.voters.universe == 'Control') & slice],
                              row['contact_rate'])

        def slice_cace_ci(row):
            slice = True
            for col in row.index.tolist()[:-1]:
                if col=='contact_rate':
                    break

                slice = slice & (self.voters[col]==row[col])
            return dem_cace_ci(self.voters[(self.voters.universe == 'Treatment') & slice],
                               self.voters[(self.voters.universe == 'Control') & slice],
                               row['contact_rate'], i=iterations)


        self.contact_rates['cace'] = self.contact_rates.apply(slice_cace,axis=1)

        if ci:
            self.contact_rates['quantiles'] = self.contact_rates.apply(slice_cace_ci,axis=1)
