from .nycet_cace import OnTheCace
from .contact_rate_calculator import ContactRateCalculator

class Cace():

    def __init__(self, exp_by_voter_df, dem_slices, rel_contact_types):
        self.exp_by_voter_df = exp_by_voter_df.copy()
        self.dem_slices = dem_slices
        self.rel_contact_types = rel_contact_types
        self.contact_rates = ContactRateCalculator(self.exp_by_voter_df,self.dem_slices, \
                                self.rel_contact_types).contact_rates.rename(columns={0:'contact_rate'})
        self.cace_calculator = OnTheCace(self.contact_rates,self.exp_by_voter_df)

    def calc_cace(self,ci=True,iterations=1000):
        self.cace_calculator.dem_cace(ci,iterations)
        return self.cace_calculator.contact_rates

class CaceRunner():

    def __init__(self, voter_df, contact_types):
        self.voter_df = voter_df
        self.c_types = contact_types.method
        self.dem_list = {'party','sex','race','regstatus','age_bracket','org'}
        self.results = {}

    def calc_all_orgs(self):
        print('Calculating All Orgs...')
        c = Cace(self.voter_df, ['election_w_year'], self.c_types).calc_cace()
        c['org'] = 'All Orgs'
        self.results[('all_org',None)] = c[['election_w_year','org','cace','quantiles','contact_rate']]

    def calc_by_org(self):
        print('Calculating Orgs...')
        c=Cace(self.voter_df,['election_w_year','org'], self.c_types).calc_cace()
        self.results[('org',None)] = c[['election_w_year','org','cace','quantiles','contact_rate']]

    def calc_by_demos(self):
        print('Starting Demographics...')
        for d1 in self.dem_list:
            print('Solo Dem: {}'.format(d1))
            if (d1,None) not in self.results:
                c = Cace(self.voter_df,['election_w_year',d1],
                             self.c_types).calc_cace()
                self.results[(d1,None)] = c[['election_w_year',d1,'cace','quantiles','contact_rate']]
            print('Paired Dems')
            self.dem_list.remove(d1)
            for d2 in self.dem_list:
                print('{}, {}'.format(d1,d2))
                if ((d1,d2) not in self.results) and ((d2,d1) not in self.results):
                    c = Cace(self.voter_df,['election_w_year',d1,d2],self.c_types).calc_cace()
                    self.results[(d1,d2)] = c[['election_w_year',d1,d2,'cace','quantiles','contact_rate']]
            self.dem_list.add(d1)

    def run(self):
        self.calc_all_orgs()
        self.calc_by_org()
        self.calc_by_demos()
