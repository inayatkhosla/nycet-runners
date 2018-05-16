from .packages import *
import lib.db_puller as dbp
import lib.db_writer as dbw


class MarginCalculator(object):

    def __init__(self, data_level, password):
        self.data_level = data_level
        self.password = password
        self.ED_COLS = ['office','county','ed','electionyear','electionmonth']
        self.HL_COLS = ['office','districtnumber','electionyear','electionmonth']


    def read_in_data(self):
        reload(dbp)
        dbpuller_ny = dbp.DBPuller('ny',self.password)
        dbpuller_app = dbp.DBPuller('apps',self.password)

        ed_results = dbpuller_ny.pull('electionresults') ## CHANGE TO NY DB
        ed_results['ed'] = ed_results['ed'].apply(lambda x:x.replace('  ',' '))
        #ed_avail = ed_results['office'] + ed_results['districtnumber']

        #hl_results = dbpuller_app.pull('hl_results')
        #hl_results['districtnumber'] = hl_results['districtnumber'].astype(str)
        #hl_results[~(hl_results['office'] + hl_results['districtnumber']).isin(ed_avail)]

        #ed_results['ed_data'] = 1
        #hl_results['ed_data'] = 0
        #self.results = pd.concat([ed_results,hl_results])
        self.results = ed_results

        self.districts = dbpuller_ny.pull('electiondistricts')
        self.comp_races = dbpuller_ny.pull('comp_races')
        self.maps_office = dbpuller_ny.pull('maps_office')
        self.maps_pollean = dbpuller_ny.pull('maps_pollean')



    def clean_data(self):
        roi = list(self.comp_races['race_type'].unique())
        self.results = self.results[self.results['office'].isin(roi)]

        mo = dict(zip(self.maps_office['office'], self.maps_office['map']))
        self.results['office'] = self.results['office'].replace(mo)

        mpl = dict(zip(self.maps_pollean['party'], self.maps_pollean['map']))
        self.results['pol_lean'] = self.results['party'].map(mpl).fillna('other')


    def exclude_primaries(self):
        self.results = self.results[(self.results['electionmonth'] == 11)]


    def preprocess_data_level(self):
        if self.data_level == 'ed':
            self.ucols = self.ED_COLS
            #self.results = self.results[self.results['ed_data'] == 1]
        elif self.data_level == 'hl':
            self.ucols = self.HL_COLS
            coi = ['office','electionyear','electionmonth','districtnumber','candidate','party','pol_lean']
            self.results = (self.results.groupby(coi)['votecount'].sum().reset_index())


    def add_raceid(self):
        """
        adds a unique race identifier; handles both ED and higher level data
        """
        race_df = self.results[self.ucols].drop_duplicates()
        race_df = race_df.reset_index()
        race_df.rename(columns={'index':'race_id'},inplace=True)
        self.results = self.results.merge(race_df,on=self.ucols)
        self.race_df = race_df


    def add_comp_counts(self):
        """
        adds columns listing number of candidates, parties, and
        political leanings in each race
        """
        comp_cts = self.results.groupby('race_id')['candidate','party','pol_lean'].nunique().reset_index()
        comp_cts.columns = ['race_id','cand_ct','party_ct','pol_lean_ct']
        self.race_df = self.race_df.merge(comp_cts,on='race_id')

    def calc_tvc(self):
        tvc = self.results.groupby('race_id')['votecount'].sum().reset_index()
        tvc.rename(columns={'votecount':'total_vc'},inplace=True)
        self.tvc = tvc

    def add_tvc(self):
        race_df = self.race_df.merge(self.tvc,on='race_id')
        self.race_df = race_df[race_df['total_vc'] > 0]

    def calc_vote_perc(self,df,cut):
        """
        calculates vote proportion by party, candidate, or
        political leaning
        """
        svc = df.groupby(['race_id',cut])['votecount'].sum().reset_index()
        svc = svc.merge(self.tvc,on='race_id')
        svc['vote_prop'] = (svc['votecount']/svc['total_vc'])*100
        return svc

    def calc_margin(self,df,cut):
        df = df.sort_values("vote_prop")
        first = (df.groupby(['race_id'])['vote_prop'].nth(-1).reset_index().
                            rename(columns={'vote_prop':'first'}))
        second = (df.groupby(['race_id'])['vote_prop'].nth(-2).reset_index().
                            rename(columns={'vote_prop':'second'}))
        mov = first.merge(second,on='race_id')
        mov['margin'] = (mov['first'] - mov['second']).round(1)
        mov = mov[['race_id','margin']]
        mov.rename(columns={'margin':'{}_margin'.format(cut)},inplace=True)
        return mov

    def get_winners(self,df,cut):
        winn = df.loc[df.groupby(['race_id'])['votecount'].idxmax()]
        winn = winn[['race_id',cut]]
        winn.rename(columns={cut:'winning_{}'.format(cut)},inplace=True)
        return winn

    def add_winn_margins(self):
        empt = []
        for i in ['candidate','party','pol_lean']:
            prop = self.calc_vote_perc(self.results,i)
            winners = self.get_winners(self.results,i)
            mov = self.calc_margin(prop,i)
            winmov = winners.merge(mov,on='race_id',how='left')
            empt.append(winmov)
        winn_margins = empt[0].merge((empt[1].merge(empt[2],on='race_id',how='outer')),how='outer')
        self.race_metrics = self.race_df.merge(winn_margins,on='race_id',how='left')

    def fill_nulls(self):
        cols = [col for col in self.race_metrics.columns if 'margin' in col]
        self.race_metrics[cols] = self.race_metrics[cols].fillna(100)

    def run(self):
        self.read_in_data()
        self.clean_data()
        self.exclude_primaries()
        self.preprocess_data_level()
        self.add_raceid()
        self.add_comp_counts()
        self.calc_tvc()
        self.add_tvc()
        self.add_winn_margins()
        self.fill_nulls()
