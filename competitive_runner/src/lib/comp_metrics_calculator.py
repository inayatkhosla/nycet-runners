from lib.packages import *
import lib.db_puller as dbp
import lib.db_writer as dbw

import lib.margin_calculator as mgcl

import pdb

class MetricsGenerator(object):

    def __init__(self, data_level, password):
        self.data_level = data_level
        self.password = password

    def read_in_data(self):
        mc = mgcl.MarginCalculator(self.data_level, self.password)
        mc.run()
        self.race_metrics = mc.race_metrics

    def preprocess(self):
        df_base = self.race_metrics.copy()
        if self.data_level == 'ed':
            df_base['countyed'] = df_base['county'] + df_base['ed']
        elif self.data_level == 'hl':
            df_base['off_dist'] = df_base['office'] + '_' + df_base['districtnumber'].astype(str)
        df_base['eym'] = df_base['electionyear'].astype(str) + df_base['electionmonth'].astype(str)
        df_base['eym'] = df_base['eym'].astype(int)
        self.df_base = df_base

    def gen_hist_metrics(self):
        if self.data_level == 'ed':
            col = 'countyed'
        elif self.data_level == 'hl':
            col = 'off_dist'
        dist_df = (self.df_base.groupby([col])['race_id'].nunique().reset_index().
                  rename(columns={'race_id':'tot_elec'}))

        wpcts = self.df_base.groupby([col])['winning_party'].nunique().reset_index()
        wpcts.columns = [col,'winning_part_ct']

        med_mgn = self.df_base.groupby([col])['pol_lean_margin'].median().reset_index()
        med_mgn = med_mgn.rename(columns={'pol_lean_margin':'median_pl_margin'}).round(1)

        dist_df = dist_df.merge(med_mgn,on=col)
        self.dist_df = dist_df.merge(wpcts,on=col)

    def calc_db_dropoff(self):
        dbv = self.df_base.groupby(['countyed','electionyear','eym','office'])['total_vc'].sum().reset_index()
        dbv['doey'] = np.where((dbv['electionyear'] % 2 == 0),dbv['electionyear'],dbv['electionyear']-1)

        tb = dbv[dbv['office'].isin(['Governor','President'])]
        tb.rename(columns={'office':'tb_office','total_vc':'tb_votecount'},inplace=True)

        dbv = dbv.merge(tb[['countyed','tb_office','tb_votecount','doey']],on=['countyed','doey'],how='left')
        dbv['db_prop'] = ((dbv['total_vc']/dbv['tb_votecount']).round(2)).fillna(-1)
        self.dbv = dbv[['countyed','office','eym','total_vc','tb_votecount','db_prop']]
        self.df_base = self.df_base.merge(dbv,on=['countyed','office','eym'])

    def ed_gen_office_metrics(self):
        mr = self.df_base.loc[self.df_base.groupby(['countyed','office'])['eym'].idxmax()]
        mr = mr[['countyed','office','winning_party','winning_candidate','pol_lean_margin','db_prop']]

        mr_reshaped = mr.set_index(['countyed','office']).unstack().reset_index()
        mr_reshaped.columns = [' '.join(col).strip() for col in mr_reshaped.columns.values]
        mr_reshaped.columns = mr_reshaped.columns.str.replace('\s+', '_')

        abbs = {'winning_party': 'wp',
                'winning_candidate': 'wc',
                'pol_lean_margin': 'pl_margin',
                'db_prop': 'dbdo'}

        for i in abbs:
            mr_reshaped.columns = mr_reshaped.columns.str.replace(i,abbs[i])

        self.dist_df = self.dist_df.merge(mr_reshaped,on='countyed')

        self.dist_df['pl_margin_CityCouncil_Member'] = self.dist_df['pl_margin_CityCouncil_Member'].fillna(200)
        self.dist_df[['dbdo_Governor','dbdo_President']] = self.dist_df[['dbdo_Governor','dbdo_President']].fillna(1)
        self.dist_df[['dbdo_US_Senator','dbdo_CD','dbdo_SD','dbdo_AD','dbdo_CityCouncil_Member']] = self.dist_df[['dbdo_US_Senator','dbdo_CD','dbdo_SD','dbdo_AD','dbdo_CityCouncil_Member']].fillna(-1)
        fillcols = ['pl_margin_President','pl_margin_US_Senator','pl_margin_CD','pl_margin_SD','pl_margin_AD','pl_margin_Governor']
        self.dist_df[fillcols] = self.dist_df[fillcols].fillna(0)

        self.dist_df = self.dist_df[['countyed','tot_elec','median_pl_margin',
                                     'winning_part_ct',
                                     'wp_President','wc_President','pl_margin_President','dbdo_President',
                                     'wp_Governor','wc_Governor','pl_margin_Governor','dbdo_Governor',
                                     'wp_US_Senator','wc_US_Senator','pl_margin_US_Senator','dbdo_US_Senator',
                                     'wp_CD','wc_CD','pl_margin_CD','dbdo_CD',
                                     'wp_SD','wc_SD','pl_margin_SD','dbdo_SD',
                                     'wp_AD','wc_AD','pl_margin_AD','dbdo_AD',
                                     'wp_CityCouncil_Member','wc_CityCouncil_Member','pl_margin_CityCouncil_Member','dbdo_CityCouncil_Member']]


    def hl_gen_office_metrics(self):
        mr = self.df_base.loc[self.df_base.groupby(['off_dist'])['eym'].idxmax()]
        mr = mr[['off_dist','winning_pol_lean','pol_lean_margin','winning_party','winning_candidate']]
        mr.rename(columns={'pol_lean_margin':'most_rec_pl_margin'},inplace=True)
        self.dist_df = self.dist_df.merge(mr,on='off_dist')
        self.dist_df['office'] = self.dist_df['off_dist'].apply(lambda x: x.rsplit('_',1)[0])
        self.dist_df['district'] = self.dist_df['off_dist'].apply(lambda x: x.rsplit('_',1)[1])
        self.dist_df = self.dist_df[['office','district','tot_elec',
                                     'median_pl_margin','winning_part_ct',
                                     'winning_pol_lean','most_rec_pl_margin',
                                     'winning_party','winning_candidate']]


    def gen_office_metrics(self):
        if self.data_level == 'ed':
            self.ed_gen_office_metrics()
        elif self.data_level == 'hl':
            self.hl_gen_office_metrics()

    def run(self):
        self.read_in_data()
        self.preprocess()
        self.gen_hist_metrics()
        if self.data_level == 'ed':
            self.calc_db_dropoff()
        else:
            pass
        self.gen_office_metrics()
        dbwriter = dbw.DBWriter(self.password)
        print('writing to db')
        dbwriter.to_pg(self.dist_df,'{}_metrics'.format(self.data_level),'|')
