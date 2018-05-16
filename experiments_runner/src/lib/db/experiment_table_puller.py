from lib.packages import *
from .db_puller import DBPuller

class ExperimentTablePuller():

    def __init__(self, org_map, password):
        self.org_map = org_map
        self.password = password

    def connect_to_db(self):
        self.dbpuller = DBPuller(db='ny',password=self.password)

    def pull_tables(self):
        self.experiments = self.dbpuller.pull('experiments')
        self.contact_history = self.dbpuller.pull('contacthistory')
        self.persons = self.dbpuller.pull('person')

    def add_election_year(self):

        def election_year(x):
            return x.election + ' - ' + str(x.year)

        self.experiments['election_w_year'] = self.experiments.apply(election_year, axis=1)

    def clean_orgs(self):
        mod_contacts_df = self.contact_history.merge(self.org_map, left_on='org', right_on='contact_history')
        self.contact_history = mod_contacts_df[mod_contacts_df['org']!='NYIC'] #drop 1200 NYIC contacts bc supposed to be individual orgs

    def run(self):
        self.connect_to_db()
        self.pull_tables()
        self.add_election_year()
        self.clean_orgs()
