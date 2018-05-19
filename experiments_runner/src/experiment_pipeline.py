from lib.packages import *
from lib.db.experiment_table_puller import ExperimentTablePuller
from lib.format.experiment_by_voter_formatter import ExperimentByVoterFormatter
from lib.cace.cace_runner import CaceRunner
from lib.format.db_formatter import DBFormatter
from lib.db.db_writer import DBWriter

"""
PROCESS

    Step 1: Pull experiments, contact_history, persons tables from ny db.
            Dependencies: org_mapping

    Step 2: Run ExperimentByVoterFormatter to create base file.
            Dependencies: election_mapping, contact_types, TablePuller (experiments and call history)

    Step 3: Run CaceRunner to determine cace results.
            Dependencies: voter_df, contact_types

    Step 4: Run DBFormatter to format cace results for db.

    Step 5: Run DBWriter to upload cace results into cace_metrics table in apps db.

INSTRUCTIONS

    Install Docker
        Mac: https://docs.docker.com/docker-for-mac/install/
        Windows: https://docs.docker.com/docker-for-windows/install/

    Add new experiments to the experiments table in the ny database

    Update the following files in the src/input folder
        1. contact_types.csv
            - list all contact types to include in experiment analysis
        2. election_mapping.csv
            - year: year of election
            - election: election name in db
            - start: date experiment started
            - end: date experiment ended; normally day of election
        3. org_mapping.csv
            - contact_history: org name as it appears in contacthistory table
            - experiments: org name as it appears in experiments table

    Once above steps are done...

    Run these commands in a terminal:
        1. docker build . -t nycet
        2. docker run -t -e PASSWORD=type-password-here nycet
"""

class ExperimentPipeline():

    def __init__(self, db_password):
        self.db_password = db_password
        self.election_dates = pd.read_csv('input/election_mapping.csv')
        self.contact_types = pd.read_csv('input/contact_types.csv')
        self.org_map = pd.read_csv('input/org_mapping.csv')

    def run(self):
        print('1/5 Started pulling database tables.')
        etp = ExperimentTablePuller(self.org_map, self.db_password)
        etp.run()
        print('1/5 Finished.')

        print('2/5 Started generating experiment by voter file.')
        ebvf = ExperimentByVoterFormatter(self.election_dates, self.contact_types, etp)
        ebvf.run()
        del etp.experiments, etp.contact_history, etp.persons
        print('2/5 Finished.')

        print('3/5 Started calculating CACE metrics.')
        cr = CaceRunner(ebvf.voter_df, self.contact_types)
        cr.run()
        print('3/5 Finished.')

        print('4/5 Started formatting CACE metrics for db.')
        dbf = DBFormatter(cr.results)
        dbf.run()
        print('4/5 Finished.')

        print('5/5 Started writing CACE metrics to db.')
        dw = DBWriter(self.db_password)
        dw.to_pg(dbf.cace_db,table_name='new_cace_metrics',delim='\t')
        print('5/5 ALL DONE.')


if __name__ == '__main__':

    db_password = sys.argv[1]
    ep = ExperimentPipeline(db_password=db_password)
    ep.run()
