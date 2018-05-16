from lib.packages import *

class DBWriter():
    def __init__(self,password):
        self.password = password
        self.DBNAME = 'apps'
        self.HOST = 'nycet-postgres.c1swnd7n2f4l.us-east-1.rds.amazonaws.com'
        self.USER = 'apps_user'

    def connect(self):
        self.engine = create_engine('postgresql+psycopg2://{}:{}@{}:5432/{}'.
                                format(self.USER,self.password,self.HOST,self.DBNAME))
        self.engine_conn = self.engine.connect()

    def to_pg(self,df,table_name,delim):
        self.connect()
        df.fillna('',inplace=True)
        data = io.StringIO()
        df.to_csv(data, header=False, index=False,sep=delim)
        data.seek(0)
        raw = self.engine.raw_connection()
        curs = raw.cursor()
        curs.execute("DROP TABLE IF EXISTS {}".format(table_name))
        empty_table = pd.io.sql.get_schema(df,table_name,con=self.engine)
        empty_table = empty_table.replace('"', '')
        curs.execute(empty_table)
        curs.copy_from(data, table_name, sep = delim)
        curs.connection.commit()
        self.engine_conn.close()
