try:
    from .packages import *
except:
    from packages import *

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
        data = io.StringIO() #create file in memory/as string
        df.to_csv(data, header=False, index=False,sep=delim) #write csv to that StringIO
        data.seek(0) #start at beginning of "csv"
        raw = self.engine.raw_connection() #create raw connection
        curs = raw.cursor() #cursor
        curs.execute("DROP TABLE IF EXISTS {}".format(table_name)) #drop table if exists
        empty_table = pd.io.sql.get_schema(df,table_name,con=self.engine) #create schema from df
        empty_table = empty_table.replace('"', '') #???? who knows
        curs.execute(empty_table) #create table in db from schema
        curs.copy_from(data, table_name, sep = delim) #copy StringIO
        curs.connection.commit() #close connection
        self.engine_conn.close()
