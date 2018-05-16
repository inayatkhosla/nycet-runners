from lib.packages import *

class DBPuller():
    def __init__(self, db, password):
        self.dbname = db
        self.password = password
        self.HOST = 'nycet-postgres.c1swnd7n2f4l.us-east-1.rds.amazonaws.com'
        self.USER = 'apps_user'

    def connect(self):
        conn_string = ("host='{}' user ='{}' password='{}' dbname='{}'".
                        format(self.HOST,self.USER,self.password,self.dbname))
        self.conn = psycopg2.connect(conn_string)
        #print("Connected!\n")

    def pull(self, table):
        self.connect()
        query = ("SELECT * FROM {};".format(table))
        print('pulling {}'.format(table) )
        results = pd.read_sql(query,self.conn)
        self.conn.close()
        print(results.shape)
        return results

    def drop(self, table):
        self.connect()
        cur = self.conn.cursor()
        cur.execute("DROP TABLE {};".format(table))
        self.conn.commit()
        self.conn.close()
