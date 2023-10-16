import psycopg2 as ps
import json
import logging

class PostgreSQL:

    def __init__(self):
        '''
            Class to interact with Postgres Database.
        '''
        try:
            self.credentials = json.load(open('databases/onpremise/credentials.json', 'r'))
            self.db_name = self.credentials['db_name']
            self.user = self.credentials['user']
            self.password = self.credentials['password']
            self.conn = ps.connect(f'dbname={self.db_name} user={self.user} password={self.password} port=1524')
            self.cursor = self.conn.cursor()
        except Exception as e:
            logging.error('Failed to connect to database, please check.')
            logging.error(e, exc_info=True)

    def query(self, query : str) -> list:
        '''
            Given a query, execute and return results.
        '''
        self.cursor.execute(query)
        r = self.cursor.fetchall()
        return r