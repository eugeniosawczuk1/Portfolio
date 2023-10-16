import psycopg2 as ps
import psycopg2.extras as psx
import json
import logging
import pandas as pd
import numpy as np

class PostgreSQL:

    def __init__(self):
        '''
            Class to interact with Postgres Database from Amazon RDS.
        '''
        try:
            self.credentials = json.load(open('databases/amazonrds/credentials.json', 'r'))
            self.host = self.credentials['host']
            self.db_name = self.credentials['db_name']
            self.user = self.credentials['user']
            self.password = self.credentials['password']
            self.conn = ps.connect(f'host={self.host} dbname={self.db_name} user={self.user} password={self.password} port=5432')
            self.cursor = self.conn.cursor()
        except Exception as e:
            logging.error('Failed to connect to database, please check.')
            logging.error(e, exc_info=True)


    def upsert(self, df : pd.DataFrame, table : str):
    
        # if table doesn't exists, create
        if self.check_if_table_exists(table) == False:
            try:
                self.create_table_from_dataframe(df, table)
            except Exception as e:
                logging.error(e)
        # if index doesn't exists, create it in the ukey
        if self.check_if_index_exists(table) == False:
            try:
                self.create_index(table)
            except Exception as e:
                logging.error(e)
        df = df.fillna(np.nan).replace([np.nan], [None])
        values = [val for val in df.values.tolist()]
        columns = df.columns.to_list()
        vals = ''
        for col in columns:
            vals += '%s,'
        vals = vals[:-1]
        columns_str = ''
        for col in columns:
            columns_str += col + ','
        columns_str = columns_str[:-1]
        statement = f'INSERT INTO {table}({columns_str}) VALUES({vals}) '
        statement += 'ON CONFLICT (ukey) DO UPDATE SET '
        statement += ','.join([col + '=' + f'EXCLUDED.{col}' for col in df.columns.tolist()]) + ';'
        psx.execute_batch(self.cursor, statement, values)
        self.conn.commit()

    def insert(self, df: pd.DataFrame, table: str):
        '''
            Given a dataframe and a table name, insert data.
        '''
         # if table doesn't exists, create
        if self.check_if_table_exists(table) == False:
            try:
                self.create_table_from_dataframe(df, table)
            except Exception as e:
                logging.error(e)
        # if index doesn't exists, create it in the ukey
        if self.check_if_index_exists(table) == False:
            try:
                self.create_index(table)
            except Exception as e:
                logging.error(e)
        values = [val for val in df.values.tolist()]
        columns = df.columns.to_list()
        vals = ''
        for col in columns:
            vals += '%s,'
        vals = vals[:-1]
        columns_str = ''
        for col in columns:
            columns_str += col + ','
        columns_str = columns_str[:-1]
        statement = f'INSERT INTO {table} ({columns_str}) VALUES ({vals})'
        self.cursor.executemany(statement, values)
        self.conn.commit()

    def raw_query(self, query : str) -> list:
        '''
            Given a query, execute and return results.
        '''
        self.cursor.execute(query)
        r = self.cursor.fetchall()
        return r
    
    def query(self, query) -> pd.DataFrame:
        cursor = self.conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame.from_records(result, columns=[x[0] for x in cursor.description])
        return df
    

    def check_if_index_exists(self, table : str):
        query = f"SELECT constraint_name FROM information_schema.constraint_column_usage WHERE table_name = '{table}' AND column_name = 'ukey'"
        data = self.raw_query(query)
        if len(data) == 0:
            return False
        
    def create_index(self, table : str):
        query = f'ALTER TABLE {table} ADD CONSTRAINT ukey_unique_{table} UNIQUE(ukey)'
        self.cursor.execute(query)
        self.conn.commit()

    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        '''
            This method identify the dataframe column data types and 
            translate them to postgresql data types, then, create the table.
        '''
        logging.info(f'Creating table {table_name}')
        query = f'CREATE TABLE {table_name} ('
        try:
            for col in df.columns:
                column_data_type = str(df[col].dtype)
                # Consulto por el nombre literal ya que estos campos son comunes en todas las tablas segun el modelo.
                if col == 'UKEY':
                    query += f'{col} VARCHAR(8000),'
                elif col == 'FECHA_FILTRO':
                    query += f'{col} DATE,'
                elif col == 'FECHA_CREACION,':
                    query += f'{col} TIMESTAMP'
                elif col == 'FECHA_MODIFICACION':
                    query += f'{col} TIMESTAMP'
                # Pregunto por las columnas que tengan DATE en el nombre para asignarles el tipo de dato correcto.
                elif 'DATE' in col:
                    query += f'{col} TIMESTAMP,' 
                # Despues chequeo por el tipo.
                elif column_data_type == 'object':
                    query += f'{col} VARCHAR(8000),'
                elif 'int' in column_data_type:
                    query += f'{col} NUMERIC,'
                elif 'float' in column_data_type:
                    query += f'{col} FLOAT,'
                else:
                    # Si el tipo de dato no esta contemplado en el codigo, se envia una alerta al usuario y se crea el campo VARCHAR como default para evitar falla de proceso.
                    # TODO: Estar atento a los tipos de datos nuevos y agregarlos al codigo.
                    logging.warning(f'Warning: {col} has type {column_data_type} and not contemplated. Returning VARCHAR as default.')
                    query += f'{col} VARCHAR(600), '
            if query[-1] == ',':
                retval = query[:-1]
            else:
                retval = query
            query = retval + ')'
            try:
                self.cursor.execute(query)
                self.conn.commit()
            except Exception as e:
                logging.error(f'Failed, check. {e}')
        except Exception as e:
            logging.error(f'Failed to create table, check.\n\n{e}')


    
    def check_if_table_exists(self, table_name: str) -> bool:
        '''
            Query database dictionary to check if a table exists.
        '''
        query = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
        df = self.query(query)
        if df.empty == True:
            return False
        
    