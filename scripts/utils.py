import pandas as pd
import numpy as np
from datetime import datetime
import json
import requests
import psycopg2
import dateutil.parser

class ParquetToJson:
    def __init__(self, filepath):
        self.filepath = filepath

    def extract_values(self,row):
        '''Function that converts a dictionary into a pandas series.'''
        try:
            return pd.Series(row['params'])
        except Exception as e:
            print(f"Error while extracting values: {e}")
            # Rerurn in case of error
            return pd.Series([np.nan, np.nan, np.nan, np.nan]) 
    
    def fix_dates1(self, x):
        '''Fuction that fix timestamps from '%Y-%m-%dT%H:%M:%S' to 'dd/mm/yyyy HH:MM:SS' format.'''
        try:
            dt_object = datetime.strptime(x, '%Y-%m-%dT%H:%M:%S')
            formatted_timestamp = str(dt_object.strftime('%d/%m/%Y %H:%M:%S'))

            return formatted_timestamp
        except Exception as e:
            print(f"Error while converting ts from '%Y-%m-%dT%H:%M:%S' to 'dd/mm/yyyy HH:MM:SS' format: {e}")
            return np.nan
        
    def fix_dates2(self,x):
        '''Fuction that fix timestamps from floats to 'dd/mm/yyyy HH:MM:SS' format.'''
        try:
            dt_object = datetime.utcfromtimestamp(float(x))
            formatted_timestamp = str(dt_object.strftime('%d/%m/%Y %H:%M:%S'))

            return formatted_timestamp
        except Exception as e:
            print(f"Error while converting ts from floats to 'dd/mm/yyyy HH:MM:SS' format: {e}")
            return np.nan
    
    def replace_none_with_unknown(self,x):
        '''Function that replaces empty values with 'Uknown' value.'''
        try:
            if not x:
                return 'Unknown'
            else:
                return x
        except Exception as e:
            print(f"Error while replacing None values with Unknown {e}")
            return np.nan
        
    def create_session_id_user_agent_dict(self,session_col, user_agent_col):
        '''Finction that creates a dictionary with unique session_ids as keys and user_agents as values'''
        try:
            session_id_user_agent_tuple_list = [tpl for tpl in set(list(zip(list(user_agent_col), list(session_col)))) if tpl[1]]
            session_id_user_agent_tuple_dict = {key:value for key, value in session_id_user_agent_tuple_list}
            return session_id_user_agent_tuple_dict
        except Exception as e:
            print(F'Error while creating  the dictionary : {e}')
            return np.nan
    
    def fill_session_id(self,row, dict):
        '''Function that recieves a df's row for None session_id and a dictionary as inputs and returns then new row with new session_id '''
        
        try:
            row['session_id']  = dict[ row['user_agent']]
            return row
        except Exception as e:
            print(f'Error while finding the new session_id: {e}')
            return np.nan

    def read_dataset(self):
        '''Read the Parquet file data'''
        try:
            self.df = pd.read_parquet(self.filepath)
            print("Successfull read of the dataset")
        except Exception as e:
            print(f"Error in reading the dataset: {e}")

    def preprocess_dataset(self):
        '''Preprocess the Parquet file data'''
        if hasattr(self, 'df'):
            # Do all the preprocessing omn the dataset
            self.df[['apartment', 'apartments', 'page', 'requests']] = self.df.apply(lambda row: self.extract_values(row), axis = 1)
            self.df.drop('params', axis=1, inplace=True)
            self.df['ts'] = self.df['ts'].apply(lambda x: str(x) if pd.notnull(x) else '')
            self.df['ts'] = self.df['ts'].apply(lambda x: self.fix_dates1(x) if ('-' in x and x != '') else x)
            self.df['ts'] = self.df['ts'].apply(lambda x: self.fix_dates2(x) if ('/' not in x and x != '') else x)
            self.df = self.df[self.df.ts != '']
            self.df['event_type'] = self.df['event_type'].apply(self.replace_none_with_unknown)
            self.df['user_country'] = self.df['user_country'].apply(self.replace_none_with_unknown)
            self.df['user_agent'] = self.df['user_agent'].apply(self.replace_none_with_unknown)
            self.df['page_country'] = self.df['page_country'].apply(self.replace_none_with_unknown)
            self.df['env'] = self.df['env'].apply(self.replace_none_with_unknown)

            self.session_id_user_agent_tuple_dict =  self.create_session_id_user_agent_dict(self.df.session_id, self.df.user_agent)

            self.df = self.df.apply(lambda row: self.fill_session_id(row, self.session_id_user_agent_tuple_dict) if pd.isnull(row['session_id']) else row, axis=1)
            print("Data preprocessed succeddfully")
        else:
            print("Please read the data first.")
    
    def export_to_json(self, output_filepath):
        '''Export the data to a json file'''
        if hasattr(self, 'df'):
            try:
                self.df.to_json(output_filepath, orient='records', lines=True)
                print(f"Data exported to JSON successfully at {output_filepath}")
            except Exception as e:
                print(f"Error exporting data to JSON {e}")
        else:
            print("Data not read or preprocessed. Please read and preprocess the data first.")

class PostgresFunctions:
    def __init__(self, cred_path, output_filepath):
        self.cred_path = cred_path
        self.output_filepath = output_filepath

        with open(cred_path) as json_file:
            cred_data = json.load(json_file)
        self.username = cred_data['username']
        self.password = cred_data['password']
        self.host = cred_data['host']
        self.port = cred_data['port']
        self.db = cred_data['db']

        with open(output_filepath, 'r') as file:
            self.dict_data = [json.loads(line) for line in file]


    def check_postgres_connection(self):
        try:
            conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.db,
            user=self.username,
            password=self.password
            )
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            if cursor.fetchone()[0] == 1:
                print("Successful Test Connection to PostgreSQL")
            conn.close()
            return True
        except Exception as e:
            print("Failed test connection to PostgreSQL:", e)
            return False
        
    def create_postgres_connection(self):
        print('Connecting to Postgres DB...')
        self.conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    database=self.db,
                    user=self.username,
                    password=self.password
                )
    
    def create_postges_table_if_not_exist(self):
        print('Checking if table exist..')
        create_table_queries = ["""
        CREATE TABLE IF NOT EXISTS dim_sessions (
            session_id TEXT PRIMARY KEY,
            user_agent TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fct_customer_events_data (
            customer_events_data_id SERIAL PRIMARY KEY,
            event_type TEXT,
            session_id TEXT,
            user_country TEXT,
            user_agent TEXT,
            page_country TEXT,
            env TEXT,
            apartment TEXT,
            apartments TEXT,
            page TEXT,
            requests TEXT,
            date TIMESTAMP
        );
        """
        ]
        for query in create_table_queries:
            self.conn.cursor().execute(query)
            self.conn.commit()

    def update_dim_and_fct_tables(self):
        print('Updating dimension and fact tables...')
        counter = 0
        for record in self.dict_data[0:10]:
                event_type  = record["event_type"]
                session_id = record["session_id"]
                user_country = record["user_country"]
                user_agent = record['user_agent']
                page_country = record['page_country']
                env = record['env']
                apartment = record['apartment']
                apartments = str(record['apartments'])
                page = str(record['page'])
                requests = record['requests']

                parsed_date = dateutil.parser.parse(record['ts'])
                date = parsed_date.strftime('%d/%m/%Y %H:%M:%S')

                insert_query = f"INSERT INTO fct_customer_events_data ( event_type, session_id, user_country, user_agent, page_country, env, apartment, apartments, page, requests, date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                values = ( event_type, session_id, user_country, user_agent, page_country, env, apartment, apartments, page, requests, date )

                self.conn.cursor().execute(insert_query, values)
                self.conn.commit()


                insert_query2 = f"INSERT INTO dim_sessions ( session_id, user_agent) VALUES (%s, %s) ON CONFLICT DO NOTHING"
                values = ( session_id, user_agent )

                self.conn.cursor().execute(insert_query2, values)
                self.conn.commit()

                counter +=1
        print(f'{counter} records inserted.')

    def close_connection(self):
        self.conn.cursor().close()
        self.conn.close()

# if __name__ == "__main__":
#     filepath = 'files/parquet/homelike_assignment_data.parquet'
#     output_filepath = 'files/json/homelike_assignment_data.json'

#     converter = ParquetToJson(filepath)
#     converter.read_dataset()
#     converter.preprocess_dataset()
#     converter.export_to_json(output_filepath)