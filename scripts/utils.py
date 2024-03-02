import pandas as pd
import numpy as np
from datetime import datetime

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
            #self.df[['apartment', 'apartments', 'page', 'requests']] = self.df.apply(lambda row: self.extract_values(row), axis = 1)
            #self.df.drop('params', axis=1, inplace=True)
            self.df['ts'] = self.df['ts'].apply(lambda x: str(x) if pd.notnull(x) else '')
            self.df['ts'] = self.df['ts'].apply(lambda x: self.fix_dates1(x) if ('-' in x and x != '') else x)
            self.df['ts'] = self.df['ts'].apply(lambda x: self.fix_dates2(x) if ('/' not in x and x != '') else x)
            self.df = self.df[self.df.ts != '']
            self.df['event_type'] = self.df['event_type'].apply(self.replace_none_with_unknown)
            self.df['user_country'] = self.df['user_country'].apply(self.replace_none_with_unknown)
            self.df['user_agent'] = self.df['user_agent'].apply(self.replace_none_with_unknown)
            self.df['page_country'] = self.df['page_country'].apply(self.replace_none_with_unknown)
            self.df['env'] = self.df['env'].apply(self.replace_none_with_unknown)

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


# if __name__ == "__main__":
#     filepath = 'files/parquet/homelike_assignment_data.parquet'
#     output_filepath = 'files/json/homelike_assignment_data.json'

#     converter = ParquetToJson(filepath)
#     converter.read_dataset()
#     converter.preprocess_dataset()
#     converter.export_to_json(output_filepath)