import pandas as pd
import numpy as np

class ParquetToJson:
    def __init__(self, filepath):
        self.filepath = filepath

    def extract_values(self,row):
        try:
            return pd.Series(row['params'])
        except Exception as e:
            print(f"Error extracting values: {e}")
            # Rerurn in case of error
            return pd.Series([np.nan, np.nan, np.nan, np.nan]) 

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
            self.df.dropna(inplace=True)
            self.df[['apartment', 'apartments', 'page', 'requests']] = self.df.apply(lambda row: self.extract_values(row), axis = 1)
            self.df.drop('params', axis=1, inplace=True)
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


if __name__ == "__main__":
    filepath = 'files/parquet/homelike_assignment_data.parquet'
    output_filepath = 'files/json/homelike_assignment_data.json'

    converter = ParquetToJson(filepath)
    converter.read_dataset()
    converter.preprocess_dataset()
    converter.export_to_json(output_filepath)