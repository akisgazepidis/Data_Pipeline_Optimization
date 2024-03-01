from scripts.utils import ParquetToJson


filepath = 'files/parquet/homelike_assignment_data.parquet'
output_filepath = 'files/json/homelike_assignment_data.json'

converter = ParquetToJson(filepath)
converter.read_dataset()
converter.preprocess_dataset()
converter.export_to_json(output_filepath)