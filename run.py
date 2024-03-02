from scripts.utils import ParquetToJson, PostgresFunctions


# filepath = 'files/parquet/homelike_assignment_data.parquet'
output_filepath = 'files/json/homelike_assignment_data.json'

# converter = ParquetToJson(filepath)
# converter.read_dataset()
# converter.preprocess_dataset()
# converter.export_to_json(output_filepath)

cred_path = 'files/json/credentials.json'

postgres = PostgresFunctions(cred_path, output_filepath)
postgres.check_postgres_connection()
postgres.create_postgres_connection()
postgres.create_postges_table_if_not_exist()
postgres.update_dim_and_fct_tables()
postgres.close_connection()