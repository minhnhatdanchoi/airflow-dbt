import os
import pandas as pd
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from mimetypes import guess_type


class CleanAndLoadDataOperator(BaseOperator):
    def __init__(self, dwh_connection,file_path, schema_bronze, **kwargs):
        super().__init__(**kwargs)
        self.dwh_connection = dwh_connection
        self.schema_bronze = schema_bronze
        self.metadata_sheet_name = "metadata"
        self.file_path = file_path
        self.sheet_name = None

    def validate_excel_file(self, file_path):
        try:
            mime_type = guess_type(file_path)[0]
            self.log.info(f"MIME type detected by mimetypes: {mime_type}")
            valid_mime_types = [
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']
            valid_extensions = ['.xlsx']
            if mime_type in valid_mime_types or file_path.lower().endswith(
                tuple(valid_extensions)):
                pd.read_excel(file_path, engine='openpyxl')
                self.log.info(
                    f"File '{file_path}' passed validation as a valid Excel file.")
                return True
            else:
                raise ValueError(
                    f"File '{file_path}' has an invalid MIME type or extension.")
        except Exception as e:
            self.log.error(f"File validation failed for '{file_path}': {e}")
            raise

    def load_data_to_table(self, file_path, table_name):
        try:
            self.log.info(f"Validating Excel file: {file_path}")
            self.validate_excel_file(file_path)
            self.log.info(f"Loading data from file: {file_path}")
            self.log.info(f"File size: {os.path.getsize(file_path)} bytes")

            sheet_names = pd.ExcelFile(file_path, engine="openpyxl").sheet_names
            self.log.info(f"Available sheets in the file: {sheet_names}")

            if not self.sheet_name or self.sheet_name not in sheet_names:
                raise ValueError(f"Sheet '{self.sheet_name}' not found in the file.")

            data_df = pd.read_excel(file_path, sheet_name=self.sheet_name, dtype=str,
                                    engine='openpyxl')
            metadata_df = pd.read_excel(file_path,
                                        sheet_name=self.metadata_sheet_name,
                                        dtype=str, engine='openpyxl', header=5)

            column_mappings = dict(
                zip(metadata_df["col_name_excel"], metadata_df["col_name_des"]))
            column_types = dict(
                zip(metadata_df["col_name_des"], metadata_df["type_of_col_des"]))

            if any(col not in data_df.columns for col in column_mappings):
                missing_columns = [col for col in column_mappings if
                                   col not in data_df.columns]
                self.log.error(f"Missing columns in the data: {missing_columns}")
                raise ValueError(f"Columns missing: {missing_columns}")

            data_df = data_df[
                [col for col in column_mappings if col in data_df.columns]].rename(
                columns=column_mappings)
            data_df = self.clean_data(data_df, column_types)
            self.replace_data_in_table(data_df, table_name)
        except Exception as e:
            self.log.error(f"Failed to load data into table '{table_name}': {e}")
            raise

    def clean_data(self, df, column_types):
        for column, dtype in column_types.items():
            if column not in df.columns:
                self.log.warning(f"Column '{column}' is missing in the data.")
                continue
            if dtype.lower() in ["timestamp", "datetime"]:
                df[column] = pd.to_datetime(df[column], errors='coerce')
            elif dtype == "float":
                df[column] = pd.to_numeric(df[column], errors='coerce')
            elif dtype == "int":
                df[column] = pd.to_numeric(df[column], errors='coerce',
                                           downcast="integer")
            else:
                df[column] = df[column].replace(r'^\s*$', None,
                                                regex=True)
                df[column] = df[column].replace(
                    {pd.NA: None, "nan": None, "NaT": None})

        df = df.where(pd.notnull(df), None)
        return df

    def replace_data_in_table(self, df, table_name):
        hook = PostgresHook(postgres_conn_id=self.dwh_connection)
        table_name = table_name.lower()
        try:
            self.log.info(f"Preparing to replace all data in table '{table_name}'.")
            engine = hook.get_sqlalchemy_engine()
            with engine.begin() as connection:
                delete_sql = f"DELETE FROM {self.schema_bronze}.{table_name};"
                self.log.info(f"Executing SQL: {delete_sql}")
                connection.execute(delete_sql)
                self.log.info(f"Deleted all data from table '{table_name}'.")
                df.to_sql(table_name, con=connection, schema=self.schema_bronze,
                          if_exists='append', index=False)
                self.log.info(
                    f"New data successfully inserted into table '{table_name}'.")
                self.log.info(
                    f"Inserted {len(df)} records into table '{table_name}'.")
        except Exception as e:
            self.log.error(f"Failed to replace data in table '{table_name}': {e}")
            raise

    def execute(self, context: Context):
        file_name = self.file_path.split('/')[
            -1]  # Extract the file name from the path
        download_task_id = f"download_file_{file_name}"
        file_path = context['ti'].xcom_pull(task_ids=download_task_id,
                                            key='local_file_path')
        table_name = context['ti'].xcom_pull(task_ids='create_table',
                                             key='table_name')
        self.sheet_name = "data"
        self.load_data_to_table(file_path, table_name)
        self.log.info(
            f"Processed file '{file_path}' and loaded data into table '{table_name}'.")