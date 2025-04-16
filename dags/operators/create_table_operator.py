import pandas as pd
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.providers.postgres.hooks.postgres import PostgresHook

class CreateTableFromMetadataOperator(BaseOperator):
  def __init__(self, dwh_connection,file_path, schema_bronze, **kwargs):
    super().__init__(**kwargs)
    self.dwh_connection = dwh_connection
    self.schema_bronze = schema_bronze
    self.file_path = file_path
  def synchronize_table_schema(self, hook, table_name, metadata_df):
    existing_columns_query = f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = '{self.schema_bronze}' AND table_name = '{table_name}';
        """
    existing_columns = hook.get_pandas_df(existing_columns_query)
    existing_columns_dict = dict(
      zip(existing_columns["column_name"], existing_columns["data_type"]))

    metadata_columns_dict = dict(
      zip(metadata_df["col_name_des"], metadata_df["type_of_col_des"]))

    # Detect changes
    for col_name, col_type in metadata_columns_dict.items():
      if col_name not in existing_columns_dict:
        # Add new column
        alter_sql = f"ALTER TABLE {self.schema_bronze}.{table_name} ADD COLUMN {col_name} {col_type};"
        hook.run(alter_sql)
        self.log.info(f"Added column '{col_name}' with type '{col_type}'.")

  def execute(self, context: Context):
    file_name = self.file_path.split('/')[-1]
    download_task_id = f"download_file_{file_name}"
    file_path = context['ti'].xcom_pull(task_ids=download_task_id,
                                              key='local_file_path')

    try:
      metadata_df = pd.read_excel(file_path, sheet_name="metadata", header=5,
                                  usecols="B:C", dtype=str)
      metadata_df.columns = ['col_name_des', 'type_of_col_des']
      metadata_df.dropna(inplace=True)
      self.log.info(f"Loaded metadata for table creation: {metadata_df}")

      table_name = \
      pd.read_excel(file_path, sheet_name="metadata", header=None, nrows=1,
                    usecols=[1]).iloc[0, 0].strip()
      table_name = table_name.lower()

      hook = PostgresHook(postgres_conn_id=self.dwh_connection)

      # Check if the table exists and synchronize schema
      table_exists_query = f"""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = '{self.schema_bronze}' 
                AND table_name = '{table_name}'
            );
            """
      table_exists = hook.get_first(table_exists_query)[0]

      if table_exists:
        self.synchronize_table_schema(hook, table_name, metadata_df)
      else:
        columns = [f"{row['col_name_des']} {row['type_of_col_des']}" for _, row
                   in metadata_df.iterrows()]
        create_table_sql = f"CREATE TABLE {self.schema_bronze}.{table_name} ({', '.join(columns)});"
        self.log.info(f"Executing SQL to create table: {create_table_sql}")
        hook.run(create_table_sql)
        self.log.info(f"Table '{table_name}' created successfully.")

      context['ti'].xcom_push(key='table_name', value=table_name)
    except Exception as e:
      self.log.error(f"Failed to create or update table schema: {e}")
      raise
