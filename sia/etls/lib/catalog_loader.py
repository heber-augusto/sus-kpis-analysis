from google.cloud import storage

class DeltaLakeDatabaseGsCreator:
    def __init__(self, spark_session, storage_client, gs_bucket_id, database_location, database_name):
        self.spark_session = spark_session
        self.storage_client = storage_client
        self.gs_bucket_id = gs_bucket_id
        self.database_location = database_location
        self.database_name = database_name
        self.db_folder_path = f'gs://{self.gs_bucket_id}/{self.database_location}/{self.database_name}.db'
        
    def create_database(self, use_db_folder_path = True):
        if use_db_folder_path == True:
            query_db_folder_path = f"LOCATION '{self.db_folder_path}'"
        else:
            query_db_folder_path = ""
        # Criação do banco de dados Delta Lake
        create_db_query = f"CREATE DATABASE IF NOT EXISTS {self.database_name} {query_db_folder_path}"
        self.spark_session.sql(create_db_query)
        
        print(f"Banco de dados {self.database_name} criado.")
        
    def recreate_tables(self):
        # Cliente do Google Cloud Storage
        print(f"listando conteúdos do bucket {self.gs_bucket_id} do caminho {self.database_location} e database {self.database_name}")
        # Lista os blobs no bucket
        prefix = f'{self.database_location}/{self.database_name}.db/'
        print(f'prefix: {prefix}')
        blobs = self.storage_client.list_blobs(
            bucket_or_name = self.gs_bucket_id,
            prefix=prefix,  # <- you need the trailing slash
            delimiter="/")
        
        temp_blobs = [blob for blob in blobs]
        table_list = [prefix for prefix in blobs.prefixes]
        for table_name in table_list:
            #print(table_name)

            # Criação da tabela Delta Lake
            delta_location = f"{self.db_folder_path}/{table_name}"
            #print(delta_location)
            
            # Criação da tabela Delta Lake
            create_table_query = f"CREATE TABLE IF NOT EXISTS {self.database_name}.{table_name} USING delta LOCATION '{delta_location}'"
            self.spark_session.sql(create_table_query)
            #print(f"Tabela {table_name} criada")
            #print(f"Tabela {table_name} criada com comando {create_table_query}")
        
        print("Recriação das tabelas concluída.")
