from google.cloud import storage
import os
from pathlib import Path

def get_folders_from_prefix(storage_client, bucket_id, prefix):
    blobs = storage_client.list_blobs(
        bucket_or_name = bucket_id,
        prefix=prefix,  # <- you need the trailing slash
        delimiter="/")

    temp_blobs = [blob for blob in blobs]
    # Dividir a string usando '/' como delimitador e pegar o último elemento
    folder_list = [ \
      prefix.split('/')[-2]
      for prefix in blobs.prefixes]
    return folder_list


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
        # Dividir a string usando '/' como delimitador e pegar o último elemento
        table_list = [ \
          prefix.split('/')[-2]
          for prefix in blobs.prefixes]
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


def load_entire_catalog(spark_session, storage_client, bucket_id, lake_prefix, lake_zones = ['bronze', 'silver'], use_db_folder_path = True):
    for lake_zone in lake_zones:
        database_list = get_folders_from_prefix(
            storage_client, 
            bucket_id, 
            prefix = f'{lake_prefix}/{lake_zone}/')
        print(database_list)
        for database_name in database_list:
            database_location = f'{lake_prefix}/{lake_zone}'  # Substitua com o local do seu banco de dados Delta Lake
            db_creator = DeltaLakeDatabaseGsCreator(
                spark_session = spark_session,
                storage_client = storage_client,
                gs_bucket_id = bucket_id,
                database_location = database_location,
                database_name = database_name.replace('.db', ''))
            db_creator.create_database(use_db_folder_path)
            db_creator.recreate_tables()



def get_folders_from_prefix_fs(prefix):
    p = Path(prefix)
    subdirectories = [x.__str__() for x in p.iterdir() if (x.is_dir() & (x.__str__().find('.ipynb_checkpoints') < 0))]
    return [ \
          directory.split('/')[-1]
          for directory in subdirectories]


class DeltaLakeDatabaseFsCreator:
    def __init__(self, spark_session, database_location, database_name):
        self.spark_session = spark_session
        self.database_location = database_location
        self.database_name = database_name
        self.db_folder_path = f'{self.database_location}/{self.database_name}.db'

    def create_database(self, use_db_folder_path = True):
        if use_db_folder_path == True:
            query_db_folder_path = f"LOCATION '{self.db_folder_path}'"
        else:
            query_db_folder_path = ""
        # Criação do banco de dados Delta Lake
        create_db_query = f"CREATE DATABASE IF NOT EXISTS {self.database_name} {query_db_folder_path}"
        self.spark_session.sql(create_db_query)

        print(f"Banco de dados {self.database_name} criado.")

    def recreate_tables(self, table_filter = None):
        # Lista arquivos do caminho
        print(f"listando conteúdos do caminho {self.database_location} e database {self.database_name}")
        # Lista os blobs no bucket
        prefix = f'{self.database_location}/{self.database_name}.db/'
        print(f'prefix: {prefix}')

        # Dividir a string usando '/' como delimitador e pegar o último elemento
        table_list = get_folders_from_prefix_fs(prefix)
        print(f'table_list: {table_list}')
        for table_name in table_list:
            final_table_name = f'{self.database_name}.{table_name}'
            if (table_filter != None) and (final_table_name not in table_filter):
                continue

            # Criação da tabela Delta Lake
            delta_location = f"{self.db_folder_path}/{table_name}"
            #print(delta_location)

            # Criação da tabela Delta Lake
            create_table_query = f"CREATE TABLE IF NOT EXISTS {self.database_name}.{table_name} USING delta LOCATION '{delta_location}'"
            self.spark_session.sql(create_table_query)
            print(f"Tabela {table_name} criada")
            print(f"Tabela {table_name} criada com comando {create_table_query}")

        print("Recriação das tabelas concluída.")


def load_entire_catalog_fs(spark_session, lake_prefix, lake_zones = ['bronze', 'silver'], use_db_folder_path = True, database_filter = None, table_filter = None):
    for lake_zone in lake_zones:
        database_list = get_folders_from_prefix_fs(
            prefix = f'{lake_prefix}/{lake_zone}/')

        print(database_list)
        for database_name in database_list:
            if (database_filter != None) and (database_name not in database_filter):
                continue
            database_location = f'{lake_prefix}/{lake_zone}'  # Substitua com o local do seu banco de dados Delta Lake
            db_creator = DeltaLakeDatabaseFsCreator(
                spark_session = spark_session,
                database_location = database_location,
                database_name = database_name.replace('.db', ''))
            db_creator.create_database(use_db_folder_path)
            db_creator.recreate_tables(table_filter)

def load_entire_catalog_fs_v2(
      spark_session, 
      databases_path, 
      use_db_folder_path = True, 
      database_filter = None, 
      table_filter = None):

      database_list = get_folders_from_prefix_fs(
          prefix = databases_path)

      print(database_list)
      for database_name in database_list:
          if (database_filter != None) and (database_name not in database_filter):
              continue
          db_creator = DeltaLakeDatabaseFsCreator(
              spark_session = spark_session,
              database_location = databases_path,
              database_name = database_name.replace('.db', ''))
          db_creator.create_database(use_db_folder_path)
          db_creator.recreate_tables(table_filter)
