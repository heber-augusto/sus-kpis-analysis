import os
import time

from pyspark.sql.functions import input_file_name
from lib.catalog_loader import DeltaLakeDatabaseGsCreator, load_entire_catalog
from lib.table_utilities import vacuum_table
from lib.gs_spark_session import create_gs_spark_session
from lib.bronze_files_utilities import get_pending_files_from_bronze
from lib.delta_table_creators import ParquetToDelta

from google.cloud import storage

json_filename = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')
project_id = os.getenv('GCLOUD_PROJECT', '')
spark_home = os.getenv('SPARK_HOME','')
file_group = os.getenv('SUS_FILE_GROUP','')
assert(file_group != '')
file_type = os.getenv('SUS_FILE_TYPE','')
assert(file_type != '')

# id do bucket dentro do projeto
bucket_id = 'observatorio-oncologia'

# nome da pasta do projeto
project_folder_name = 'monitor'

#nome da pasta onde estamos criando o delta lake, dentro do bucket
lake_prefix = "lake-rosa-prd"

# Cria a sessão spark
spark = create_gs_spark_session(
    gcs_project_id=project_id, 
    gcs_json_keyfile=json_filename, 
    warehouse_dir=f"gs://{bucket_id}/{lake_prefix}/", 
    spark_path=spark_home)

if __name__ == "__main__":
    print(f'atualizando arquivos do {file_group}-{file_type}')

    # Cliente do Google Cloud Storage
    storage_client = storage.Client.from_service_account_json(json_filename)
    # carrega catalogo de banco de dados, na zona bronze
    load_entire_catalog(
        spark_session = spark, 
        storage_client=storage_client, 
        bucket_id = bucket_id, 
        lake_prefix = lake_prefix, 
        lake_zones = ['bronze',])

    parquet_to_delta = ParquetToDelta(spark)
    database_name = f"{file_group.lower()}_bronze"  # Nome do banco de dados baseado no grupo de arquivo (SIA, SIH, etc)
    table_name = f"{file_type.lower()}"        # Nome da tabela Delta (tipo de arquivo, dentro do grupo SIA-PA, SIA-QA, etc)
    
    unique_columns = []  # Deixe a lista de colunas únicas vazia
    partition_columns = ["_filename", ]  # Defina as colunas de partição
    
    # Configurar o nome do banco de dados
    parquet_to_delta.set_database_name(database_name)
    
    # Configurar o nome da tabela
    parquet_to_delta.set_table_name(table_name)
    
    # Realizar a ingestão inicial
    parquet_to_delta.set_partition_columns(partition_columns)


    start_time = time.time()
    max_files = int(os.getenv('MAX_FILES','50'))  # Número máximo de arquivos a serem processados
    max_time = max(int(os.getenv('MAX_TIME','1800')), 1800)  # Limite de tempo em segundos (default segurança:  h)
    
    pending_files_path = get_pending_files_from_bronze(
        storage_client = storage_client,
        bucket_name = bucket_id,
        spark_session = spark, 
        file_group = file_group, 
        file_type=file_type)

    print(f'encontrados {len(pending_files_path)} arquivos fora da tabela')
    processed_files = []
    for pending_file_path in pending_files_path:
        if (len(processed_files) >= max_files) or (time.time() - start_time) >= max_time:
            break
    
        processed_files.append(pending_file_path)
    
        print(f'processando arquivo {pending_file_path}')
        try:
            parquet_to_delta.update_partitions(
                pending_file_path
                )
            print(f'arquivo {pending_file_path} processado com sucesso')
        except:
            print(f'falha processando arquivo {pending_file_path}')
            continue

    # realiza limpeza de tabela delta, considerando 24 horas de retenção
    vacuum_table(
        spark_session = spark,
        database_name = database_name,
        table_name = table_name,        
        retention_hours = 24
    )



