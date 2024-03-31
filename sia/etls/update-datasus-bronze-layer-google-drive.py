import os
import time
from pathlib import Path
import json

from pyspark.sql.functions import input_file_name
from lib.catalog_loader import DeltaLakeDatabaseFsCreator, load_entire_catalog_fs_v2
from lib.table_utilities import vacuum_table
from lib.fs_spark_session import create_fs_spark_session
from lib.bronze_files_utilities import get_gd_pending_files
from lib.delta_table_creators import ParquetToDelta

from google.cloud import storage

json_filename = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')
spark_home = os.getenv('SPARK_HOME','')
file_group = os.getenv('SUS_FILE_GROUP','')
assert(file_group != '')
file_type = os.getenv('SUS_FILE_TYPE','')
assert(file_type != '')
team_drive_id = os.getenv('GOOGLE_RAW_DRIVE_ID')
datalake_prefix = os.getenv('DATALAKE_PREFIX')
max_files = int(os.getenv('MAX_FILES','100'))  # Número máximo de arquivos a serem processados
max_time = max(int(os.getenv('MAX_TIME','1800')), 3600)  # Limite de tempo em segundos (default segurança:  h)


def get_complete_file_path(datalake_prefix, file_group, file_type, file_dict):
    state      = file_dict['state']
    year       = file_dict['year']
    month      = file_dict['month']
    file_name  = str(Path(file_dict['file_name']).with_suffix('.parquet.gzip'))
    return os.path.join(datalake_prefix, f"monitor-rosa-raw/monitor/{state}/{year}/{month}/{file_group}/{file_type}/{file_name}")


last_modified_path = os.path.join(datalake_prefix, f"monitor-rosa-bronze/checkpoints/{file_group}{file_type}.checkpoint")

databases_path = os.path.join(datalake_prefix, "monitor-rosa-bronze/databases")

spark = create_fs_spark_session(
    warehouse_dir=databases_path,
    spark_path=spark_home
)

# carrega catalogo de banco de dados, na zona bronze
database_filter = [f'{file_group}_bronze.db',]
table_filter = [f'{file_group}_bronze.{file_type}',]


load_entire_catalog_fs_v2(
    spark_session = spark,
    databases_path = databases_path,
    database_filter=database_filter,
    table_filter=table_filter    
)


parquet_to_delta = ParquetToDelta(spark)
database_name = f"{file_group.lower()}_bronze"  # Nome do banco de dados baseado no grupo de arquivo (SIA, SIH, etc)
table_name = f"{file_type.lower()}"        # Nome da tabela Delta (tipo de arquivo, dentro do grupo SIA-PA, SIA-QA, etc)

unique_columns = []  # Deixe a lista de colunas únicas vazia
partition_columns = ["_filename", ]  # Defina as colunas de partição

# Configurar o nome do banco de dados
parquet_to_delta.set_database_name(database_name)

# Configurar o nome da tabela
parquet_to_delta.set_table_name(table_name)

db_creator = DeltaLakeDatabaseFsCreator(
    spark_session = spark, 
    database_location = databases_path, 
    database_name = database_name)
db_creator.create_database()

# Realizar a ingestão inicial
parquet_to_delta.set_partition_columns(partition_columns)

try:
    checkpoint_file_dict = json.load(open(last_modified_path))
except:
    checkpoint_file_dict  = None

if __name__ == "__main__":
    print(f'atualizando arquivos do {file_group}-{file_type}')
    start_time = time.time()
    
    
    processed_files = []
    stop_process = False
    while(stop_process != True):
        done_files = get_gd_pending_files(
            auth_json_path = json_filename,
            team_drive_id = team_drive_id,
            file_type = file_type,
            checkpoint_file_dict = checkpoint_file_dict,
            max_files = max_files)
    
        if (len(done_files) == 0):
            stop_process = True
            break
    
        print(f'encontrados {len(done_files)} arquivos a serem atualizados')
        
        ret = """for done_file in done_files:
            if (len(processed_files) >= max_files) or (time.time() - start_time) >= max_time:
                stop_process = True
                break      
            pending_file_path = get_complete_file_path(
                datalake_prefix, 
                file_group, 
                file_type, 
                done_file)
            last_modified_time = done_file['modifiedTime']
            pending_file_name = os.path.basename(pending_file_path)
    
            processed_files.append(pending_file_path)
    
            # print(f'processando arquivo {pending_file_name}')
            try:
                parquet_to_delta.update_partitions(
                    pending_file_path
                    )
                print(f'arquivo {pending_file_name} processado com sucesso')
            except Exception as ex:
                print(f'falha processando arquivo {pending_file_name}: {ex}')
            checkpoint_file_dict = {
               'last_modified_file_name':done_file['done_file_name'],
               'last_modified_time':last_modified_time,           
            }
            fd = open(last_modified_path, 'w+')
            fd.write(json.dumps(checkpoint_file_dict))
            fd.close()"""
    
    # realiza limpeza de tabela delta, considerando 24 horas de retenção
    #vacuum_table(
    #    spark_session = spark,
    #    database_name = database_name,
    #    table_name = table_name,
    #    retention_hours = 24
    #)            
