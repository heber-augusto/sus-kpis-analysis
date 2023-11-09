import os
from pyspark.sql.functions import input_file_name
from lib.catalog_loader import DeltaLakeDatabaseGsCreator
from lib.table_utilities import vacuum_tables_from_database
from lib.gs_spark_session import create_gs_spark_session
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

# nome da pasta do projeto
project_folder_name = 'monitor'

#nome da pasta onde estamos criando o delta lake, dentro do bucket
dev_lake_name = "lake-rosa-dev"

# Cria a sessão spark
spark = create_gs_spark_session(
    gcs_project_id=project_id, 
    gcs_json_keyfile=json_filename, 
    warehouse_dir=f"gs://{bucket_id}/{dev_lake_name}/", 
    spark_path=spark_home)

if __name__ == "__main__":
    print('atualizando arquivos do {file_group}-{file_type}')
  
