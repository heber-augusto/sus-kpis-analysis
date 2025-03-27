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
assert(json_filename != '')
spark_home = os.getenv('SPARK_HOME','')
datalake_prefix = os.getenv('DATALAKE_PREFIX')

databases_path = os.path.join(datalake_prefix, "monitor-rosa-bronze/databases")

spark = create_fs_spark_session(
    warehouse_dir=databases_path,
    spark_path=spark_home
)

zone_names = ['monitor-rosa-bronze','monitor-rosa-silver','monitor-rosa-gold']

zone_paths = [os.path.join(datalake_prefix, f"{zone_name}/databases") for zone_name in zone_names]

# Carrega catalogo de banco de dados, na zona bronze
database_filter = None #['cnes_bronze.db',]

table_filter = ['sia_bronze.ar','sia_bronze.aq', 'ibge_silver.cadastro_municipios', 'ibge_silver.demografia_municipios' ]

for databases_path in zone_paths:
    load_entire_catalog_fs_v2(
        spark_session = spark,
        databases_path = databases_path,
        use_db_folder_path=True,
        database_filter=database_filter,
        table_filter=table_filter
    )

databases = spark.sql(f"SHOW DATABASES;")
databases.show()

for row in databases.collect():
    spark.sql(f"SHOW TABLES FROM {row['namespace']};").show(truncate=False)


# 3.1 - Definindo utils, variáveis e filtros
def get_select_all_query(table_name, where_clause=''):
    return f"""
    SELECT
        *
    FROM {table_name}
    {where_clause}
    """

def run_sql_query(sql_query):
    return spark.sql(sql_query)

# 1.9 - Cria banco de dados silver
# Define variaveis para criar banco de dados cancer_mama, na camada Gold
destination_database_name = 'cancer_mama_silver'
databases_path = os.path.join(datalake_prefix, "monitor-rosa-silver/databases")
db_creator_silver = DeltaLakeDatabaseFsCreator(
    spark_session = spark, 
    database_location = databases_path, 
    database_name = destination_database_name)
db_creator_silver.create_database()


# Filtro pelo CID
cid_filter = ['C500', 'C501', 'C502', 'C503', 'C504', 'C505', 'C506', 'C508', 'C509']

cid_filter = f"""({','.join([f"'{cid_id}'" for cid_id in cid_filter])})"""

proc_id_dict = {
    '0201010569': 'BIOPSIA/EXERESE DE NÓDULO DE MAMA',
    '0201010585': 'PUNÇÃO ASPIRATIVA DE MAMA POR AGULHA FINA',
    '0201010607': 'PUNÇÃO DE MAMA POR AGULHA GROSSA',
    '0203010035': 'EXAME DE CITOLOGIA (EXCETO CERVICO-VAGINAL E DE MAMA)',
    '0203010043': 'EXAME CITOPATOLOGICO DE MAMA',
    '0203020065': 'EXAME ANATOMOPATOLOGICO DE MAMA - BIOPSIA',
    '0203020073': 'EXAME ANATOMOPATOLOGICO DE MAMA - PECA CIRURGICA',
    '0205020097': 'ULTRASSONOGRAFIA MAMARIA BILATERAL',
    '0208090037': 'CINTILOGRAFIA DE MAMA (BILATERAL)',
    '0204030030': 'MAMOGRAFIA',
    '0204030188': 'MAMOGRAFIA BILATERAL PARA RASTREAMENTO'
}

proc_id_filter = f"""({','.join([f"'{proc_id}'" for proc_id in proc_id_dict.keys()])})"""

# 3.2 - Carrega tabela SIA.AR filtrando dados de câncer de mama e procedimentos de interesse
sql_query_ar = get_select_all_query(
    table_name='sia_bronze.ar',
    where_clause=f"""
        WHERE AP_CIDPRI IN {cid_filter} AND _filename like 'ARPR1810.parquet.gzip'
        """
)
cancer_ar_filtered = run_sql_query(sql_query_ar)

cancer_ar_filtered\
      .repartition(1)\
      .write\
      .format("delta")\
      .mode("overwrite")\
      .saveAsTable(f"{destination_database_name}.ar_filtered")

