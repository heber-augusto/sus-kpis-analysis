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

table_filter = None #['sia_bronze.ar','sia_bronze.aq', 'ibge_silver.cadastro_municipios', 'ibge_silver.demografia_municipios' ]

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
