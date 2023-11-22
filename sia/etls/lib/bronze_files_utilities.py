from google.cloud import storage
from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp
import re

def get_parquet_files_from_gs_storage(storage_client, bucket_name, spark_session, prefix_pattern):
    # Acesse o bucket desejado
    bucket = storage_client.get_bucket(bucket_name)

    # Use expressão regular para criar um padrão para o prefixo
    prefix_regex = re.compile(prefix_pattern)

    # Liste todos os objetos no bucket
    blobs = bucket.list_blobs()

    # Filtrar apenas os arquivos Parquet que correspondem ao padrão do prefixo
    arquivos_parquet_com_data = []

    for blob in blobs:
        if blob.name.endswith('.parquet.gzip') and prefix_regex.match(blob.name):
            arquivo_info = {
                'caminho': f"""gs://{bucket_name}/{blob.name}""",
                'data_criacao': blob.time_created
            }
            arquivos_parquet_com_data.append(arquivo_info)

    # Crie um DataFrame a partir da lista de dicionários
    df = spark_session.createDataFrame(arquivos_parquet_com_data)

    # Ajuste o formato da coluna 'data_criacao' para corresponder ao formato desejado
    df = df.withColumn("file_modification_time", to_timestamp(col("data_criacao"), "yyyy-MM-dd HH:mm:ss.SSS"))

    # Renomeie a coluna 'caminho' para '_filepath'
    df = df.withColumnRenamed("caminho", "_filepath")

    df = df.select('_filepath','file_modification_time')

    return df

def get_pending_files_from_bronze(storage_client, bucket_name, spark_session, file_group, file_type):
    prefix_pattern = f'monitor/[^/]+/[^/]+/[^/]+/{file_group.upper()}/{file_type.upper()}/[^/]+\.parquet\.gzip'
    parquet_df = get_parquet_files_from_gs_storage(
        storage_client=storage_client, 
        bucket_name=bucket_name, 
        spark_session=spark_session, 
        prefix_pattern=prefix_pattern)

    parquet_df_with_filepath = (parquet_df
        .selectExpr("_filepath", "file_modification_time as current_file_modification_time")
    ).distinct()
    
    total_file_system = parquet_df_with_filepath.count()
    
    processed_files_df = spark_session.sql(f"""
      SELECT distinct _filepath, file_modification_time
      FROM {file_group.lower()}_bronze.{file_type.lower()}""")
    
    pending_files = parquet_df_with_filepath.join(
        processed_files_df,
        parquet_df_with_filepath._filepath == processed_files_df._filepath,
        how = 'left'
    ).where("""
    (file_modification_time is NULL) OR 
    (current_file_modification_time != file_modification_time)
    """)
    pending_result = [row['_filepath'] for row in pending_files.collect()]

    pending_count = len(pending_result)

    print(f"{file_group}-{file_type}: {(100* (total_file_system - pending_count) / total_file_system):.2f}% processados de um total de {total_file_system} arquivos ")

    return pending_result
