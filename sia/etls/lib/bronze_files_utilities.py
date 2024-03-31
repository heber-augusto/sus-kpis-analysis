from google.cloud import storage
from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import google.auth
from google.oauth2 import service_account

import re
import io
import json
import os


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


def autenticar_servico(json_caminho, escopos):
    credenciais = service_account.Credentials.from_service_account_file(json_caminho, scopes=escopos)
    return build('drive', 'v3', credentials=credenciais)


def list_gd_files(
    service,
    team_drive_id,
    file_type,
    max_files = 100,
    checkpoint_file_dict = None
    ):
  """Search file in drive location
  """

  try:
    files = []
    page_token = None
    if checkpoint_file_dict is None:
      date_filter = ''
    else:
      date_filter = f"and modifiedTime >= '{checkpoint_file_dict['last_modified_time']}' and name != '{checkpoint_file_dict['last_modified_file_name']}'"
    while True:
      # pylint: disable=maybe-no-member
      response = (
          service.files()
          .list(
              q=f"name contains '{file_type}' and mimeType != 'application/vnd.google-apps.folder' and name contains '.done' and trashed = false {date_filter}",
              spaces="drive",
              pageToken=page_token,
              fields="nextPageToken, files(id, name, modifiedTime, createdTime, parents)",
              driveId=team_drive_id,
              includeItemsFromAllDrives=True,
              supportsAllDrives=True,
              corpora='drive',
              pageSize=min(1000, max_files if max_files > 0 else 100),
              orderBy='modifiedTime'

          )
          .execute()
      )

      #for file in response.get("files", []):
        # Process change
      #  print(f'Found file: {file.get("name")}, {file.get("id")}')
      # print(f'Found {len(response.get("files", []))} done files')
      files.extend(response.get("files", []))
      page_token = response.get("nextPageToken", None)
      if page_token is None:
        break
      if (max_files > 0) and (len(files) >= max_files):
          break

  except HttpError as error:
    print(f"An error occurred: {error}")
    files = None

  return files[:max_files]



def get_gd_pending_files(auth_json_path, team_drive_id, file_type, checkpoint_file_dict, max_files):
    escopos = ['https://www.googleapis.com/auth/drive.readonly']
    service = autenticar_servico(auth_json_path, escopos)
    
    done_files = list_gd_files(
        service = service,
        team_drive_id = team_drive_id,
        file_type = file_type,
        checkpoint_file_dict = checkpoint_file_dict,
        max_files = max_files
        )
    json_files_list = []
    for done_file in done_files[:max_files]:
        request = (
            service.files()
            .get_media(fileId=done_file['id']
            )
          )
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
          status, done = downloader.next_chunk()
          # print(f"Download {int(status.progress() * 100)}%.")
        file_dict = json.loads(file.getvalue())
        if file_dict['file_group'] == file_type:     
            file_dict['done_file_name'] = done_file['name']
            file_dict['createdTime'] = done_file['createdTime']
            file_dict['modifiedTime'] = done_file['modifiedTime']
            file_dict['parentId'] = done_file['parents'][0]
            json_files_list.append(file_dict)
    return json_files_list
