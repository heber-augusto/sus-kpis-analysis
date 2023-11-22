from pyspark.sql import SparkSession
from delta import DeltaTable
from pyspark.sql.functions import input_file_name, lit, substring_index
from datetime import datetime
import pytz

class ParquetToDelta:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.unique_columns = []
        self.partition_columns = []
        self.database_name = None
        self.table_name = None
        self.update_datetime_utc = datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")

    def set_unique_columns(self, unique_columns):
        self.unique_columns = unique_columns

    def set_partition_columns(self, partition_columns):
        self.partition_columns = partition_columns

    def set_database_name(self, database_name: str):
        self.database_name = database_name

    def set_table_name(self, table_name: str):
        self.table_name = table_name

    def ingest_initial_parquet_to_delta(self, parquet_path: str):
        # Ler todos os arquivos Parquet no diretório de entrada como um DataFrame
        parquet_df = self.spark.read.option("recursiveFileLookup", "true").parquet(parquet_path)

        # Adicionar colunas adicionais
        parquet_df_with_updates = self.add_columns(parquet_df)

        # Salvar o DataFrame como uma tabela Delta no banco de dados especificado
        self.write_to_delta(parquet_df_with_updates)

    def update_partitions(self, parquet_path: str):
        # Ler o arquivo Parquet como um DataFrame
        parquet_df = self.spark.read.option("recursiveFileLookup", "true").parquet(parquet_path)

        # Adicionar colunas adicionais
        parquet_df_with_updates = self.add_columns(parquet_df)

        # Atualizar a tabela Delta no banco de dados especificado
        self.update_delta_table(parquet_df_with_updates)

    def update_partitions_from_list(self, parquet_files: list):
        # Inicialize um DataFrame vazio para armazenar os dados de todos os arquivos
        combined_df = None

        for parquet_file in parquet_files:
            # Ler o arquivo Parquet como um DataFrame
            parquet_df = self.spark.read.option("recursiveFileLookup", "true").parquet(parquet_file)

            # Adicionar colunas adicionais
            parquet_df_with_updates = self.add_columns(parquet_df)

            if combined_df is None:
                combined_df = parquet_df_with_updates
            else:
                combined_df = combined_df.union(parquet_df_with_updates)

        # Atualizar a tabela Delta no banco de dados especificado
        self.update_delta_table(combined_df)

    def add_columns(self, df):
        # Adicionar coluna contendo o caminho do arquivo Parquet e de criação/modificação do arquivo
        df_with_filepath = (df
            .withColumn("_filepath", input_file_name())
            .selectExpr("*", "_metadata.file_modification_time as file_modification_time")
        )

        # Adicionar coluna contendo o nome do arquivo Parquet
        df_with_filename = df_with_filepath.withColumn("_filename", substring_index(input_file_name(), "/", -1))

        # Adicionar coluna com a data/hora de atualização em UTC
        df_with_update_datetime = df_with_filename.withColumn("_update_datetime", lit(self.update_datetime_utc))

        return df_with_update_datetime

    def write_to_delta(self, delta_df):
        delta_df.write.format("delta").partitionBy(*self.partition_columns).saveAsTable(f"{self.database_name}.{self.table_name}")
        print(f"Tabela Delta '{self.table_name}' criada com sucesso no banco de dados '{self.database_name}'.")

    def update_delta_table(self, delta_df):
        
        table_name = f"{self.database_name}.{self.table_name}"
        delta_df \
          .repartition(*self.partition_columns) \
          .write \
          .format("delta") \
          .option("mergeSchema", "true") \
          .partitionBy(*self.partition_columns) \
          .mode("overwrite") \
          .option("partitionOverwriteMode", "dynamic") \
          .saveAsTable(table_name)

        print(f"Partições da tabela Delta '{self.table_name}' no banco de dados '{self.database_name}' atualizadas com sucesso.")
