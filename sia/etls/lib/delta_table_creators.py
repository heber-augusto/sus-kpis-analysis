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

        # Adicionar uma coluna contendo o caminho do arquivo Parquet
        parquet_df_with_filepath = parquet_df.withColumn("_filepath", input_file_name())

        # Adicionar uma coluna contendo o nome do arquivo Parquet
        parquet_df_with_filename = parquet_df_with_filepath.withColumn("_filename", substring_index(input_file_name(), "/", -1))

        # Adicionar uma coluna com a data/hora de atualização em UTC
        parquet_df_with_update_datetime = parquet_df_with_filename.withColumn("_update_datetime", lit(self.update_datetime_utc))

        # Salvar o DataFrame como uma tabela Delta no banco de dados especificado
        delta_df = parquet_df_with_update_datetime
        delta_df.write.format("delta").partitionBy(*self.partition_columns).saveAsTable(f"{self.database_name}.{self.table_name}")
        print(f"Tabela Delta '{self.table_name}' criada com sucesso no banco de dados '{self.database_name}'.")

    def update_partitions(self, parquet_path: str):
        # Ler o arquivo Parquet como um DataFrame
        parquet_df = self.spark.read.option("recursiveFileLookup", "true").parquet(parquet_path)

        if not self.database_name:
            print("O nome do banco de dados não foi especificado. Use set_database_name para definir o nome do banco de dados.")
            return

        # Adicionar uma coluna contendo o caminho do arquivo Parquet e de criação/modificação do arquivo
        parquet_df_with_filepath = (parquet_df
            .withColumn("_filepath", input_file_name())
            .selectExpr("*", "_metadata.file_modification_time as file_modification_time")
        )        

        # Adicionar uma coluna contendo o nome do arquivo Parquet
        parquet_df_with_filename = parquet_df_with_filepath.withColumn("_filename", substring_index(input_file_name(), "/", -1))

        # Adicionar uma coluna com a data/hora de atualização em UTC
        parquet_df_with_update_datetime = parquet_df_with_filename.withColumn("_update_datetime", lit(self.update_datetime_utc))

        # Construir a condição de merge para colunas de partição
        merge_condition = " AND ".join([f"source.{col} = target.{col}" for col in self.partition_columns])

        if self.unique_columns:
            # Se houver colunas únicas definidas, adicionar à condição de merge
            merge_condition = " AND ".join([f"source.{col} = target.{col}" for col in self.unique_columns]) + " AND " + merge_condition

        # Atualizar a tabela Delta no banco de dados especificado
        delta_table = DeltaTable.forName(self.spark, f"{self.database_name}.{self.table_name}")
        delta_table.alias("target").merge(parquet_df_with_update_datetime.alias("source"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        print(f"Partições da tabela Delta '{self.table_name}' no banco de dados '{self.database_name}' atualizadas com sucesso.")

    def update_partitions_from_list(self, parquet_files: list):
        if not self.database_name:
            print("O nome do banco de dados não foi especificado. Use set_database_name para definir o nome do banco de dados.")
            return

        # Inicialize um DataFrame vazio para armazenar os dados de todos os arquivos
        combined_df = None

        # Construir a condição de merge para colunas de partição
        merge_condition = " AND ".join([f"source.{col} = target.{col}" for col in self.partition_columns])

        if self.unique_columns:
            # Se houver colunas únicas definidas, adicionar à condição de merge
            merge_condition = " AND ".join([f"source.{col} = target.{col}" for col in self.unique_columns]) + " AND " + merge_condition

        for parquet_file in parquet_files:
            # Ler o arquivo Parquet como um DataFrame
            parquet_df = self.spark.read.option("recursiveFileLookup", "true").parquet(parquet_file)

            # Adicionar uma coluna contendo o caminho do arquivo Parquet e de criação/modificação do arquivo
            parquet_df_with_filepath = (parquet_df
                .withColumn("_filepath", input_file_name())
                .selectExpr("*", "_metadata.file_modification_time as file_modification_time")
            )

            # Adicionar uma coluna contendo o nome do arquivo Parquet
            parquet_df_with_filename = parquet_df_with_filepath.withColumn("_filename", substring_index(input_file_name(), "/", -1))

            # Adicionar uma coluna com a data/hora de atualização em UTC
            parquet_df_with_update_datetime = (parquet_df_with_filename
                .withColumn("_update_datetime", lit(self.update_datetime_utc))
            )

            if combined_df is None:
                combined_df = parquet_df_with_update_datetime
            else:
                combined_df = combined_df.union(parquet_df_with_update_datetime)

        # Atualizar a tabela Delta no banco de dados especificado
        delta_table = DeltaTable.forName(self.spark, f"{self.database_name}.{self.table_name}")
        delta_table.alias("target").merge(combined_df.alias("source"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        print(f"Partições da tabela Delta '{self.table_name}' no banco de dados '{self.database_name}' atualizadas com sucesso.")
