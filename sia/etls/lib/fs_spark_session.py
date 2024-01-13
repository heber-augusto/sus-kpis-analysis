from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from delta.pip_utils import configure_spark_with_delta_pip
import findspark
import psutil

def get_available_memory_spark():
    mem_info = psutil.virtual_memory()
    available_memory_gb = mem_info.available / (1024 ** 3)  # Convertendo bytes para gigabytes
    return available_memory_gb

def create_fs_spark_session(warehouse_dir, spark_path):
    # Utiliza findspark para inicializar corretamente o ambiente
    findspark.init(spark_path)
    # Obter a quantidade de memória disponível
    available_memory_gb = get_available_memory_spark()
    # Configurar o Spark com base na memória total disponível
    total_memory_gb = available_memory_gb  # Use a memória total disponível
    margin_gb = 1  # Margem para o sistema operacional e outros processos

    # Dividir a memória entre driver, executor e shuffle
    driver_memory_gb = int(0.2 * total_memory_gb)
    executor_memory_gb = int(0.6 * total_memory_gb)
    shuffle_memory_gb = total_memory_gb - driver_memory_gb - executor_memory_gb - margin_gb

    # Cria a configuração do Spark
    spark_conf = SparkConf()
    #spark_conf.set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    #spark_conf.set("spark.hadoop.fs.gs.project.id", gcs_project_id)
    #spark_conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    #spark_conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_json_keyfile)
    spark_conf.set("spark.sql.parquet.compression.codec", "gzip")
    spark_conf.set("spark.sql.warehouse.dir", warehouse_dir)
    #spark_conf.set("spark.delta.logStore.gs.impl","io.delta.storage.GCSLogStore")

    spark_conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    spark_conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark_conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark_conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    spark_conf.set("spark.executor.memory", f"{executor_memory_gb}g")
    spark_conf.set("spark.driver.memory", f"{driver_memory_gb}g")
    spark_conf.set("spark.sql.shuffle.partitions", max(1, int(shuffle_memory_gb * 0.5)))

    appName = 'Sus SQL'
    master = 'local[*]'

    # Cria a sessão Spark com a configuração
    spark_builder = SparkSession.builder \
        .appName("Conexao_GCS_Spark") \
        .config(conf=spark_conf)

    spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()
    return spark
