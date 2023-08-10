import os
import findspark

json_filename = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')
project_id = os.getenv('GCLOUD_PROJECT', '')
spark_home = os.getenv('SPARK_HOME','')
findspark.init(spark_home)

# id do bucket dentro do projeto
bucket_id = 'observatorio-oncologia'

# nome da pasta do projeto
project_folder_name = 'monitor'

dev_lake_name = "lake-rosa-dev"

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.conf import SparkConf
    from delta.pip_utils import configure_spark_with_delta_pip
    
    # Cria a configuração do Spark
    spark_conf = SparkConf()
    spark_conf.set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    spark_conf.set("spark.hadoop.fs.gs.project.id", project_id)
    spark_conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    spark_conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", json_filename)
    spark_conf.set("spark.sql.parquet.compression.codec", "gzip")
    spark_conf.set("spark.sql.warehouse.dir", f"gs://{bucket_id}/{dev_lake_name}/")
    spark_conf.set("spark.delta.logStore.gs.impl","io.delta.storage.GCSLogStore")
    
    #spark_conf.set("spark.jars.packages", ",io.delta:delta-core_2.12-2.4.0")
    spark_conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    spark_conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark_conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    
    appName = 'Sus SQL'
    master = 'local[*]'
    
    # Cria a sessão Spark com a configuração
    spark_builder = SparkSession.builder \
        .appName("Conexao_GCS_Spark") \
        .config(conf=spark_conf)
    
    spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()


    data = spark.read.format("csv")\
        .option("header",True)\
        .option("sep",";")\
        .load(f"gs://{bucket_id}/{project_folder_name}/ibge_data/ibge_cidades.csv")
  
    print(data.show())
