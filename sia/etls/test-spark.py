import os
import findspark


from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from delta.pip_utils import configure_spark_with_delta_pip
from pyspark.sql.functions import input_file_name

json_filename = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')
project_id = os.getenv('GCLOUD_PROJECT', '')
spark_home = os.getenv('SPARK_HOME','')
findspark.init(spark_home)

# id do bucket dentro do projeto
bucket_id = 'observatorio-oncologia'

# nome da pasta do projeto
project_folder_name = 'monitor'

dev_lake_name = "lake-rosa-dev"


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


def get_cancer_raw_dataframe(parquet_paths):
    df=spark.read.parquet(parquet_path)
    spark.udf.register("filename", lambda x: x.rsplit('/', 1)[-1].rsplit('.')[0])
    df = df.selectExpr('*', 'filename(input_file_name()) as file_name')
    return df

def get_select_all_query (table_name, where_clause = ''):
    return f"""
    SELECT
        *
    FROM {table_name}
    {where_clause}
    """

def run_sql_query(sql_query):
    return spark.sql(sql_query)

# filtro pelo cid
cid_filter = ['C500', 'C501', 'C502', 'C503', 'C504', 'C505', 'C506', 'C508', 'C509']

# dicionario de procedimentos
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

proc_id_filter = [f"'{proc_id}'" for proc_id in list(proc_id_dict.keys())]
proc_id_filter = f"""({','.join(proc_id_filter)})"""
print(proc_id_filter)
cid_filter = [f"'{cid_id}'" for cid_id in cid_filter]
cid_filter = f"""({','.join(cid_filter)})"""
print(cid_filter)


if __name__ == "__main__":
    data = spark.read.format("csv")\
        .option("header",True)\
        .option("sep",";")\
        .load(f"gs://{bucket_id}/{project_folder_name}/ibge_data/ibge_cidades.csv")
  
    print(data.show())

    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

    # cria tabela delta contendo dados de quimioterapia
    parquet_path=f'gs://{bucket_id}/{project_folder_name}/*/*/*/SIA/AQ/*.parquet.gzip'
    cancer_aq_raw = get_cancer_raw_dataframe(parquet_path)
    cancer_aq_raw.createOrReplaceTempView("cancer_aq")

    sql_query = get_select_all_query(
        table_name = 'cancer_aq'  ,
        where_clause = f"WHERE AP_CIDPRI IN {cid_filter}"  ,
    )
    
    cancer_aq_filtered = run_sql_query(sql_query)
    cancer_aq_filtered.write.format("delta").mode("overwrite").saveAsTable("bronze.cancer_aq_filtered")

    # cria tabela delta contendo dados de redioterapia
    parquet_path=f'gs://{bucket_id}/{project_folder_name}/*/*/*/SIA/AR/*.parquet.gzip'
    cancer_ar_raw = get_cancer_raw_dataframe(parquet_path)
    cancer_ar_raw.createOrReplaceTempView("cancer_ar")
    sql_query = get_select_all_query(
        table_name = 'cancer_ar'  ,
        where_clause = f"WHERE AP_CIDPRI IN {cid_filter}"  ,
    )
    
    cancer_ar_filtered = run_sql_query(sql_query)
    cancer_ar_filtered.write.format("delta").mode("overwrite").saveAsTable("bronze.cancer_ar_filtered")


    df = spark.sql("select count(*), AP_UFMUN from bronze.cancer_ar_filtered group by AP_UFMUN")
    print(df.show())

    df = spark.sql("select count(*), AP_UFMUN from bronze.cancer_aq_filtered group by AP_UFMUN")
    print(df.show())
