import os
import findspark


from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from delta.pip_utils import configure_spark_with_delta_pip
from pyspark.sql.functions import input_file_name
from lib.catalog_loader import DeltaLakeDatabaseGsCreator
from lib.table_utilities import vacuum_tables_from_database
from google.cloud import storage

json_filename = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')
project_id = os.getenv('GCLOUD_PROJECT', '')
spark_home = os.getenv('SPARK_HOME','')
findspark.init(spark_home)

# id do bucket dentro do projeto
bucket_id = 'observatorio-oncologia'

# nome da pasta do projeto
project_folder_name = 'monitor'

# nome da pasta do projeto
project_folder_name = 'monitor'

#nome da pasta onde estamos criando o delta lake, dentro do bucket
dev_lake_name = "lake-rosa-dev"

# zona (define a pasta dentro do delta lake)
lake_zone = "silver"

#nome da base de dados (utilizada no padrão delta)
database_name = "cancer_data"


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
    cidades = spark.read.format("csv")\
        .option("header",True)\
        .option("sep",";")\
        .load(f"gs://{bucket_id}/{project_folder_name}/ibge_data/ibge_cidades.csv")
    cidades.createOrReplaceTempView("cadastro_cidades")

    # Cliente do Google Cloud Storage
    storage_client = storage.Client.from_service_account_json(json_filename)
    database_location = f'{dev_lake_name}/{lake_zone}'  # Substitua com o local do seu banco de dados Delta Lake
    
    db_creator = DeltaLakeDatabaseGsCreator(
        spark_session = spark, 
        storage_client = storage_client,
        gs_bucket_id = bucket_id,
        database_location = database_location, 
        database_name = database_name)
    db_creator.create_database()
    db_creator.recreate_tables()

    # cria tabela delta contendo dados de quimioterapia
    parquet_path=f'gs://{bucket_id}/{project_folder_name}/*/*/*/SIA/AQ/*.parquet.gzip'
    cancer_aq_raw = get_cancer_raw_dataframe(parquet_path)
    cancer_aq_raw.createOrReplaceTempView("cancer_aq")

    sql_query = get_select_all_query(
        table_name = 'cancer_aq'  ,
        where_clause = f"WHERE AP_CIDPRI IN {cid_filter}"  ,
    )
    
    cancer_aq_filtered = run_sql_query(sql_query)
    cancer_aq_filtered\
          .repartition(1)\
          .write\
          .format("delta")\
          .mode("overwrite")\
          .saveAsTable(f"{database_name}.aq_filtered")

    # cria tabela delta contendo dados de redioterapia
    parquet_path=f'gs://{bucket_id}/{project_folder_name}/*/*/*/SIA/AR/*.parquet.gzip'
    cancer_ar_raw = get_cancer_raw_dataframe(parquet_path)
    cancer_ar_raw.createOrReplaceTempView("cancer_ar")
    sql_query = get_select_all_query(
        table_name = 'cancer_ar'  ,
        where_clause = f"WHERE AP_CIDPRI IN {cid_filter}"  ,
    )
    
    cancer_ar_filtered = run_sql_query(sql_query)
    cancer_ar_filtered\
      .repartition(1)\
      .write\
      .format("delta")\
      .mode("overwrite")\
      .saveAsTable(f"{database_name}.ar_filtered")
    

    # cria dados consolidados de pacientes e procedimentos (quimio e radioterapia)
    cancer_aq_res = spark.sql(f"""
    SELECT
        AP_CMP as data,
        AP_CNSPCN as paciente,
        AQ_ESTADI as estadiamento,
        DOUBLE(AP_VL_AP)  as custo,
        INT(AP_OBITO) as obito,
        AP_MUNPCN as municipio
    FROM {database_name}.aq_filtered
    """)
    
    cancer_ar_res = spark.sql(f"""
    SELECT
        AP_CMP as data,
        AP_CNSPCN as paciente,
        AR_ESTADI as estadiamento,
        DOUBLE(AP_VL_AP)  as custo,
        INT(AP_OBITO) as obito,
        AP_MUNPCN as municipio
    FROM {database_name}.ar_filtered
    """)
    
    
    df_union = cancer_aq_res.union(cancer_ar_res)
    
    
    df_union.createOrReplaceTempView("cancer_ordered")
    
    
    res_consolidado = spark.sql("""
    SELECT
        paciente,
        FIRST(data) as data_primeiro_estadiamento,
        LAST(data) as data_ultimo_estadiamento,
        COUNT(1) as numero_procedimentos,
        FIRST(estadiamento) as primeiro_estadiamento,
        LAST(estadiamento) as ultimo_estadiamento,
        MAX (estadiamento) as maior_estadiamento,
        MIN (estadiamento) as menor_estadiamento,
        SUM(custo) as custo_total,
        MAX(obito) as indicacao_obito,
        FIRST(municipio) as primeiro_municipio,
        LAST(municipio) as ultimo_municipio
    FROM (SELECT * FROM cancer_ordered ORDER BY paciente, data)
    GROUP BY paciente
    """)

    df_union\
      .repartition(1)\
      .write\
      .format("delta")\
      .mode("overwrite")\
      .saveAsTable(f"{database_name}.procedimentos")

    res_consolidado\
      .repartition(1)\
      .write\
      .format("delta")\
      .mode("overwrite")\
      .saveAsTable(f"{database_name}.pacientes")

    procedimentos_e_pacientes = spark.sql(f"""
      select
         c.*,
         p.data_primeiro_estadiamento,
         p.data_ultimo_estadiamento,
         p.primeiro_estadiamento,
         p.maior_estadiamento,
         p.ultimo_estadiamento,
         p.custo_total,
         p.primeiro_municipio,
         p.ultimo_municipio,
         p.indicacao_obito
      from {database_name}.procedimentos as c
      FULL OUTER JOIN {database_name}.pacientes as p
      ON c.paciente = p.paciente
      """)
    
    procedimentos_e_pacientes\
      .repartition(1)\
      .write\
      .format("delta")\
      .mode("overwrite")\
      .saveAsTable(f"{database_name}.procedimentos_e_pacientes")

    #consolida dados por estado e municipio
    diagnosticos_por_estadiamento_municipio = spark.sql(f"""
      select
          primeiro_estadiamento,
          data_primeiro_estadiamento as data,
          primeiro_municipio as municipio,
          count(distinct (paciente)) as numero_diagnosticos
      from {database_name}.pacientes
      WHERE primeiro_estadiamento != ''
      GROUP BY primeiro_estadiamento, data_primeiro_estadiamento, primeiro_municipio
      """)
    
    diagnosticos_por_estadiamento_municipio.createOrReplaceTempView("diagnosticos_por_estadiamento_municipio")

    dados_estad_municipio_mensal = spark.sql(f"""
      SELECT
        data,
        municipio,
        primeiro_estadiamento,
        SUM(custo) as custo_estadiamento,
        count(distinct (paciente)) as numero_pacientes,
        SUM(distinct (obito)) as obitos,
        SUM(distinct (indicacao_obito)) as obito_futuro,
        count(1) as numero_procedimentos
      FROM
        (select * from {database_name}.procedimentos_e_pacientes order by data)
      GROUP BY  data, municipio, primeiro_estadiamento
              """)
    dados_estad_municipio_mensal.createOrReplaceTempView("dados_municipios_mensal")
    
    dados_estad_municipio_mensal = spark.sql("""
      SELECT
          mm.*,
          COALESCE(em.numero_diagnosticos,0) as numero_diagnosticos
      FROM dados_municipios_mensal mm
      FULL OUTER JOIN diagnosticos_por_estadiamento_municipio em
      ON mm.data = em.data AND
         mm.municipio = em.municipio AND
         mm.primeiro_estadiamento = em.primeiro_estadiamento
    """)
    
    dados_estad_municipio_mensal\
      .repartition(1)\
      .write\
      .format("delta")\
      .mode("overwrite")\
      .saveAsTable(f"{database_name}.dados_municipios_mensal")

    dados_estad_mensal = spark.sql(f"""
      SELECT
        estado,
        data,
        primeiro_estadiamento,
        SUM(custo_estadiamento) as custo_estadiamento,
        SUM(numero_pacientes) as numero_pacientes,
        COUNT(DISTINCT( municipio)) as numero_municipios,
        SUM(obitos) as obitos,
        SUM(obito_futuro) as obitos_futuros,
        SUM(numero_procedimentos) as numero_procedimentos,
        SUM(numero_diagnosticos) as numero_diagnosticos
    
      FROM
      (SELECT
         cadastro_cidades.nome_uf as estado,
         mm.*
      FROM {database_name}.dados_municipios_mensal mm
      LEFT JOIN cadastro_cidades
      ON int(mm.municipio) = int(cadastro_cidades.id/10)
      order by data) as dados_estado
    
      GROUP BY estado, data, primeiro_estadiamento
    """)
    
    dados_estad_mensal\
      .repartition(1)\
      .write\
      .format("delta")\
      .mode("overwrite")\
      .saveAsTable(f"{database_name}.dados_estados_mensal")


    # realiza limpeza de tabelas delta, considerando 24 horas de retenção
    vacuum_tables_from_database(
        database_name = database_name,
        retention_hours = 24
    )

