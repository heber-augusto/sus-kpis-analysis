import os
import time
from pathlib import Path
import json

from pyspark.sql.functions import input_file_name
from lib.catalog_loader import DeltaLakeDatabaseFsCreator, load_entire_catalog_fs_v2
from lib.table_utilities import vacuum_tables_from_database
from lib.fs_spark_session import create_fs_spark_session
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
# Define variaveis para criar banco de dados cancer_mama, na camada Silver
destination_database_name = 'cancer_mama_silver'
databases_path = os.path.join(datalake_prefix, "monitor-rosa-silver/databases")
db_creator_silver = DeltaLakeDatabaseFsCreator(
    spark_session = spark, 
    database_location = databases_path, 
    database_name = destination_database_name)
db_creator_silver.create_database()

# 1.10 - Cria banco de dados gold
# Define variaveis para criar banco de dados cancer_mama, na camada Gold
destination_database_name_gold = 'cancer_mama'
databases_path = os.path.join(datalake_prefix, "monitor-rosa-gold/databases")
db_creator_gold = DeltaLakeDatabaseFsCreator(
    spark_session = spark, 
    database_location = databases_path, 
    database_name = destination_database_name_gold)
db_creator_gold.create_database()


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
        WHERE AP_CIDPRI IN {cid_filter}
        AND len(AP_CNSPCN) > 0
        """ # AND _filename like '%ARPR18%'
)
cancer_ar_filtered = run_sql_query(sql_query_ar)

cancer_ar_filtered\
      .repartition(1)\
      .write\
      .format("delta")\
      .mode("overwrite")\
      .saveAsTable(f"{destination_database_name}.ar_filtered")

# 3.3 Carrega tabela SIA.AQ filtrando dados de câncer de mama e procedimentos de interesse
sql_query_aq = get_select_all_query(
    table_name='sia_bronze.aq',
    where_clause=f"""
        WHERE AP_CIDPRI IN {cid_filter}
        AND len(AP_CNSPCN) > 0
    """
)

cancer_aq_filtered = run_sql_query(sql_query_aq)

cancer_aq_filtered\
      .repartition(1)\
      .write\
      .format("delta")\
      .mode("overwrite")\
      .saveAsTable(f"{destination_database_name}.aq_filtered")


# IV - Processamento dos Dados dos Pacientes e Procedimentos
# 4.1 - Cria dados consolidados de pacientes e procedimentos (quimio e radioterapia)
# Municipios do DF são todos substituídos por Brasilia pois o cadastro não existe na tabela do IBGE

# Radioterapia
cancer_ar_res = spark.sql(f"""
SELECT
    AP_CMP as data,
    AP_CNSPCN as paciente,
    AR_ESTADI as estadiamento,
    DOUBLE(AP_VL_AP)  as custo,
    INT(AP_OBITO) as obito,
    (case when int(AP_MUNPCN/10000) = 53 then '530010' else AP_MUNPCN end) as municipio
FROM {destination_database_name}.ar_filtered
""")

# quimioterapia
cancer_aq_res = spark.sql(f"""
SELECT
    AP_CMP as data,
    AP_CNSPCN as paciente,
    AQ_ESTADI as estadiamento,
    DOUBLE(AP_VL_AP)  as custo,
    INT(AP_OBITO) as obito,
    (case when int(AP_MUNPCN/10000) = 53 then '530010' else AP_MUNPCN end) as municipio
FROM {destination_database_name}.aq_filtered
""")

# 4.2 - Unifica os dados de radio e quimio consolidados

df_union = cancer_aq_res.union(cancer_ar_res)

df_union.createOrReplaceTempView("cancer_ordered")

df_union\
  .repartition(1)\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .saveAsTable(f"{destination_database_name_gold}.procedimentos")

# 4.3 - Consolidando os dados por paciente
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

res_consolidado\
  .repartition(1)\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .saveAsTable(f"{destination_database_name_gold}.pacientes")


# 4.4 - Procedimentos e Pacientes
procedimentos_e_pacientes = spark.sql(f"""
  SELECT
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
  FROM {destination_database_name_gold}.procedimentos AS c
  RIGHT JOIN {destination_database_name_gold}.pacientes AS p
  ON c.paciente = p.paciente
  where c.data is not null
""")

procedimentos_e_pacientes\
  .repartition(1)\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .saveAsTable(f"{destination_database_name_gold}.procedimentos_e_pacientes")

# V - Agregação por Município e Estado
# 5.1 - Consolida dados por municipio
diagnosticos_por_estadiamento_municipio_df = spark.sql(f"""
    SELECT
        primeiro_estadiamento,
        data_primeiro_estadiamento AS data,
        primeiro_municipio AS municipio,
        COUNT(DISTINCT(paciente)) AS numero_diagnosticos
    FROM {destination_database_name_gold}.pacientes
    WHERE primeiro_estadiamento != ''
    GROUP BY primeiro_estadiamento, data_primeiro_estadiamento, primeiro_municipio
""")

diagnosticos_por_estadiamento_municipio_df.createOrReplaceTempView("diagnosticos_por_estadiamento_municipio")

# 5.2 - Consolida dados mensais por municipio
dados_estad_municipio_mensal_df = spark.sql(f"""
    SELECT
        data,
        primeiro_municipio as municipio,
        primeiro_estadiamento,
        SUM(custo) AS custo_estadiamento,
        COUNT(DISTINCT(paciente)) AS numero_pacientes,
        SUM(DISTINCT(obito)) AS obitos,
        SUM(DISTINCT(indicacao_obito)) AS obito_futuro,
        COUNT(1) AS numero_procedimentos
    FROM
        (SELECT * FROM {destination_database_name_gold}.procedimentos_e_pacientes ORDER BY data)
    GROUP BY data, primeiro_municipio, primeiro_estadiamento
""")
dados_estad_municipio_mensal_df.createOrReplaceTempView("dados_municipios_mensal")

# 5.3 - Consolida dados por municipio mensal
dados_estad_municipio_mensal = spark.sql("""
    SELECT
        mm.*,
        COALESCE(em.numero_diagnosticos, 0) AS numero_diagnosticos
    FROM dados_municipios_mensal mm
    FULL OUTER JOIN diagnosticos_por_estadiamento_municipio em
    ON mm.data = em.data
    AND mm.municipio = em.municipio
    AND mm.primeiro_estadiamento = em.primeiro_estadiamento
""")

dados_estad_municipio_mensal\
    .repartition(1)\
    .write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable(f"{destination_database_name_gold}.dados_municipios_mensal")

# 5.4 - Agregação por estado
dados_estad_mensal = spark.sql(f"""
    SELECT
        estado,
        data,
        primeiro_estadiamento,
        SUM(custo_estadiamento) AS custo_estadiamento,
        SUM(numero_pacientes) AS numero_pacientes,
        COUNT(DISTINCT(municipio)) AS numero_municipios,
        SUM(obitos) AS obitos,
        SUM(obito_futuro) AS obitos_futuros,
        SUM(numero_procedimentos) AS numero_procedimentos,
        SUM(numero_diagnosticos) AS numero_diagnosticos
    FROM (
        SELECT
            cadastro_cidades.nome_uf AS estado,
            mm.*
        FROM {destination_database_name_gold}.dados_municipios_mensal mm
        LEFT JOIN ibge_silver.cadastro_municipios AS cadastro_cidades
        ON int(mm.municipio) = int(cadastro_cidades.id / 10)
        ORDER BY data
    ) AS dados_estado
    GROUP BY estado, data, primeiro_estadiamento
""")

dados_estad_mensal\
    .repartition(1)\
    .write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable(f"{destination_database_name_gold}.dados_estados_mensal")


vacuum_tables_from_database(
        spark_session = spark,
        database_name = destination_database_name,
        retention_hours = 24
    )

vacuum_tables_from_database(
        spark_session = spark,
        database_name = destination_database_name_gold,
        retention_hours = 24
    )

# Realiza a consulta na tabela e salva em um DataFrame do Spark
df_cancer_municipios = spark.sql("SELECT * FROM cancer_mama.dados_municipios_mensal")

# Converte o DataFrame do Spark para um DataFrame do Pandas
df_cancer_municipios_pandas = df_cancer_municipios.toPandas()

# Salva o DataFrame do Pandas em um arquivo CSV
output_csv_file = "dados_municipios_mensal_new.csv"

output_csv_path = os.path.join(datalake_prefix, "monitor-rosa-gold/shared", output_csv_file)

df_cancer_municipios_pandas.to_csv(output_csv_path, index=False)

print(f"Dados exportados para {output_csv_path}")


# Realiza a consulta na tabela e salva em um DataFrame do Spark
df_cancer_estado = spark.sql("SELECT * FROM cancer_mama.dados_estados_mensal")

# Converte o DataFrame do Spark para um DataFrame do Pandas
df_cancer_estado_pandas = df_cancer_estado.toPandas()

# Salva o DataFrame do Pandas em um arquivo CSV
output_csv_file = "dados_estados_mensal_new.csv"
output_csv_path = os.path.join(datalake_prefix, "monitor-rosa-gold/shared", output_csv_file)
df_cancer_estado_pandas.to_csv(output_csv_path, index=False)

print(f"Dados exportados para {output_csv_path}")


