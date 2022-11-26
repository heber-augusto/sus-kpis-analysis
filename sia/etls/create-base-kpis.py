# Instalação de bibliotecas e pacotes para leitura de arquivos

import glob
import os
import pandas as pd
import numpy as np 
import os
import datetime

## Definições globais
home_dir = os.path.expanduser('~')
print(home_dir)

input_dir = os.getenv('INPUT_DIR', os.path.join(home_dir,'input-files'))
print(input_dir)

output_dir = os.getenv('OUTPUT_DIR', os.path.join(home_dir,'output-files'))
print(output_dir)

temp_dir   = os.getenv('TEMP_DIR', os.path.join(home_dir,'temp-files'))
print(temp_dir)

states   = os.getenv('STATES', 'SP')
if states == '':
    states = 'SP'
states = states.split(',')
print(states)

def get_files(state, year, month, file_type, file_group):
    initial_path = input_dir
    internal_folder = f"""{state}/{year}/{month}/{file_type}/{file_group}"""
    glob_filter = f"{initial_path}/{internal_folder}/*.parquet.gzip"
    # print(f'glob_filter: {glob_filter}')
    return glob.glob(glob_filter)

# SIA PA, AQ e AR: Leitura, filtro e transformação inicial dos arquivos

## Informações sobre filtros pertinentes ao contexto de câncer de mama:

### SIH: realizar filtro através da variável DIAG_PRINC (4 caracteres)
# * Filtro: C500, C501, C502, C503, C504, C505, C506, C508 e C509

### RHC: realizar filtro através da variável LOCTUDET (3 caracteres)
# * Filtro: C50

### SIA – APAC de Quimioterapia e Radioterapia (AQ e AR)
#Realizar filtro através da variável AP_CIDPRI (4 caracteres)
# * Filtro: C500, C501, C502, C503, C504, C505, C506, C508 e C509

### SIA – Procedimentos ambulatoriais (PA)

#Os arquivos de procedimentos ambulatoriais são um pouco diferentes por um motivo: a pessoa já pode ter o diagnóstico e está realizando um procedimento OU a pessoa está realizando um exame com finalidade diagnóstica (mamografia, ultrassonografia, etc). Então, neste caso, podemos pensar em dois filtros:

#Realizar filtro através da variável PA_CIDPRI (4 caracteres)
# * Filtro: C500, C501, C502, C503, C504, C505, C506, C508 e C509

#Realizar filtro através da variável do código de procedimento ambulatorial “PA_PROC_ID” (10 caracteres)
# * Filtros:
#  * 201010569	BIOPSIA/EXERESE DE NÓDULO DE MAMA
#  * 201010585	PUNÇÃO ASPIRATIVA DE MAMA POR AGULHA FINA
#  * 201010607	PUNÇÃO DE MAMA POR AGULHA GROSSA
#  * 203010035	EXAME DE CITOLOGIA (EXCETO CERVICO-VAGINAL E DE MAMA)
#  * 203010043	EXAME CITOPATOLOGICO DE MAMA
#  * 203020065	EXAME ANATOMOPATOLOGICO DE MAMA - BIOPSIA
#  * 203020073	EXAME ANATOMOPATOLOGICO DE MAMA - PECA CIRURGICA
#  * 205020097	ULTRASSONOGRAFIA MAMARIA BILATERAL
#  * 208090037	CINTILOGRAFIA DE MAMA (BILATERAL)
#  * 204030030	MAMOGRAFIA
#  * 204030188	MAMOGRAFIA BILATERAL PARA RASTREAMENTO

## Variáveis de filtro
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
proc_id_filter = list(proc_id_dict.keys())


## Funções de filtro para arquivo SIA PA, AQ e AR

def filter_pa_content(df):
    """

    """
    return df[df.PA_CIDPRI.isin(cid_filter) & \
              df.PA_PROC_ID.isin(proc_id_filter)]

def filter_ar_content(df):
    """

    """
    return df[df.AP_CIDPRI.isin(cid_filter)]

filter_aq_content = filter_ar_content

## Função para unir diversos arquivos em um único datraframe
def create_cancer_dataframe(file_paths, filter_function=filter_pa_content):
    """

    """
    filtered_contents = [
      filter_function(pd.read_parquet(file_path))
      for file_path in file_paths
      ]

    return pd.concat(
        filtered_contents, 
        ignore_index=True)

## Função para retornar lista de arquivos (caminho completo)
def get_file_paths(state, years, months, file_type, file_group):
    """

    """
    file_paths = []
    for year in years:
        for month in months:
            file_paths.extend(
                get_files(
                    state,
                    year,
                    month,
                    file_type,
                    file_group)
            )
    return file_paths

## SIA PA: Leitura e união de dados para o período desejado
### Anos e meses a serem lidos e processados
start_year = 2008
end_year = datetime.date.today().year
years  = [f'{year + 2008:02d}' for year in range(end_year - start_year + 1)]
months = [f'{month + 1:02d}' for month in range(12)]
file_type = 'SIA'

for state in states:
    print('processing files from SP')
    ### Monta lista de arquivos a serem lidos
    file_paths_by_type = {}

    # Arquivos de produção ambulatorial
    file_paths_by_type['PA'] = get_file_paths(
        state,
        years,
        months,
        file_type,
        'PA'
    )

    # Arquivos de radioterapia
    file_paths_by_type['AR'] = get_file_paths(
        state,
        years,
        months,
        file_type,
        'AR'
    )

    # Arquivos de quimioteraia
    file_paths_by_type['AQ'] = get_file_paths(
        state,
        years,
        months,
        file_type,
        'AQ'
    )

    print(f"""Identificados {len(file_paths_by_type['PA'])} arquivos de produção ambulatorial para {state}""")
    print(f"""Identificados {len(file_paths_by_type['AR'])} arquivos de radioterapia para {state}""")
    print(f"""Identificados {len(file_paths_by_type['AQ'])} arquivos de quimioterapia para {state}""")


    # Montagem do dataset cancer AR e AQ

    # Estes dois datasets (cancer_aq e cancer AR) não realizam nenhum tipo de filtro ou transformação 
    # nos dados originais, apenas consolidam os registros de todos os anos/meses, coletados do FTP do SUS, 
    # em um único dataframe.

    cancer_dataframe_pa = create_cancer_dataframe(file_paths_by_type['PA'], filter_function=filter_pa_content)
    cancer_dataframe_aq = create_cancer_dataframe(file_paths_by_type['AQ'], filter_function=filter_ar_content)
    cancer_dataframe_ar = create_cancer_dataframe(file_paths_by_type['AR'], filter_function=filter_aq_content)

    destination_folder = f"""{output_dir}/{state}/consolidado/"""
    
    cancer_dataframe_pa.to_parquet(
        f'{destination_folder}cancer_pa.parquet.gzip', 
        compression='gzip')

    cancer_dataframe_aq.to_parquet(
        f'{destination_folder}cancer_aq.parquet.gzip', 
        compression='gzip')

    cancer_dataframe_ar.to_parquet(
        f'{destination_folder}cancer_ar.parquet.gzip', 
        compression='gzip')


