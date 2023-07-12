import glob
import os
import pandas as pd
import numpy as np 
import os
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, MONTHLY


DEF_START_DATE = (date.today() + relativedelta(months=-6))
DEF_END_DATE = (date.today() + relativedelta(months=-1))

## Definições globais
home_dir = os.path.expanduser('~')
print(home_dir)

input_dir = os.getenv('INPUT_DIR', os.path.join(home_dir,'input-files'))
print(input_dir)

output_dir = os.getenv('OUTPUT_DIR', os.path.join(home_dir,'output-files'))
print(output_dir)

temp_dir   = os.getenv('TEMP_DIR', os.path.join(home_dir,'temp-files'))
print(temp_dir)

START_DATE = (os.getenv('START_DATE', DEF_START_DATE.strftime('%Y-%m-%d')))
END_DATE = (os.getenv('END_DATE', DEF_END_DATE.strftime('%Y-%m-%d')))
STATES = (os.getenv('STATES', 'SP'))

# validate start date
try:
    strt_dt = datetime.strptime(START_DATE,'%Y-%m-%d')
except ValueError:
    strt_dt = DEF_START_DATE                
    pass
# validate end date
try:
    end_dt = datetime.strptime(END_DATE,'%Y-%m-%d')
except ValueError:
    end_dt = DEF_END_DATE                
    pass


# validate states
if STATES == '':
    states = ['SP',]
else:
    states = STATES.split(',')

list_of_dates = [dt for dt in rrule(MONTHLY, dtstart=strt_dt, until=end_dt)]

print(f'reading files between {strt_dt} and {end_dt} from states {states}')

def get_files(state, year, month, file_type, file_group):
    initial_path = input_dir
    internal_folder = f"""{state}/{year}/{month:02d}/{file_type}/{file_group}"""
    glob_filter = f"{initial_path}/{internal_folder}/*.parquet.gzip"
    print(f'glob_filter: {glob_filter}')
    return glob.glob(glob_filter)

def load_and_update_new_data(current_df_path, new_data, updated_df_path, key_to_sort):
    current_dataframe = pd.read_parquet(
        current_df_path)

    new_df = (
        pd.concat([current_dataframe, new_data])
        .drop_duplicates(keep='last')
        .sort_values(key_to_sort, ascending=False)
        .reset_index(drop=True))    
    
    
    ## Cria arquivo de registros de procedimentos (radioterapia e quimioterapia)
    new_df.to_parquet(
        updated_df_path, 
        compression='gzip')
    
    return new_df


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
def get_file_paths(state, list_of_dates, file_type, file_group):
    """

    """
    file_paths = []
    for dt in list_of_dates:
        year = dt.year
        month = dt.month        
        file_paths.extend(
            get_files(
                state,
                year,
                month,
                file_type,
                file_group)
        )
    return file_paths


if __name__ == "__main__":
  print('test.py')
