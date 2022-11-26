# Instalação de bibliotecas e pacotes para leitura de arquivos

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

## SIA PA: Leitura e união de dados para o período desejado
file_type = 'SIA'

for state in states:
    print(f'processing files from {state}')
    ### Monta lista de arquivos a serem lidos
    file_paths_by_type = {}

    # Arquivos de produção ambulatorial
    file_paths_by_type['PA'] = get_file_paths(
        state,
        list_of_dates,
        file_type,
        'PA'
    )

    # Arquivos de radioterapia
    file_paths_by_type['AR'] = get_file_paths(
        state,
        list_of_dates,
        file_type,
        'AR'
    )

    # Arquivos de quimioteraia
    file_paths_by_type['AQ'] = get_file_paths(
        state,
        list_of_dates,
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
        f'{destination_folder}cancer_pa_copy.parquet.gzip', 
        compression='gzip')

    cancer_dataframe_aq.to_parquet(
        f'{destination_folder}cancer_aq_copy.parquet.gzip', 
        compression='gzip')

    cancer_dataframe_ar.to_parquet(
        f'{destination_folder}cancer_ar_copy.parquet.gzip', 
        compression='gzip')

    # Montagem do dataset cancer, consolidando procedimentos

    # Este dataset consolida registros dois tipos de procedimentos (AQ e AR)

    #Possui as seguintes colunas:

    #- data: valores da coluna **AP_CMP** (AR e AQ);
    #- paciente (cns_encrypted): valores da coluna **AP_CNSPCN** (AR e AQ);
    #- estadiamento: valores dascolunas **AQ_ESTADI** (AQ) e **AR_ESTADI** (AR);
    #- custo: valores da coluna **AP_VL_AP** (AQ ou AR) convertido para double;
    #- municipio: valores da coluna **AP_MUNPCN** (AQ ou AR);
    #- obito: valores da coluna **AP_OBITO** (AQ ou AR), convertido para inteiro;
    #- tipo: tipo de procedimento ('ar' ou 'aq').
    
    ## Transformação dos tipos das colunas 
    #- custo (AP_VL_AP) em double
    #- Indicação de óbito (AP_OBITO) em inteiro.    
    
    cancer_dataframe_aq['custo'] = cancer_dataframe_aq['AP_VL_AP'].astype(np.double)
    cancer_dataframe_ar['custo'] = cancer_dataframe_ar['AP_VL_AP'].astype(np.double)
    cancer_dataframe_aq['obito'] = cancer_dataframe_aq['AP_OBITO'].astype(np.integer)
    cancer_dataframe_ar['obito'] = cancer_dataframe_ar['AP_OBITO'].astype(np.integer)
    
    ## União entre AQ e AR
    columns_aq = ['AP_CMP', 'AP_CNSPCN', 'AQ_ESTADI', 'custo', 'AP_MUNPCN', 'obito']
    columns_ar = ['AP_CMP', 'AP_CNSPCN', 'AR_ESTADI', 'custo', 'AP_MUNPCN', 'obito']

    normalized_columns = ['data','paciente','estadiamento', 'custo', 'municipio', 'obito']

    renamed_aq = cancer_dataframe_aq[columns_aq]
    renamed_aq.columns = normalized_columns
    renamed_aq['tipo'] = 'aq'

    renamed_ar = cancer_dataframe_ar[columns_ar]
    renamed_ar.columns = normalized_columns
    renamed_ar['tipo'] = 'ar'

    cancer_dataframe = pd.concat(
        [
          renamed_aq, 
          renamed_ar
        ], 
        ignore_index=True)
    
    current_cancer_dataframe = pd.read_parquet(
        f'{destination_folder}cancer.parquet.gzip')

    new_cancer_dataframe = (
        pd.concat([current_cancer_dataframe, cancer_dataframe])
        .drop_duplicates(keep='last')
        .sort_values('data', ascending=False)
        .reset_index(drop=True))    
    
    
    ## Cria arquivo de registros de procedimentos (radioterapia e quimioterapia)
    new_cancer_dataframe.to_parquet(
        f'{destination_folder}cancer_copy.parquet.gzip', 
        compression='gzip')
    
    # Montagem do dataset de Exames de Paciente (1 linha por paciente)

    #Colunas:

    # - Chave da paciente (cns_encrypted)
    # - Custo total do tratamento
    # - Estadiamento inicial
    # - Estadiamento final
    # - Indicação de óbito
    # - Localização de moradia

    ## Proposta de solução:

    # - Tipos de arquivos a serem utilizados: AQ e AR
    # - chave do paciente:
    #  - AQ: coluna AP_CNSPCN
    #  - AR: coluna AP_CNSPCN  
    # - custo total do tratamento: será uma estimativa considerando apenas radioterapia e quimioterapia, calculado somando os valores dos procedimentos (presentes em AR e AQ), para cada chave de paciente. Sendo:
    #   - custos em AQ: soma de AP_VL_AP;
    #   - custos em AR: soma de AP_VL_AP.
    # - Estadiamento inicial: calculado utilizando o valor do estadiamento (presentes em AQ:AQ_ESTADI e AR:AR_ESTADI) do registro mais antigo de radioterapia ou quimioterapia, de um determinado paciente;
    # - Estadiamento final: calculado utilizando o valor do estadiamento (presentes em AQ:AQ_ESTADI e AR:AR_ESTADI) do registro mais recente de radioterapia ou quimioterapia, de um determinado paciente;
    # - Localização de moradia: utilizar coluna AP_MUNPCN (presentes em AR e AQ). Talvez criar dois campos, AP_MUNPCN presente no registro mais antigo e AP_MUNPCN presente no registro mais novo;
    # - Indicação de óbito: valor máximo do campo AP_OBITO, presente em AQ e AR (0: sem indicação e 1:com indicação de óbito) O valor 1 indicará óbito.    
    
    ## Cálculo dos valores considerando apenas AQ e AR
    df_paciente = new_cancer_dataframe\
        .sort_values(by=['data'], ascending=True)\
        .groupby(['paciente'])\
        .agg(
            data_primeiro_estadiamento=('data', 'first'),         
            data_ultimo_estadiamento=('data', 'last'),                  
            primeiro_estadiamento=('estadiamento', 'first'), 
            maior_estadiamento=('estadiamento', 'max'),
            ultimo_estadiamento=('estadiamento', 'last'),
            custo_total=('custo', 'sum'),
            primeiro_municipio=('municipio', 'first'), 
            ultimo_municipio=('municipio', 'last'),
            indicacao_obito=('obito', 'max'),
            )\
        .reset_index()

    df_paciente.to_parquet(
        f'{destination_folder}pacientes_copy.parquet.gzip', 
        compression='gzip')
    
    # Montagem do dataset de Procedimentos com dados consolidados Paciente (1 linha por procedimento)

    # Colunas:

    #  - data: data do procedimento;
    #  - paciente: código do paciente; 
    #  - estadiamento: estadiamento anotado no procedimento;
    #  - custo: custo do procedimento;
    #  - municipio: municipio do estadiamento;
    #  - obito: anotação de óbito no procedimento;
    #  - tipo: tipo de procedimento (ar - radioterapia ou aq - quimioterapia)
    #  **dados consolidados de todos os procedimentos do paciente**
    #  - data_primeiro_estadiamento: data do primeiro procedimento do paciente;
    #  - data_ultimo_estadiamento: data do último procedimento do paciente;
    #  - primeiro_estadiamento: primeiro estadiamento identificado no paciente;
    #  - maior_estadiamento: maior estadiamento identificado no paciente;
    #  - ultimo_estadiamento: ultimo estadiamento identificado no paciente;
    #  - custo_total: custo total dos procedimentos do paciente;
    #  - primeiro_municipio: municipio do primeiro procedimento do paciente;
    #  - ultimo_municipio: municipio do último procedimento do paciente;
    #  - indicacao_obito: indica se o paciente tem marcação de obito em algum procedimento.
    procedimentos_e_pacientes = pd.merge(
        new_cancer_dataframe, 
        df_paciente,
        on='paciente',
        how="outer") 
    
    procedimentos_e_pacientes.to_parquet(
        f'{destination_folder}procedimentos_e_pacientes_copy.parquet.gzip', 
        compression='gzip')
    
    # Criação de dataset de consolidando dados de pacientes por estadiamento/municipio/mês

    # Colunas:

    #  - data: mês do procedimento;
    #  - municipio: municipio do procedimento;
    #  - primeiro_estadiamento: estadiamento no primeiro procedimento do paciente (teoricamente, informa diagnostico precoce ou tardio);
    #  - custo_estadiamento: soma dos custos dos procedimentos para um dado mes/municipio/estadiamento;
    #  - numero_pacientes: número de pacientes no mes/municipio/estadiamento;
    #  - obtitos: número de obitos no mes/municipio/estadiamento;
    #  - obito_futuro: pacientes com anotação de obito no futuro;
    #  - numero_procedimentos: número de procedimentos realizados no mes/municipio/estadiamento;     

    dados_estad_municipio_mensal = procedimentos_e_pacientes\
        .sort_values(by=['data'], ascending=True)\
        .groupby(['data', 'municipio','primeiro_estadiamento'])\
        .agg(
            custo_estadiamento=('custo', 'sum'),
            numero_pacientes=('paciente', 'nunique'), 
            obtitos=('obito', 'sum'),
            obito_futuro=('indicacao_obito', 'sum'),
            numero_procedimentos=('data','count')
            )\
        .reset_index()  

    dados_estad_municipio_mensal.to_parquet(
        f'{destination_folder}dados_estad_municipio_mensal_copy.parquet.gzip', 
        compression='gzip')
    
    
    # Criação de dataset de consolidando dados de pacientes por estadiamento/mês

    # Colunas:

    #  - data: mês do procedimento;
    #  - primeiro_estadiamento: estadiamento no primeiro procedimento do paciente (teoricamente, informa diagnostico precoce ou tardio);
    #  - numero_municipios: municipios com procedimentos mes/estadiamento;  
    #  - custo_estadiamento: soma dos custos dos procedimentos para um dado mes/estadiamento;
    #  - numero_pacientes: número de pacientes no mes/estadiamento;
    #  - obtitos: número de obitos no mes/estadiamento;
    #  - obito_futuro: pacientes com anotação de obito no futuro;
    #  - numero_procedimentos: número de procedimentos realizados no mes/estadiamento.     
    
    dados_estad_mensal = dados_estad_municipio_mensal\
        .sort_values(by=['data'], ascending=True)\
        .groupby(['data', 'primeiro_estadiamento'])\
        .agg(
            custo_estadiamento=('custo_estadiamento', 'sum'),
            numero_pacientes=('numero_pacientes', 'sum'),         
            numero_municipios=('municipio', 'nunique'), 
            obtitos=('obtitos', 'sum'),
            obito_futuro=('obito_futuro', 'sum'),
            numero_procedimentos=('numero_procedimentos','sum')
            )\
        .reset_index()    
    
        dados_estad_mensal.to_parquet(
            f'dados_estad_mensal_copy.parquet.gzip', 
            compression='gzip')    
    
