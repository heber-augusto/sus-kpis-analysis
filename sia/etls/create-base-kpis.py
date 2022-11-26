# Instalação de bibliotecas e pacotes para leitura de arquivos

import glob
import os
import pandas as pd
import numpy as np 
import os

## Definições globais
home_dir = os.path.expanduser('~')
print(home_dir)
output_dir = os.getenv('OUTPUT_DIR', os.path.join(home_dir,'output-files'))
print(output_dir)
temp_dir   = os.getenv('TEMP_DIR', os.path.join(home_dir,'temp-files'))
print(temp_dir)

def get_files(state, year, month, file_type, file_group):
    initial_path = os.path.join(r'/content/',local_folder_name,project_folder_name)
    internal_folder = f"""{state}/{year}/{month}/{file_type}/{file_group}"""
    # print(f"{initial_path}/{internal_folder}/*.parquet.gzip")
    return glob.glob(f"{initial_path}/{internal_folder}/*.parquet.gzip")

# SIA PA, AQ e AR: Leitura, filtro e transformação inicial dos arquivos
"""
## Informações sobre filtros pertinentes ao contexto de câncer de mama:

### SIH: realizar filtro através da variável DIAG_PRINC (4 caracteres)
 * Filtro: C500, C501, C502, C503, C504, C505, C506, C508 e C509

### RHC: realizar filtro através da variável LOCTUDET (3 caracteres)
 * Filtro: C50

### SIA – APAC de Quimioterapia e Radioterapia (AQ e AR)
Realizar filtro através da variável AP_CIDPRI (4 caracteres)
 * Filtro: C500, C501, C502, C503, C504, C505, C506, C508 e C509

### SIA – Procedimentos ambulatoriais (PA)

Os arquivos de procedimentos ambulatoriais são um pouco diferentes por um motivo: a pessoa já pode ter o diagnóstico e está realizando um procedimento OU a pessoa está realizando um exame com finalidade diagnóstica (mamografia, ultrassonografia, etc). Então, neste caso, podemos pensar em dois filtros:

Realizar filtro através da variável PA_CIDPRI (4 caracteres)
 * Filtro: C500, C501, C502, C503, C504, C505, C506, C508 e C509

Realizar filtro através da variável do código de procedimento ambulatorial “PA_PROC_ID” (10 caracteres)
 * Filtros:
  * 201010569	BIOPSIA/EXERESE DE NÓDULO DE MAMA
  * 201010585	PUNÇÃO ASPIRATIVA DE MAMA POR AGULHA FINA
  * 201010607	PUNÇÃO DE MAMA POR AGULHA GROSSA
  * 203010035	EXAME DE CITOLOGIA (EXCETO CERVICO-VAGINAL E DE MAMA)
  * 203010043	EXAME CITOPATOLOGICO DE MAMA
  * 203020065	EXAME ANATOMOPATOLOGICO DE MAMA - BIOPSIA
  * 203020073	EXAME ANATOMOPATOLOGICO DE MAMA - PECA CIRURGICA
  * 205020097	ULTRASSONOGRAFIA MAMARIA BILATERAL
  * 208090037	CINTILOGRAFIA DE MAMA (BILATERAL)
  * 204030030	MAMOGRAFIA
  * 204030188	MAMOGRAFIA BILATERAL PARA RASTREAMENTO"""

