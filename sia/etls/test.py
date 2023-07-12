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


if __name__ == "__main__":
  print('test.py')
