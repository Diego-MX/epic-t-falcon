# Databricks notebook source
# MAGIC %md 
# MAGIC # Introducción
# MAGIC A partir de las tablas provistas por el CMS Fiserv y almacenadas en formato Δ, se generan los siguientes reportes regulatorios:  
# MAGIC `i)`&ensp; **R2422**- (mensualmente) es un resumen de las cuentas activas de acuerdo al género.  
# MAGIC `ii)` **Sispagos**- (trimestral) contiene el número de cuentas y el saldo promedio de acuerdo a ciertas clasificaciones.   
# MAGIC &ensp;&ensp; En un inicio se tomaron las clasificaciones fijas, o sea sólo una;  
# MAGIC &ensp;&ensp; pero a continuación se requirió la expansión de otras clasificaciones.  
# MAGIC 
# MAGIC Este _notebook_ incluye las siguientes partes: 
# MAGIC - Preparación: sirve para definir paquetes y variables.  
# MAGIC - Ejecución de los reportes en el orden mencionado.  Para cada uno de ellos:  
# MAGIC   `i)`&ensp; Se calcula el reporte.  
# MAGIC   `ii)` Se escribe en el _datalake_. 

# COMMAND ----------

# MAGIC %pip install -r ../reqs_dbks.txt

# COMMAND ----------

from importlib import reload
import config
reload(config)

# COMMAND ----------

from datetime import datetime as dt, date, timedelta as delta 
from dateutil.relativedelta import relativedelta as r_delta
import io
from math import floor
import pandas as pd
from pandas.core import frame, series
from pyspark.sql import functions as F, types as T
from pyspark.sql import dataframe as spk_frame
import re
from typing import Union

from azure.identity import ClientSecretCredential
from azure.storage.blob import ContainerClient

from config import (ConfigEnviron, 
    ENV, SERVER, RESOURCE_SETUP, DATALAKE_PATHS as paths)

app_environ = ConfigEnviron(ENV, SERVER, spark)
app_environ.set_credential()
app_environ.sparktransfer_credential()

resources = RESOURCE_SETUP[ENV]
abfss_slv = paths['abfss'].format('silver', resources['storage'])
blob_path = paths['blob'].format(resources['storage'])

datasets = f"{abfss_slv}/{paths['datasets']}"
reports  = f"{abfss_slv}/{paths['reports']}"

accounts_loc = f"{datasets}/dambs/delta"
clients_loc  = f"{datasets}/damna/delta"


#%% Blob Things
slv_container = ContainerClient(blob_path, 'silver', app_environ.credential) 


def get_name_date(a_str): 
    yes_match = re.match(r'(r2422|sispagos)_([\d\-]{10})\.csv')
    get_it = (dt.strptime(yes_match.group(1), '%Y-%m-%d') 
              if yes_match else None)
    return get_it

def floor_date(a_dt: date, period='month'):
    if period == 'month': 
        a_floor = a_dt.replace(day=1)
    if period == 'quarter': 
        f_month = 3*floor((a_dt.month - 1)/3)+1
        a_floor = a_dt.replace(day=1, month=f_month)
    return a_floor

def next_whole_period(a_dt: date, period='month'): 
    # If A_DATE is starting the calendar period, it returns that date, 
    #     if not, returns the beginning of the next period.  
    period_months = {'month': 1, 'quarter': 3}
    the_date = floor_date(a_dt - delta(1), period) + r_delta(months=period_months[period])
    return the_date

def past_whole_period(a_dt: date, period='month', to_return='date'): 
    # If A_DATE is ending the calendar period, it returns that date, 
    #    if not, returns the end of the previous period. 
    the_date = floor_date(a_dt + delta(1), period) - delta(1)
    the_return = (the_date if to_return == 'date' 
            else floor_date(the_date, 'period'))
    return the_return

def file_exists(a_path): 
    if a_path[:5] == "/dbfs":
        return os.path.exists(path)
    else:
        try:
            dbutils.fs.ls(path)
            return True
        except: 
            return False
                
def write_to_datalake(a_df: Union[spk_frame.DataFrame, frame.DataFrame, series.Series], 
        a_path, container=None, overwrite=False): 
    
    if isinstance(a_df, spk_frame.DataFrame):
        if file_exists(a_path): 
            dbutils.fs.rm(a_path)
        
        pre_path = re.sub(r'\.csv$', '', a_path)
        a_df.coalesce(1).write.format('csv').save(pre_path)
        
        path_000 = [ff.path for ff in dbutils.fs.ls(pre_path) 
                if re.match(r'.*000\.csv$', ff.name)][0]
        dbutils.fs.cp(path_000, a_path)
        dbutils.fs.rm(pre_path, recurse=True)

    elif isinstance(a_df, (frame.DataFrame, series.Series)):
        if container is None: 
            raise "Valid Container is required"
            
        the_blob = container.get_blob_client(a_path)
        to_index = {frame.DataFrame: False, 
                    series.Series  : True}
        
        str_df = a_df.to_csv(index=to_index[type(a_df)], encoding='utf-8')
        the_blob.upload_blob(str_df, encoding='utf-8', overwrite=overwrite)
    

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Escritura de archivo
# MAGIC 
# MAGIC Las tablas reporte se calculan directamente en formato Δ, y de acuerdo al flujo funcional 
# MAGIC se escriben como `csv` en una carpeta tipo SFTP dentro del _datalake_.   
# MAGIC 
# MAGIC La tabla Δ tiene funcionalidad para escribirse como `csv`, con la particularidad de que 
# MAGIC agrega muchos archivos de metadatos.  Para remediar esto, convertimos el _dataframe_ de Spark resumen 
# MAGIC a formato Pandas, y lo escribimos como blob.  
# MAGIC 
# MAGIC La siguiente configuración y función se encargan de estos pasos.   

# COMMAND ----------


cms_tables = {
    'clients' : 'din_clients.slv_ops_cms_damna_stm', 
    'txns'    : 'farore_transactions.slv_ops_cms_atptx_stm', 
    'accounts': 'nayru_accounts.slv_ops_cms_dambs_stm', 
}

accounts_range = (spark.read.format('delta')
    .load(accounts_loc)
    .select(F.max('file_date').alias('max_date'), 
            F.min('file_date').alias('min_date'))
    .collect()[0])

print(accounts_range)
  

# COMMAND ----------

# MAGIC %md
# MAGIC # Reporte R-2422

# COMMAND ----------

r2422_dates = filter(None, [get_name_date(f_ish.name) 
    for f_ish in dbutils.fs.ls(f"{reports}/r2422/processed/")])

pre_start = max(r2422_dates) if any(r2422_dates) else accounts_range['min_date']

r2422_start = next_whole_period(pre_start, 'month')    
r2422_end   = past_whole_period(accounts_range['max_date'], 'month')

print(f"From ({r2422_start}) to ({r2422_end})")


# COMMAND ----------

clients_genders = (spark.read.format('delta')
    .load(clients_loc)
    .select(F.col('amna_acct').alias('ambs_cust_nbr'), F.col('amna_gender_code_1'))
    .distinct())

accounts = (spark.read.format('delta')
    .load(accounts_loc)
    .filter(F.col('file_date').between(r2422_start, r2422_end))
    .withColumn('MONTH_REPORT', F.trunc(F.col('file_date'), 'month'))
    .groupby(*['MONTH_REPORT', 'ambs_cust_nbr', 'ambs_acct'])
        .agg(F.count('ambs_acct').alias('n_reps'))
    .withColumn('cuenta_mes', (F.col('n_reps') > 0).cast(T.IntegerType()))
    .join(clients_genders, on='ambs_cust_nbr'))

select_cols = ['MONTH_REPORT'] + [f'CONTRACT_ACTIVE_DEBIT_CARD_{gender}' 
        for gender in ['MALE', 'FEMALE', 'NOT_SPECIFIED']] 
      
r_2422 = (accounts
    .groupby('MONTH_REPORT')
        .pivot('amna_gender_code_1').sum('cuenta_mes')
    .withColumnRenamed('0', 'CONTRACT_ACTIVE_DEBIT_CARD_NOT_SPECIFIED')
    .withColumnRenamed('1', 'CONTRACT_ACTIVE_DEBIT_CARD_MALE')
    .withColumnRenamed('2', 'CONTRACT_ACTIVE_DEBIT_CARD_FEMALE')
    .select(*select_cols))

display(r_2422)

# COMMAND ----------

# MAGIC %md
# MAGIC # Reporte Sispagos

# COMMAND ----------

sis_dates = filter(None, [get_name_date(f_ish.name) 
    for f_ish in dbutils.fs.ls(f"{reports}/sispagos/processed/")])

pre_start = max(sis_dates) if any(sis_dates) else accounts_range['min_date']

sis_start = next_whole_period(pre_start, 'quarter')    
sis_end   = past_whole_period(accounts_range['max_date'], 'quarter')

print(f"From ({sis_start}) to ({sis_end})")


# COMMAND ----------


select_cols = ['Trimestre', 'Seccion', 'Moneda', 
    'Tipo_Cuenta', 'Tipo_Persona', 
    'Numero_de_Cuentas', 'Saldo_Promedio']

# sispagos = (spark.read.format('delta').load(cms_tables['accounts'])
sispagos = (spark.read
    .format('delta').load(accounts_loc)
    .filter(F.col('file_date').between(sis_start, sis_end))
    .withColumn('Trimestre', F.trunc(F.col('file_date'), 'quarter'))
    .groupBy(F.col('Trimestre'))
    .agg(
        F.sum('ambs_nbr_unblked_cards').alias('numero_de_cuentas'), 
        F.sum('ambs_curr_bal').alias('suma_balances'))
    .withColumn('Saldo_Promedio', F.round(F.col('suma_balances')/F.col('Numero_de_Cuentas'), 2))
    .withColumn('Seccion',      F.lit('2.1'))
    .withColumn('Moneda',       F.lit('MXN'))
    .withColumn('Tipo_Cuenta',  F.lit('1723'))
    .withColumn('Tipo_Persona', F.lit(''))
    .select(*select_cols))

display(sispagos)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Escritura de archivos

# COMMAND ----------


pd_r2422 = r_2422.toPandas()

for _, row in pd_r2422.iterrows():
    date_str = row['MONTH_REPORT'].strftime('%Y-%m-%d')
    row_path = f"{reports}/r2422/r2422_{date_str}.csv" 
    row_trck = f"{reports}/r2422/processed/r2422_{date_str}.csv" 
    write_to_datalake(row, row_path, slv_container, overwrite=True)
    write_to_datalake(row, row_trck, slv_container, overwrite=True)

# COMMAND ----------

pd_sispagos = sispagos.toPandas()

for _, row in pd_sispagos.iterrows():
    date_str = row['Trimestre'].strftime('%Y-%m-%d')
    row_path = f"{reports}/sispagos/sispagos_{date_str}.csv" 
    row_trck = f"{reports}/sispagos/processed/sispagos_{date_str}.csv" 
    write_to_datalake(row, row_path, slv_container, overwrite=True)
    write_to_datalake(row, row_trck, slv_container, overwrite=True)
