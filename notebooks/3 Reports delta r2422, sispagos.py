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

# MAGIC %pip install -q -r ../reqs_dbks.txt

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import subprocess
import yaml

spark = SparkSession.builder.getOrCreate()
dbks_secrets = DBUtils(spark).secrets

with open("../user_databricks.yml", 'r') as _f: 
    u_dbks = yaml.safe_load(_f)

epicpy_load = {
    'url'   : 'github.com/Bineo2/data-python-tools.git', 
    'branch': 'dev-diego', 
    'token' :  dbks_secrets.get(u_dbks['dbks_scope'], u_dbks['dbks_token'])}

url_call = "git+https://{token}@{url}@{branch}".format(**epicpy_load)
subprocess.check_call(['pip', 'install', url_call])

# COMMAND ----------

from azure.storage.blob import ContainerClient
from datetime import datetime as dt
from pyspark.dbutils import DBUtils
from pyspark.sql import functions as F, types as T, SparkSession
import re

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------

from importlib import reload
import epic_py; reload(epic_py)

from epic_py.delta import EpicDF
from epic_py.tools import next_whole_period, past_whole_period

from src.tools import write_datalake
from config import (ConfigEnviron, 
    t_agent, t_resources,                    
    ENV, SERVER, RESOURCE_SETUP, DATALAKE_PATHS as paths)

# COMMAND ----------

app_environ = ConfigEnviron(ENV, SERVER, spark)
app_environ.set_credential()
app_environ.sparktransfer_credential()


resources = RESOURCE_SETUP[ENV]
abfss_slv = t_resources.get_resource_url('abfss', 'storage', container='silver')
blob_path = t_resources.get_resource_url('blob', 'storage')

datasets = f"{abfss_slv}/{paths['datasets']}"
reports  = f"{abfss_slv}/{paths['reports']}"

accounts_loc = f"{datasets}/dambs/delta"
clients_loc  = f"{datasets}/damna/delta"


#%% Blob Things
slv_container = ContainerClient(blob_path, 'silver', app_environ.credential) 




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
    'accounts': 'nayru_accounts.slv_ops_cms_dambs_stm', }

accounts_range = (EpicDF(spark, accounts_loc)
    .select(F.max('file_date').alias('max_date'), 
            F.min('file_date').alias('min_date'))
    .collect()[0])

print(accounts_loc)
print(accounts_range)  

# COMMAND ----------

# MAGIC %md
# MAGIC # Reporte R-2422

# COMMAND ----------

def get_name_date(a_str): 
    to_match = re.match(r'(r2422|sispagos)_([\d\-]{10})\.csv')
    get_it = (dt.strptime(to_match.group(1), '%Y-%m-%d') 
              if yes_match else None)
    return get_it


r2422_dates = filter(None, [get_name_date(f_ish.name) 
        for f_ish in dbutils.fs.ls(f"{reports}/r2422/processed/")])

pre_start = max(r2422_dates) if any(r2422_dates) else accounts_range['min_date']

r2422_start = next_whole_period(pre_start, 'month')    
r2422_end   = past_whole_period(accounts_range['max_date'], 'month')

print(f"From ({r2422_start}) to ({r2422_end})")


# COMMAND ----------

clients_genders = (EpicDF(spark, clients_loc)
    .select(F.col('amna_acct').alias('ambs_cust_nbr'), F.col('amna_gender_code_1'))
    .distinct())

accounts = (EpicDF(spark, accounts_loc)
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
        .pivot('amna_gender_code_1')
        .agg(F.sum('cuenta_mes'))
    # .withColumnRenamed('0', 'CONTRACT_ACTIVE_DEBIT_CARD_NOT_SPECIFIED')
    # .withColumnRenamed('1', 'CONTRACT_ACTIVE_DEBIT_CARD_MALE')
    # .withColumnRenamed('2', 'CONTRACT_ACTIVE_DEBIT_CARD_FEMALE')
    # .select(*select_cols)
    )

r_2422.display()

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

group_cols = {
    'agg' : {
        'numero_de_cuentas': F.sum('ambs_nbr_unblked_cards'), 
        'suma_balances'    : F.sum('ambs_curr_bal')}, 
    'mutate': {
        'Saldo_Promedio': F.round(F.col('suma_balances')/F.col('numero_de_cuentas'), 2), 
        'Seccion'       : F.lit('2.1'), 
        'Moneda'        : F.lit('MXN'), 
        'Tipo_Cuenta'   : F.lit('1723'), 
        'Tipo_Persona'  : F.lit('')}}

sispagos = (EpicDF(spark, accounts_loc)
    .filter(F.col('file_date').between(sis_start, sis_end))
    .withColumn('Trimestre', F.trunc(F.col('file_date'), 'quarter'))
    .groupBy(F.col('Trimestre'))
    .agg_plus(group_cols['agg'])
    .with_column_plus(group_cols['mutate'])
    .select(*select_cols))

sispagos.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Escritura de archivos

# COMMAND ----------

r_2422.display()

# COMMAND ----------

pd_r2422 = r_2422.toPandas()

for _, row in pd_r2422.iterrows():
    date_str = row['MONTH_REPORT'].strftime('%Y-%m-%d')
    row_path = f"{reports}/r2422/r2422_{date_str}.csv" 
    print(f"escribiendo: {row_path}")
    write_datalake(row, row_path, slv_container, overwrite=True)
    

# COMMAND ----------

pd_sispagos = sispagos.toPandas()

for _, row in pd_sispagos.iterrows():
    date_str = row['Trimestre'].strftime('%Y-%m-%d')
    row_path = f"{reports}/sispagos/sispagos_{date_str}.csv" 
    print(f"escribiendo: {row_path}")
    write_datalake(row, row_path, slv_container, overwrite=True)
    

# COMMAND ----------

[x.name for x in dbutils.fs.ls(reports + '/r2422/processed')]

