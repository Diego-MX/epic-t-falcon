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

from datetime import datetime as dt, date, timedelta as delta 
import pandas as pd
from pyspark.sql import functions as F, types as T
import re

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

# COMMAND ----------

# Straight-up Write File
write_to = "abfss://silver@stlakehyliaqas.dfs.core.windows.net/ops/regulatory/transformation-layer"

# For Blob connection purposes
dbks_scope = 'eh-core-banking'
cred_keys = {
    'tenant_id'        : 'aad-tenant-id', 
    'subscription_id'  : 'sp-core-events-suscription', 
    'client_id'        : 'sp-core-events-client', 
    'client_secret'    : 'sp-core-events-secret'}

get_secret = lambda a_key: dbutils.secrets.get(dbks_scope, a_key)

# For Blob connection with intermediate local file 
local_tempfile = "/tmp/blob_report.csv"


read_path = "ops/card-management/datasets"
blob_url = "https://stlakehyliaqas.blob.core.windows.net/"
storage_ext = "stlakehyliaqas.dfs.core.windows.net"
read_from  = f"abfss://silver@{storage_ext}/{read_path}"


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

via_pandas = False 

if via_pandas: 
    blob_creds = ClientSecretCredential(**{k: get_secret(v) for (k, v) in cred_keys.items()})
    blob_service = BlobServiceClient(blob_url, blob_creds)
    at_container = blob_service.get_container_client('silver') 
    
def file_exists(a_path): 
    if a_path[:5] == "/dbfs":
        return os.path.exists(path)
    else:
        try:
            dbutils.fs.ls(path)
            return True
        except: 
            return False
#         except Exception as e:
#             if 'java.io.FileNotFoundException' in str(e):
#                 return False
#             else:
#                 raise "Something Else Error"
        
    
def write_to_datalake(spk_df, a_path, method='with_delta', container=None): 
    if method == 'with_delta':
        if file_exists(a_path): 
            dbutils.fs.rm(a_path)
        
        pre_path = re.sub(r'\.csv$', '', a_path)
        spk_df.coalesce(1).write.format('csv').save(pre_path)
        
        path_000 = [ff.path for ff in dbutils.fs.ls(pre_path) 
                if re.match(r'.*000\.csv$', ff.name)][0]
        dbutils.fs.cp(path_000, a_path)
        dbutils.fs.rm(pre_path, recurse=True)

    elif method == 'with_pandas':
        # Esta solución registra el archivo, pero no lo muestra 
        # bajo DBUTILS.FS.LS(A_PATH) curiosamente.  
        if container is None: 
            print("Container instance is needed. ")
            return 
        the_blob = container.get_blob_client(a_path)

        pds_df = spk_df.toPandas()
        str_df = pds_df.to_csv(index=False, encoding='utf-8')
        
        the_blob.upload_blob(str_df)
        
        
cms_tables = {
    'clients' : 'din_clients.slv_ops_cms_damna_stm', 
    'txns'    : 'farore_transactions.slv_ops_cms_atptx_stm', 
    'accounts': 'nayru_accounts.slv_ops_cms_dambs_stm', 
    }

        

# COMMAND ----------

# MAGIC %md
# MAGIC # Reporte R-2422

# COMMAND ----------

print('abfss://silver@stlakehyliaqas.dfs.core.windows.net/ops/card-management/datasets/dambs/delta')
print(accounts_delta)

# COMMAND ----------

run_period = date(2022, 9, 1)

clients_delta  = f"{read_from}/damna/delta"
accounts_delta = f"{read_from}/dambs/delta"

clients_genders = (spark.read.table(cms_tables['clients'])
    .select(F.col('amna_acct').alias('ambs_cust_nbr'), F.col('amna_gender_code_1'))
    .distinct())

accounts = (spark.read.table(cms_tables['accounts'])
    .withColumn('MONTH_REPORT', F.trunc(F.col('file_date'), 'month'))
    .filter(F.col('MONTH_REPORT') == run_period)
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

dt_2422 = f"{write_to}/r2422/r2422_{run_period.strftime('%Y-%m-%d')}.csv" 

write_to_datalake(r_2422, dt_2422, 'with_delta' , None)

display(r_2422)

# COMMAND ----------

# MAGIC %md
# MAGIC # Reporte Sispagos

# COMMAND ----------

run_period = date(2022, 4, 1)

select_cols = ['Trimestre', 'Seccion', 'Moneda', 
    'Tipo_Cuenta', 'Tipo_Persona', 
    'Numero_de_Cuentas', 'Saldo_Promedio']

# AccountNumber	CustomerNumber CardExpirationDate 
# NumberUnblockedCards CurrentBalanceSign CurrentBalance
# FileDate

# sispagos = (spark.read.format('delta').load(cms_tables['accounts'])
sispagos = (spark.read.format('delta').load(accounts_delta)
    .withColumn('Trimestre', F.trunc(F.col('file_date'), 'quarter'))
    .filter(F.col('Trimestre') == run_period)
    .groupBy(F.col('Trimestre'))
    .agg(
        F.sum('ambs_nbr_unblked_cards').alias('numero_de_cuentas'), 
        F.sum('ambs_curr_bal').alias('suma_balances'))
    .withColumn('Saldo_Promedio', F.round(F.col('suma_balances')/F.col('Numero_de_Cuentas'), 2))
    .withColumn('Seccion',      F.lit('2.1'))
    .withColumn('Moneda',       F.lit('MXN'))
    .withColumn('Tipo_Cuenta',  F.lit('1723'))
    .withColumn('Tipo_Persona', F.lit(''))
    .select(*select_cols)
    )

dt_sispagos = f"{write_to}/sispagos/sispagos_{run_period.strftime('%Y-%m-%d')}.csv" 
    
write_to_datalake(sispagos, dt_sispagos, 'with_delta' , None)
display(sispagos)

# COMMAND ----------

# MAGIC %md 
# MAGIC Favor de revisar el archivo alternativo `pd_sispagos = f"{write_to}/sispagos/sispagos_{max_quarter}_df.csv"`.  
# MAGIC Este no aparece bajo `dbutils.fs.ls(...)` pero sí existe cuando se busca por: 
# MAGIC ```
# MAGIC > a_blob = at_container.get_blob_client(pd_sispagos)
# MAGIC > a_blob.exists()
# MAGIC # True
# MAGIC ```

# COMMAND ----------

dbutils.fs.ls(f"{write_to}/r2422")
