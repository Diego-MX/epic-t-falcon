# Databricks notebook source
# MAGIC %md
# MAGIC # Introducción  
# MAGIC Identificamos tres capas de transformación:  
# MAGIC * `ATPTX` transacciones
# MAGIC * `DAMBS` cuentas
# MAGIC * `DAMNA` clientes
# MAGIC 
# MAGIC Cada capa corresponde a un tipo de archivo que se deposita por el CMS Fiserv, en una misma carpeta tipo SFTP.  
# MAGIC El flujo de las capas es el siguiente:  
# MAGIC 
# MAGIC 0. La versión inicial se corre manualmente, y lee todos los archivos de la carpeta del _datalake_. 
# MAGIC Para cada archivo, se realiza lo siguiente.  
# MAGIC 1. Identificar qué tipo de archivo es.  
# MAGIC 2. Descargar y descomprimir a archivos temporales locales.  
# MAGIC    Paso necesario -aunque desafortunado-, porque Spark no sabe procesar ZIPs directos del _datalake_. 
# MAGIC 3. Parsear de acuerdo a archivo de configuración. 
# MAGIC 4. Anexar a la tabla Δ correspondiente. 
# MAGIC 
# MAGIC Cabe mencionar un par de comentarios sobre la estructura del archivo: 
# MAGIC - **Introducción**  
# MAGIC   Descripción de este _notebook_.  
# MAGIC - **Preparación**  
# MAGIC   ... pues preparan la ejecución.  
# MAGIC - **Ejecución**  
# MAGIC   Donde ocurre la acción.  
# MAGIC - **Iniciación**  
# MAGIC   Es para definir las tablas en el _metastore_ una vez que fueron guardadas en el _datalake_.   
# MAGIC   Cabe mencionar que no afecta la etapa de Ejecución.  

# COMMAND ----------

# MAGIC %md
# MAGIC # Preparación
# MAGIC La preparación tiene tres partes.  
# MAGIC `i)`&ensp;&ensp; Librerías y utilidades.  
# MAGIC `ii)`&ensp; Variables del proyecto de alto nivel, que se intercambian con el equipo de Infraestructura.  
# MAGIC `iii)` Definición de variables internas de bajo nivel, que se utilizan para el desarrollo posterior. 

# COMMAND ----------

from importlib import reload
import src.schemas
from src import schemas
reload(src.schemas)
reload(schemas)
from src.schemas import colsdf_2_pyspark


# COMMAND ----------

from datetime import datetime as dt, date, timedelta as delta
from os import listdir
import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
import re

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window as W

from src.schemas import colsdf_2_pyspark

def pd_print(a_df: pd.DataFrame, width=180): 
    options = ['display.max_rows', None, 
               'display.max_columns', None, 
               'display.width', width]
    with pd.option_context(*options):
        print(a_df)


# COMMAND ----------

# Variables de alto nivel.  (Revisar archivo CONFIG.PY)

dbks_scope = 'eh-core-banking'

blob_url = 'https://stlakehyliaqas.blob.core.windows.net/'

cred_keys = {
    'tenant_id'        : 'aad-tenant-id', 
    'subscription_id'  : 'sp-core-events-suscription', 
    'client_id'        : 'sp-core-events-client', 
    'client_secret'    : 'sp-core-events-secret'}

read_path  = 'ops/regulatory/card-management/transformation-layer/unzipped-ready'
write_path = 'ops/card-management/datasets'

storage_ext = 'stlakehyliaqas.dfs.core.windows.net'

read_from = f"abfss://bronze@{storage_ext}/{read_path}"
write_to  = f"abfss://silver@{storage_ext}/{write_path}"



# COMMAND ----------

# MAGIC %md 
# MAGIC ## Preparación de código

# COMMAND ----------

the_keys = {
    'DAMNA' : 'damna', 
    'ATPTX' : 'atpt', 
    'DAMBS1': 'dambs', 
    'DAMBS2': 'dambs', 
    'DAMBSC': 'dambsc'}

# COMMAND ----------

# MAGIC %md 
# MAGIC ## ATPT transacciones
# MAGIC 
# MAGIC Depende del `atpt_detail.feather` y otras rutas de archivo explícitas. 

# COMMAND ----------

def len_cols(cols_df: DataFrame) -> int: 
    last_fill = cols_df['aux_fill'].iloc[-1]
    up_to = -1 if last_fill else len(cols_df)
    the_len = cols_df['Length'][:up_to].sum()
    return int(the_len)
    

# COMMAND ----------

pd_print(dtls_df)

# COMMAND ----------

with_header = False

the_dir   = f"{read_from}/ATPTX" 
dtls_file = f"../refs/catalogs/atpt_detail.feather"
hdrs_file = f"../refs/catalogs/atpt_header.feather"
trlr_file = f"../refs/catalogs/atpt_trailer.feather"

where_chkpoints = f"{write_to}/atpt/checkpoints"
where_delta = f"{write_to}/atpt/delta"

dtls_df = pd.read_feather(dtls_file)
hdrs_df = pd.read_feather(hdrs_file)
trlr_df = pd.read_feather(trlr_file)

up_to_len = max(len_cols(hdrs_df), len_cols(trlr_df))
longer_rows = (F.rtrim(F.col('value')) > up_to_len)

the_selectors = colsdf_2_pyspark(cols_df, 'value')  # 1-substring, 2-typecols, 3-sorted


read_options = {
    'cloudFiles.format'                : 'text',
    'cloudFiles.useIncrementalListing' : 'true', 
    'recursiveFileLookup'              : 'true'}

write_opts = {
    'mergeSchema': True, 
    'checkpointLocation': where_chkpoints}

pre_stream = (spark.readStream
    .format('cloudFiles')
    .options(**read_options)
    .load(the_dir))

the_stream = (pre_stream
    .filter(longer_rows)              
    .select(the_selectors['1-substring'])
    .select(the_selectors['2-typecols' ])
    .select(the_selectors['3-sorted'])
    )

# pos_stream = (the_stream.writeStream.format('delta')
#     .outputMode('append')
#     .options(**write_opts))

# pos_stream.start(where_delta)

display(the_stream)

# COMMAND ----------

display(the_stream)


# COMMAND ----------

if with_header: 
    atpt_at = ""
    atpt_table = (pre_atpt
        .withColumn('is_header',  F.length(F.trim(F.col('value'))) <= int(header_len))
        .withColumn('ref_header', F.when( F.col('is_header'), F.trim(F.col('value'))).otherwise(None))
        .withColumn('details',    F.when(~F.col('is_header'), F.col('value')).otherwise(None))
        .select('is_header', *pre_headers,  *pre_columns)
        .select('is_header', *atpt_headers, *atpt_str, *atpt_sngs, *atpt_dates, *atpt_ints)
        .select('is_header', *atpt_headers, *cols_sort))
else:
    atpt_table = (pre_atpt
        .withColumn('is_header', F.length(F.trim(F.col('value'))) <= int(header_len))
        .filter(~F.col('is_header'))
        .withColumn('details',   F.when(~F.col('is_header'), F.col('value')).otherwise(None))
        .select(*pre_columns)
        .select(*atpt_str, *atpt_sngs, *atpt_dates, *atpt_ints)
        .select(*cols_sort))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC ## DAMBS cuentas

# COMMAND ----------

# MAGIC %md
# MAGIC ## DAMNA clientes
