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
# MAGIC # Ejecución
# MAGIC La preparación tiene tres partes.  
# MAGIC `i)`&ensp;&ensp; Librerías y utilidades.  
# MAGIC `ii)`&ensp; Variables del proyecto de alto nivel, que se intercambian con el equipo de Infraestructura.  
# MAGIC `iii)` Definición de variables internas de bajo nivel, que se utilizan para el desarrollo posterior. 

# COMMAND ----------

from datetime import datetime as dt, date, timedelta as delta
import glob
from itertools import product
import numpy as np
from os import listdir
import pandas as pd
from pandas.core.frame import DataFrame
import re

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window as W

import src.schemas as src_spk
from config import ENV, RESOURCE_SETUP, DATALAKE_PATHS as paths

resources = RESOURCE_SETUP[ENV]


def pd_print(a_df: pd.DataFrame, width=180): 
    options = ['display.max_rows', None, 
               'display.max_columns', None, 
               'display.width', width]
    with pd.option_context(*options):
        print(a_df)


# COMMAND ----------

# Variables de alto nivel.  (Revisar archivo CONFIG.PY)

read_path  = paths['bronze']

read_from = f"{paths['abfss'].format(stage='bronze', storage=resources['storage'])}/{paths['bronze']}"  
write_to  = f"{paths['abfss'].format(stage='silver', storage=resources['storage'])}/{paths['silver']}"  


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Preparación de código  
# MAGIC 
# MAGIC Definimos llaves claves para leer y guardar archivos.  
# MAGIC 
# MAGIC La función de los _streams_ considera los siguientes elementos:  
# MAGIC     - Se toman los fólders del _storage container_ en donde se depositaron los archivos descomprimidos.  
# MAGIC     - También utilizamos los archivos de configuración que se guardan en la carpeta local del repositorio.  
# MAGIC     - Se hace el match y se generan los streams correspondientes.  
# MAGIC     

# COMMAND ----------

stream_keys = {
    'DAMNA' : ('damna',  'damna' , 'din_clients.slv_ops_cms_damna_stm'), 
    'ATPTX' : ('atpt',   'atpt'  , 'farore_transactions.slv_ops_cms_atptx_stm'), 
    'DAMBS1': ('dambs',  'dambs' , 'nayru_accounts.slv_ops_cms_dambs_stm'), 
    'DAMBS2': ('dambs2', 'dambs2', 'nayru_accounts.slv_ops_cms_dambs2_stm'), 
    'DAMBSC': ('dambsc', 'dambsc', 'nayru_accounts.slv_ops_cms_dambsc_stm')}


# COMMAND ----------

experiment = True
if experiment: 
    a_key = 'ATPTX'

    b_key = stream_keys[a_key][0]

    the_dir   = f"{read_from}/{a_key}" 
    dtls_file = f"../refs/catalogs/{b_key}_detail.feather"
    hdrs_file = f"../refs/catalogs/{b_key}_header.feather"
    trlr_file = f"../refs/catalogs/{b_key}_trailer.feather"

    dtls_df = pd.read_feather(dtls_file)
    hdrs_df = pd.read_feather(hdrs_file)
    trlr_df = pd.read_feather(trlr_file)

    dtls_df['name'] = dtls_df['name'].str.replace(')', '', regex=False)


    up_to_len = max(len_cols(hdrs_df), len_cols(trlr_df))
    longer_rows = (F.length(F.rtrim(F.col('value'))) > up_to_len)

    prep_dtls     = src_spk.colsdf_prepare(dtls_df)
    the_selectors = src_spk.colsdf_2_select(prep_dtls, 'value')  
    # Returns Dict: [1-substring, 2-typecols, 3-sorted]

    the_schema = src_spk.colsdf_2_schema(prep_dtls)

    read_options = {
        'cloudFiles.format'                : 'text',
        'cloudFiles.useIncrementalListing' : 'true', 
        'recursiveFileLookup'              : 'true'}

    pre_stream = (spark.read
        .format('cloudFiles')
        # .schema(the_schema)  doesnt work on value. 
        .options(**read_options)
        .load(the_dir)
        .withColumn('input_file', F.input_file_name()))


# COMMAND ----------

    
# Este query no cacha información de headers. 
def stream_from_key(a_key, get_schema=False): 
    b_key = stream_keys[a_key][0]

    the_dir   = f"{read_from}/{a_key}" 
    dtls_file = f"../refs/catalogs/{b_key}_detail.feather"
    hdrs_file = f"../refs/catalogs/{b_key}_header.feather"
    trlr_file = f"../refs/catalogs/{b_key}_trailer.feather"

    dtls_df = pd.read_feather(dtls_file)
    hdrs_df = pd.read_feather(hdrs_file)
    trlr_df = pd.read_feather(trlr_file)
    
    dtls_df['name'] = dtls_df['name'].str.replace(')', '', regex=False)
    
    
    up_to_len = max(len_cols(hdrs_df), len_cols(trlr_df))
    longer_rows = (F.length(F.rtrim(F.col('value'))) > up_to_len)

    prep_dtls     = src_spk.colsdf_prepare(dtls_df)
    the_selectors = src_spk.colsdf_2_select(prep_dtls, 'value')  # 1-substring, 2-typecols, 3-sorted
    
    the_schema = src_spk.colsdf_2_schema(prep_dtls)
    
    read_options = {
        'cloudFiles.format'                : 'text',
        'cloudFiles.useIncrementalListing' : 'true', 
        'recursiveFileLookup'              : 'true'}

    pre_stream = (spark.readStream
        .format('cloudFiles')
        # .schema(the_schema)  doesnt work on value. 
        .options(**read_options)
        .load(the_dir)
        .withColumn('input_file', F.input_file_name()))
            
    date_col = F.to_date(
        F.regexp_extract(F.col('input_file'), '(20[0-9/]+)', 1), 
        'yyyy/MM/dd')
                      
    a_stream = (pre_stream
        .withColumn('file_date', date_col)
        .filter(longer_rows)
        .select('file_date', *the_selectors['1-substring'])
        .select('file_date', *the_selectors['2-typecols' ]))
    
    if get_schema: 
        the_stream = (a_stream, the_schema)
    else: 
        the_stream = (a_stream)
    
    return the_stream


def len_cols(cols_df: DataFrame) -> int: 
    last_fill = cols_df['aux_fill'].iloc[-1]
    up_to = -1 if last_fill else len(cols_df)
    the_len = cols_df['Length'][:up_to].sum()
    return int(the_len)


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Debug Section
# MAGIC 
# MAGIC Use this section to copy-paste code lines that are not working as expected:  
# MAGIC Use `if True: ...` to run the code, or change to `if False: ...` to prevent it from running via Jobs.  

# COMMAND ----------

# MAGIC %md 
# MAGIC ## CMS streams

# COMMAND ----------

# MAGIC %md
# MAGIC ### DAMNA

# COMMAND ----------

damna_key = 'DAMNA'

# damna_stream, damna_schema = stream_from_key(damna_key, get_schema=True)
damna_stream, damna_schema = stream_from_key(damna_key, get_schema=True)

damna_file = stream_keys[damna_key][1]
damna_tbl  = stream_keys[damna_key][2]

damna_chkpoints = f"{write_to}/{damna_file}/checkpoints"
damna_delta     = f"{write_to}/{damna_file}/delta"

damna_writer = (damna_stream.writeStream
    .format('delta')
    .outputMode('append')
    .options(**{
        'mergeSchema': False, 
        'checkpointLocation': damna_chkpoints}))

damna_writer.start(damna_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ATPTX

# COMMAND ----------

atptx_key = 'ATPTX'
atptx_file = stream_keys[atptx_key][1]
atptx_tbl  = stream_keys[atptx_key][2]

atptx_chkpoints = f"{write_to}/{atptx_file}/checkpoints"
atptx_delta     = f"{write_to}/{atptx_file}/delta"

atptx_stream = stream_from_key(atptx_key)

atptx_writer = (atptx_stream.writeStream.format('delta')
    .outputMode('append')
    .options(**{
        'mergeSchema': False, 
        'checkpointLocation': atptx_chkpoints}))

atptx_writer.start(atptx_delta)


# COMMAND ----------

display(atptx_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DAMBS1

# COMMAND ----------

dambs1_key = 'DAMBS1'
dambs1_file = stream_keys[dambs1_key][1]
dambs1_tbl  = stream_keys[dambs1_key][2]

dambs1_chkpoints = f"{write_to}/{dambs1_file}/checkpoints"
dambs1_delta     = f"{write_to}/{dambs1_file}/delta"

dambs1_stream = stream_from_key(dambs1_key)

dambs1_writer = (dambs1_stream
    .writeStream.format('delta')
    .outputMode('append')
    .options(**{
        'mergeSchema': False, 
        'checkpointLocation': dambs1_chkpoints}))

dambs1_writer.start(dambs1_delta)

# COMMAND ----------

display(dambs1_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DAMBS2

# COMMAND ----------

dambs2_key = 'DAMBS2'
dambs2_file = stream_keys[dambs2_key][1]
dambs2_tbl  = stream_keys[dambs2_key][2]

dambs2_chkpoints = f"{write_to}/{dambs2_file}/checkpoints"
dambs2_delta     = f"{write_to}/{dambs2_file}/delta"

dambs2_stream, dambs2_schema = stream_from_key(dambs2_key, get_schema=True)

dambs2_writer = (dambs2_stream.writeStream.format('delta')
    .outputMode('append')
    .options(**{
        'mergeSchema': False, 
        'checkpointLocation': dambs2_chkpoints}))

dambs2_writer.start(dambs2_delta)



# COMMAND ----------

display(dambs2_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DAMBSC

# COMMAND ----------

dambsc_key = 'DAMBSC'
dambsc_file = stream_keys[dambsc_key][1]
dambsc_tbl  = stream_keys[dambsc_key][2]

dambsc_chkpoints = f"{write_to}/{dambsc_file}/checkpoints"
dambsc_delta     = f"{write_to}/{dambsc_file}/delta"

dambsc_stream = stream_from_key(dambsc_key)

dambsc_writer = (dambsc_stream.writeStream.format('delta')
    .outputMode('append')
    .options(**{
        'mergeSchema': False, 
        'checkpointLocation': dambsc_chkpoints}))

dambsc_writer.start(dambsc_delta)

# COMMAND ----------

display(dambsc_stream)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Reiniciar y limpieza

# COMMAND ----------

stream_keys

# COMMAND ----------

first_time = True
if first_time: 
#     pass
    spark.sql(f"CREATE TABLE IF NOT EXISTS {damna_tbl } USING DELTA LOCATION '{damna_delta }'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {atptx_tbl } USING DELTA LOCATION '{atptx_delta }'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {dambs1_tbl} USING DELTA LOCATION '{dambs1_delta}'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {dambs2_tbl} USING DELTA LOCATION '{dambs2_delta}'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {dambsc_tbl} USING DELTA LOCATION '{dambsc_delta}'")

# COMMAND ----------

erase_table  = True
erase_source = True
if erase_table:
    pass
#     spark.sql("DROP TABLE IF EXISTS din_clients.slv_ops_cms_damna_stm")
#     spark.sql("DROP TABLE IF EXISTS farore_transactions.slv_ops_cms_atptx_stm")
#     spark.sql("DROP TABLE IF EXISTS nayru_accounts.slv_ops_cms_dambs_stm")
#     spark.sql("DROP TABLE IF EXISTS nayru_accounts.slv_ops_cms_dambs2_stm")
#     spark.sql("DROP TABLE IF EXISTS nayru_accounts.slv_ops_cms_dambsc_stm")
    
    
if erase_source: 
    pass
#     dbutils.fs.rm(f"{write_to}/damna", True)
#     dbutils.fs.rm(f"{write_to}/atpt", True)
#     dbutils.fs.rm(f"{write_to}/dambs", True)
#     dbutils.fs.rm(f"{write_to}/dambs2", True)
#     dbutils.fs.rm(f"{write_to}/dambsc", True)
