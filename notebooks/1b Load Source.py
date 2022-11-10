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

# MAGIC %pip install -r ../reqs_dbks.txt

# COMMAND ----------

from datetime import datetime as dt, date, timedelta as delta
from delta.tables import DeltaTable
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
from config import (ConfigEnviron, 
    ENV, SERVER, RESOURCE_SETUP, 
    DATALAKE_PATHS as paths, 
    DELTA_TABLES as delta_keys)

resources = RESOURCE_SETUP[ENV]
app_environ = ConfigEnviron(ENV, SERVER, spark)
app_environ.sparktransfer_credential()

read_from = f"{paths['abfss'].format('bronze', resources['storage'])}/{paths['prepared']}"  
write_to  = f"{paths['abfss'].format('silver', resources['storage'])}/{paths['datasets']}"  


def pd_print(a_df: DataFrame, width=180): 
    options = ['display.max_rows', None, 
               'display.max_columns', None, 
               'display.width', width]
    with pd.option_context(*options):
        print(a_df)

        
def len_cols(cols_df: DataFrame) -> int: 
    req_cols = set(['aux_fill', 'Length'])
    
    if not req_cols.issubset(cols_df.columns): 
        raise "COLS_DF is assumed to have attributes 'AUX_FILL', 'LENGTH'."
    
    last_fill = cols_df['aux_fill'].iloc[-1]
    up_to = -1 if last_fill else len(cols_df)
    the_len = cols_df['Length'][:up_to].sum()
    return int(the_len)


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Preparación de código  
# MAGIC 
# MAGIC Definimos llaves claves para leer y guardar archivos.  
# MAGIC 
# MAGIC La función de los _deltas_ considera los siguientes elementos:  
# MAGIC     - Se toman los fólders del _storage container_ en donde se depositaron los archivos descomprimidos.  
# MAGIC     - También utilizamos los archivos de configuración que se guardan en la carpeta local del repositorio.  
# MAGIC     - Se hace el match y se generan los deltas correspondientes.  
# MAGIC     

# COMMAND ----------

experiment = False

if experiment: 
    a_key = 'ATPTX'

    b_key = delta_keys[a_key][0]

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

    pre_cols = ['value', F.input_file_name().alias('file_path'), 
        F.col('_metadata.file_modification_time').alias('file_modified')]
    
    pre_delta = (spark.read.format('text')
        .option('recursiveFileLookup', 'true')
        .option('header', 'true')
        .load(the_dir)
        .select(*pre_cols))
    
    display(pre_delta)

    

# COMMAND ----------

# Usa las variables: DELTA_KEYS, READ_FROM, WRITE_TO


def predelta_from_key(a_key, get_schema=False): 
    b_key  = delta_keys[a_key][0]
    a_file = delta_keys[a_key][1]
    
    the_dir   = f"{read_from}/{a_key}" 
    dtls_file = f"../refs/catalogs/{b_key}_detail.feather"
    hdrs_file = f"../refs/catalogs/{b_key}_header.feather"
    trlr_file = f"../refs/catalogs/{b_key}_trailer.feather"

    dtls_df = pd.read_feather(dtls_file)
    hdrs_df = pd.read_feather(hdrs_file)
    trlr_df = pd.read_feather(trlr_file)
    
    dtls_df['name'] = dtls_df['name'].str.replace(')', '', regex=False)
    
    delta_loc = f"{write_to}/{a_file}/delta"
    
    up_to_len = max(len_cols(hdrs_df), len_cols(trlr_df))
    longer_rows = (F.length(F.rtrim(F.col('value'))) > up_to_len)

    if DeltaTable.isDeltaTable(spark, delta_loc): 
        max_modified = (spark.read.format('delta')
            .load(delta_loc)
            .select(F.max('file_modified'))
            .collect()[0][0])
    else: 
        max_modified = date(2020, 1, 1)

    meta_cols = [('_metadata.file_name', 'file_path'), 
        ('_metadata.file_modification_time', 'file_modified')]
    
    prep_dtls     = src_spk.colsdf_prepare(dtls_df)
    the_selectors = src_spk.colsdf_2_select(prep_dtls, 'value')  # 1-substring, 2-typecols, 3-sorted
    
        
    pre_delta = (spark.read.format('text')
        .option('recursiveFileLookup', 'true')
        .option('header', 'true')
        .load(the_dir, modifiedAfter=max_modified)
        .select('value', *[F.col(a_col[0]).alias(a_col[1]) for a_col in meta_cols]))
    
    a_delta = (pre_delta
        .withColumn('file_date', F.to_date(
                F.regexp_extract(F.col('file_path'), '(20[0-9/]+)', 1), 'yyyy/MM/dd'))
        .filter(longer_rows)
        .select(*[a_col[1] for a_col in meta_cols], *the_selectors['1-substring'])
        .select(*[a_col[1] for a_col in meta_cols], *the_selectors['2-typecols' ]))
    
    if get_schema: 
        the_schema = src_spk.colsdf_2_schema(prep_dtls)
        delta_tbl = (a_delta, the_schema)
    else: 
        delta_tbl = (a_delta)
    
    return delta_tbl


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Debug Section
# MAGIC 
# MAGIC Use this section to copy-paste code lines that are not working as expected:  
# MAGIC Use `if True: ...` to run the code, or change to `if False: ...` to prevent it from running via Jobs.  

# COMMAND ----------

# MAGIC %md 
# MAGIC ## CMS deltas

# COMMAND ----------

# MAGIC %md
# MAGIC ### DAMNA

# COMMAND ----------

damna_key = 'DAMNA'

damna_delta, damna_schema = predelta_from_key(damna_key, get_schema=True)

damna_file = delta_keys[damna_key][1]
damna_tbl  = delta_keys[damna_key][2]

damna_loc = f"{write_to}/{damna_file}/delta"

(damna_delta.write.format('delta')
    .mode('append')
    .option('mergeSchema', 'true')
    .save(damna_loc))

display(damna_delta)


# COMMAND ----------

# MAGIC %md
# MAGIC ### ATPTX

# COMMAND ----------

atptx_key  = 'ATPTX'
atptx_file = delta_keys[atptx_key][1]
atptx_tbl  = delta_keys[atptx_key][2]

atptx_loc = f"{write_to}/{atptx_file}/delta"

atptx_delta = predelta_from_key(atptx_key)

(atptx_delta.write.format('delta')
    .mode('append')
    .option('mergeSchema', 'true')
    .save(atptx_loc))

display(atptx_delta)

# COMMAND ----------

display(atptx_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DAMBS1

# COMMAND ----------

dambs1_key  = 'DAMBS1'
dambs1_file = delta_keys[dambs1_key][1]


dambs1_path = f"{write_to}/{dambs1_file}/delta"

dambs1_delta = predelta_from_key(dambs1_key)

(dambs1_delta
    .write.format('delta')
    .mode('append')
    .option('mergeSchema', 'true')
    .save(dambs1_path))

display(dambs1_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DAMBS2

# COMMAND ----------

dambs2_key  = 'DAMBS2'
dambs2_file = delta_keys[dambs2_key][1]
dambs2_tbl  = delta_keys[dambs2_key][2]

dambs2_path = f"{write_to}/{dambs2_file}/delta"

dambs2_delta, dambs2_schema = predelta_from_key(dambs2_key, get_schema=True)

(dambs2_delta.write.format('delta')
    .mode('append')
    .option('mergeSchema', 'true')
    .save(dambs2_path))

display(dambs2_delta)



# COMMAND ----------

# MAGIC %md
# MAGIC ### DAMBSC

# COMMAND ----------

dambsc_key  = 'DAMBSC'
dambsc_file = delta_keys[dambsc_key][1]
dambsc_tbl  = delta_keys[dambsc_key][2]

dambsc_path= f"{write_to}/{dambsc_file}/delta"

dambsc_delta = predelta_from_key(dambsc_key)

(dambsc_delta.write.format('delta')
    .mode('append')
    .option('mergeSchema', 'true')
    .save(dambsc_path))

display(dambsc_delta)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Reiniciar y limpieza

# COMMAND ----------

delta_keys

# COMMAND ----------

first_time = False
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
