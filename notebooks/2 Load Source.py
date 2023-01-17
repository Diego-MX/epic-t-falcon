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

view_tables = True

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

import src.schema_tools as src_spk
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
    
    up_to_len = max(len_cols(hdrs_df), len_cols(trlr_df))  # Siempre son (47, 56)
    longer_rows = (F.length(F.rtrim(F.col('value'))) > up_to_len)

    delta_exists = DeltaTable.isDeltaTable(spark, delta_loc)
    if delta_exists: 
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

key_1 = 'DAMNA'

damna_delta, damna_schema = predelta_from_key(damna_key, get_schema=True)

_, key_2, tbl_name = delta_keys[key_1]
a_path = f"{write_to}/{key_2}/delta"

(damna_delta.write.format('delta')
    .mode('append')
    .option('mergeSchema', 'true')
    .save(a_path))

display(damna_delta)


# COMMAND ----------

# MAGIC %md
# MAGIC ### ATPTX

# COMMAND ----------

key_1  = 'ATPTX'
_, key_2, tbl_name = delta_keys[key_1]

a_path = f"{write_to}/{key_2}/delta"

a_delta = predelta_from_key(key_1)

(a_delta.write.format('delta')
    .mode('append')
    .option('mergeSchema', 'true')
    .save(a_path))

display(a_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DAMBS1

# COMMAND ----------

key_1  = 'DAMBS1'
_, key_2, tbl_name = delta_keys[key_1]
a_path = f"{write_to}/{dambs1_file}/delta"

dambs1_delta = predelta_from_key(dambs1_key)

(dambs1_delta
    .write.format('delta')
    .mode('append')
    .option('mergeSchema', 'true')
    .save(a_path))

display(dambs1_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DAMBS2

# COMMAND ----------

key_1  = 'DAMBS2'
_, key_2, tbl_name = delta_keys[dambs2_key]

a_path = f"{write_to}/{key_2}/delta"

dambs2_delta, dambs2_schema = predelta_from_key(key_1, get_schema=True)

(dambs2_delta.write.format('delta')
    .mode('append')
    .option('mergeSchema', 'true')
    .save(a_path))

display(dambs2_delta)



# COMMAND ----------

# MAGIC %md
# MAGIC ### DAMBSC

# COMMAND ----------

key_1  = 'DAMBSC'
_, key_2, tbl_name = delta_keys[key_1]

a_path = f"{write_to}/{key_2}/delta"

dambsc_delta = predelta_from_key(key_1)

(dambsc_delta.write.format('delta')
    .mode('append')
    .option('mergeSchema', 'true')
    .save(a_path))

display(dambsc_delta)
