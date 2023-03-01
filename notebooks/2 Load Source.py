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
# MAGIC   
# MAGIC Los Extracts de Fiserv están en [este link][sharepoint].  
# MAGIC 
# MAGIC [sharepoint]: https://bineomex.sharepoint.com/:x:/r/sites/Ops-DocsValidation/Documentos%20compartidos/2%20CMS-Fiserv/Data%20Extracts.xlsx?d=w10ddf5ab755b4ea28367699379df4cc2&csf=1&web=1&e=e14qAT

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

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from datetime import datetime as dt, date, timedelta as delta
from delta.tables import DeltaTable as Δ
import glob
from itertools import product
import numpy as np
from os import listdir, makedirs
import pandas as pd
from pandas import DataFrame as pd_DF
from pyspark.sql import (DataFrame as spk_DF, 
    functions as F, types as T, Window as W)
from pathlib import Path
import re


# COMMAND ----------

from importlib import reload
import config; reload(config)
from src import tools, utilities as src_utils; reload(tools); reload(src_utils)
from src import sftp_sources as sftp_src; reload(sftp_src)

from config import (ConfigEnviron, 
    ENV, SERVER, RESOURCE_SETUP, 
    DATALAKE_PATHS as paths, 
    DELTA_TABLES as delta_keys)

resources = RESOURCE_SETUP[ENV]
app_environ = ConfigEnviron(ENV, SERVER, spark)
app_environ.sparktransfer_credential()

read_from = f"{paths['abfss'].format('bronze', resources['storage'])}/{paths['prepared']}"  
write_to  = f"{paths['abfss'].format('silver', resources['storage'])}/{paths['datasets']}"  

# Se requiere crear esta carpeta si no existe. 
# makedirs(tmp_layouts)

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

def write_fiserv_delta(df_0, a_path): 
    (df_0.write.format('delta')
        .mode('append')
        .option('mergeSchema', 'true')
        .save(a_path))
    return a_path


def reset_fiserv_delta(b_key): 
    a_path = f"{write_to}/{b_key}/delta"
    dbutils.fs.rm(a_path, True)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## CMS deltas

# COMMAND ----------

from importlib import reload
reload(sftp_src)

# COMMAND ----------

write_Δ   = True
from_blob = False

local_layouts = "/dbfs/FileStore/transformation-layer/layouts"

if from_blob: 
    blob_path = f"{paths['from-cms']}/layouts"
    sftp_src.update_sourcers(app_environ, blob_path, local_layouts)
    
# 'DAMNA' presenta problemas de escritura.
table_keys = ['ATPTX', 'DAMBS1', 'DAMBS2', 'DAMBSC']

readies_Δ  = {}
for a_key in table_keys[:]: 
    b_key  = delta_keys[a_key][1]
    in_dir = f"{read_from}/{a_key}" 
    Δ_path = f"{write_to }/{b_key}/delta"
    
    print(a_key, b_key)
    sourcer = sftp_src.prepare_sourcer(b_key, local_layouts)
    pre_Δ   = sftp_src.read_delta_basic(spark, in_dir)
    ref_Δ   = sftp_src.delta_with_sourcer(pre_Δ, sourcer)
    
    readies_Δ[a_key] = ref_Δ
    write_fiserv_delta(ref_Δ, Δ_path)
    

# COMMAND ----------

enc_detector = False
if enc_detector: 
    import chardet
    
    a_dir = Path("/dbfs/FileStore/transformation-layer/tmp_unzipped")
    a_file = a_dir/"DAMBS101_2022-04-10"

    blob = a_file.read_bytes()
    detection = chardet.detect(blob)
    encoding  = detection["encoding"]
    confidence = detection["confidence"]
    print(f"""
        Encoding: {encoding}
        Confidence: {confidence}
    """)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Adjustments

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Rewrite tables

# COMMAND ----------

reset_deltas = False
# Regrésalo a False, después de borrarlo. 

if reset_deltas: 
    remove_keys = ['DAMBS1', 'DAMBS2', 'DAMBSC', 'DAMNA', 'ATPTX']  #
    for a_key in remove_keys:
        reset_fiserv_delta(a_key)
    
