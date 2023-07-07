# Databricks notebook source
# MAGIC %md
# MAGIC # Introducción  
# MAGIC Identificamos tres capas de transformación:  
# MAGIC * `ATPTX` transacciones
# MAGIC * `DAMBS` cuentas
# MAGIC * `DAMNA` clientes
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

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------

from delta.tables import DeltaTable as Δ
from pyspark.sql import (functions as F, Window as W)
from pathlib import Path

# COMMAND ----------

from importlib import reload
import config; reload(config)
from src import tools, utilities as utils; reload(tools); reload(utils)
from src import sftp_sources as sftp; reload(sftp)

from config import (ConfigEnviron, 
    ENV, SERVER, RESOURCE_SETUP, 
    DATALAKE_PATHS as paths)


resources = RESOURCE_SETUP[ENV]
app_environ = ConfigEnviron(ENV, SERVER, spark)
app_environ.sparktransfer_credential()

local_layouts = "/dbfs/FileStore/transformation-layer/layouts"

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

# MAGIC %md 
# MAGIC ## CMS Δ's (Ejecución)

# COMMAND ----------

write_Δ = True
new_layouts = False

if new_layouts: 
    blob_path = f"{paths['from-cms']}/layouts"
    sftp.update_sourcers(app_environ, blob_path, local_layouts)
    
# 'DAMNA' presenta problemas de escritura.
table_keys = {
    'atpt'  : ['atpt_mt_ref_nbr', 'rank_ref'],  
    'damna' : ['amna_acct', 'rank_acct'], 
    'dambs' : ['ambs_acct', 'rank_acct'], 
    'dambs2': ['ambs_acct', 'rank_acct'], 
    'dambsc': ['ambs_acct', 'rank_acct']}  

readies_Δ  = {}
for a_key, on_cols in table_keys.items(): 
    in_dir = f"{read_from}/{a_key}" 
    Δ_path = f"{write_to }/{a_key}/delta"
    
    print(a_key)
    sourcer = sftp.prepare_sourcer(a_key, local_layouts)
    pre_Δ   = sftp.read_delta_basic(spark, in_dir, Δ_path)  # Si es Δ, utilizas checkpoints.  
    ref_Δ   = sftp.delta_with_sourcer(pre_Δ, sourcer)
    
    if isinstance(on_cols, str): 
        on_cols = [on_cols]
    else: 
        w_rk = (W.partitionBy('file_date', *on_cols[:-1])
            .orderBy('file_modified'))
        
        ref_Δ = (ref_Δ
            .withColumn(on_cols[-1], F.row_number().over(w_rk)))

    readies_Δ[a_key] = ref_Δ
    if Δ.isDeltaTable(spark, Δ_path): 
        utils.upsert_delta(spark, ref_Δ, Δ_path, 
            'simple', ['file_date', *on_cols])
    else: 
        continue
    

# COMMAND ----------

[x.name for x in dbutils.fs.ls('abfss://silver@stlakehyliaqas.dfs.core.windows.net/ops/card-management/(datasets|)')]

# COMMAND ----------

# MAGIC %md 
# MAGIC # Adjustments

# COMMAND ----------

# MAGIC %md 
# MAGIC ## (Re)write tables

# COMMAND ----------

debugging = False

# 'damna', 'atpt', 'dambs', 'dambs2', 'dambsc'
if debugging: 
    for a_key in ['damna', 'dambs', 'dambs2', 'dambsc']:
        print(a_key)
        (readies_Δ[a_key].write
            .partitionBy('file_date')
            .save(f"{write_to}/{a_key}/delta"))
        
# if debugging: 
#     for key in ['dambs', 'dambs2', 'dambsc']:  # 'damna', 'atpt'
#         dbutils.fs.rm(f"{write_to}/{key}", True)


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Encoding

# COMMAND ----------

debugging = False
if debugging: 
    import chardet
    
    a_dir = Path("/dbfs/FileStore/transformation-layer/tmp_unzipped")
    a_file = a_dir/"DAMNA001_2022-12-18"

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
# MAGIC ¿Qué creo que debemos hacer? 
# MAGIC - Darle formato de `data-vault`, pasar de `silver/.../datasets` a `silver/.../vault`.  
# MAGIC   - Implica hacer el análisis de _business keys_, _low changing dims_, etc.   
# MAGIC - ¿Qué validaciones se pueden hacer?,  
# MAGIC   x.ej: en cuanto a _business keys_ que no se repitan.  
# MAGIC - ¿Cómo es la relación con las contrapartes del _core_ SAP?  
# MAGIC   DAMBS vs current-account  
# MAGIC   DAMNA vs person-set  
# MAGIC   ATPTX vs transaction-set  
# MAGIC - Encapsular los actores principales como objetos, y sus procedimientos como métodos.  
# MAGIC   
