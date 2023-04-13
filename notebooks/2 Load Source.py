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

from datetime import date
from delta.tables import DeltaTable as Δ
import pandas as pd
from pyspark.sql import (DataFrame as spk_DF, functions as F)
from pathlib import Path


# COMMAND ----------

from importlib import reload
import config; reload(config)
from src import tools, utilities as src_utils; reload(tools); reload(src_utils)

from config import (ConfigEnviron, 
    ENV, SERVER, RESOURCE_SETUP, 
    DATALAKE_PATHS as paths, 
    DELTA_TABLES as delta_keys)

resources = RESOURCE_SETUP[ENV]
app_environ = ConfigEnviron(ENV, SERVER, spark)
app_environ.sparktransfer_credential()

read_from = f"{paths['abfss'].format('bronze', resources['storage'])}/{paths['prepared']}"  
write_to  = f"{paths['abfss'].format('silver', resources['storage'])}/{paths['datasets']}"  

layouts_dir = "/dbfs/FileStore/transformation-layer/layouts"
dbutils.fs.mkdirs(layouts_dir)

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

a_dir = 'abfss://bronze@stlakehyliaqas.dfs.core.windows.net/ops/regulatory/card-management/transformation-layer/layouts'
src_utils.dirfiles_df(a_dir, spark)

# COMMAND ----------

# Usa las variables: DELTA_KEYS, READ_FROM, WRITE_TO

meta_cols = [('_metadata.file_name', 'file_path'), 
             ('_metadata.file_modification_time', 'file_modified')]

def prepare_sourcer(a_key, force=False): 
    try: 
        b_key = delta_keys[a_key][0]
        blobber_file = f"{paths['from-cms']}/layouts/{b_key}_{{}}_latest.feather" 
        temper_file  = f"{layouts_dir}/{b_key}_{{}}.feather" 
        
        for type_tbl in ['detail', 'header', 'trailer']: 
            app_environ.downoad_storage_blob(temper_file.format(type_tbl), 
                    blobber_file.format(type_tbl), 'bronze')
        
        dtls_file = f"{layouts_dir}/{b_key}_detail.feather"
        hdrs_df = pd.read_feather(f"{layouts_dir}/{b_key}_header.feather")
        trlr_df = pd.read_feather(f"{layouts_dir}/{b_key}_trailer.feather")
        
        dtls_df = (df.read_feather(dtls_file)
            .assign(name = lambda df: 
                    df['name'].str.replace(')', '', regex=False)))
        
        # Centralizar a todos los archivos. 
        up_to_len = max(src_utils.len_cols(hdrs_df), src_utils.len_cols(trlr_df))  
        # Siempre son (47, 56)
        
        prep_dtls     = tools.colsdf_prepare(dtls_df)
        the_selectors = tools.colsdf_2_select(prep_dtls, 'value')  # 1-substring, 2-typecols, 3-sorted
        
        the_selectors['long_rows'] = (F.length(F.rtrim(F.col('value'))) > up_to_len)
        return (1, the_selectors)
    
    except Exception as xcpt: 
        if not force: 
            raise Exception(xcpt)
        return (-1, str(xcpt))
    

def read_delta_basic(a_key, force=False) -> spk_DF: 
    try: 
        a_file = delta_keys[a_key][1]
        Δ_path = f"{write_to}/{a_file}/delta"
        the_dir = f"{read_from}/{a_key}" 

        if Δ.isDeltaTable(spark, Δ_path): 
            max_modified = (spark.read.format('delta')
                .load(Δ_path)
                .select(F.max('file_modified'))
                .collect()[0][0])
        else: 
            max_modified = date(2020, 1, 1)

        pre_delta = (spark.read.format('text')
            .option('recursiveFileLookup', 'true')
            .option('encoding', 'iso-8859-3') 
            .option('header', 'true')
            .load(the_dir, modifiedAfter=max_modified)
            .select('value', *[F.col(a_col[0]).alias(a_col[1]) 
                    for a_col in meta_cols]))
        return (1, pre_delta)
    
    except Exception as xcpt: 
        if not force: 
            raise Exception(xcpt)
        return (-2, str(xcpt))

    
def delta_with_sourcer(df_0: spk_DF, sourcer, 
        get_schema=False, force=False) -> spk_DF: 
    try: 
        meta_keys = ['file_path', 'file_modified', 'file_date']
        
        a_delta = (df_0
            .withColumn('file_date', F.to_date(F.col('file_modified'), 'yyyy-MM-dd'))
            .filter(sourcer['long_rows'])
            .select(*meta_keys, *sourcer['1-substring'])
            .select(*meta_keys, *sourcer['2-typecols' ]))
        
        if get_schema: 
            the_schema = tools.colsdf_2_schema(prep_dtls)
            delta_tbl = (a_delta, the_schema)
        else: 
            delta_tbl = (a_delta)
        return (1, delta_tbl)
    
    except Exception as xcpt: 
        if not force: 
            raise Exception(xcpt) 
        return (-3, str(xcpt))
    
    
def write_fiserv_delta(df_0, a_key): 
    try: 
        _, key_2, tbl_name = delta_keys[a_key]
        a_path = f"{write_to}/{key_2}/delta"
        
        (df_0.write.format('delta')
            .mode('append')
            .option('mergeSchema', 'true')
            .save(a_path))
        
        return (1, a_path)
    except Exception as xcpt: 
        if not force: 
            raise Exception(str(xcpt))
        return (-4, str(xcpt))

    
def reset_fiserv_delta(a_key): 
    _, key_2, tbl_name = delta_keys[a_key]
    a_path = f"{write_to}/{key_2}/delta"

    dbutils.fs.rm(a_path, recurse=True)

    

# COMMAND ----------

# MAGIC %md 
# MAGIC ## CMS deltas

# COMMAND ----------

write_Δ = True

table_keys = ['DAMNA', 'ATPTX', 'DAMBS1', 'DAMBS2', 'DAMBSC']
readies_Δ  = {}

for a_key in table_keys[:]: 
    print(a_key)
    (status_1, sourcer) = prepare_sourcer(a_key)
        
    (status_2, pre_Δ) = read_delta_basic(a_key)
    
    (status_3, ref_Δ) = delta_with_sourcer(pre_Δ, sourcer)
        
    readies_Δ[a_key] = ref_Δ
    write_fiserv_delta(ref_Δ, a_key)
    

# COMMAND ----------

display(ref_Δ)

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

reset_deltas = False
# Return back to False, after resetting. 

if reset_deltas: 
    remove_keys = ['DAMBS1', 'DAMBS2', 'DAMBSC', 'DAMNA', 'ATPTX']  #
    for a_key in remove_keys:
        reset_fiserv_delta(a_key)
    
