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
# MAGIC    Paso necesario porque Spark no sabe procesar ZIPs directos del _datalake_. 
# MAGIC 3. Parsear de acuerdo a archivo de configuración. 
# MAGIC 4. Anexar a la tabla Δ correspondiente. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Preparación
# MAGIC La preparación tiene tres partes.  
# MAGIC `i)`&ensp;&ensp; Librerías y utilidades.  
# MAGIC `ii)`&ensp; Variables del proyecto de alto nivel, que se intercambian con el equipo de Infraestructura.  
# MAGIC `iii)` Definición de variables internas de bajo nivel, que se utilizan para el desarrollo posterior. 

# COMMAND ----------

with open("../src/install_nb_reqs.py") as nb_reqs: 
    exec(nb_reqs.read())

# COMMAND ----------

from pathlib import Path
import re

from azure.storage.blob import ContainerClient
import numpy as np
import pandas as pd
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from zipfile import ZipFile

# COMMAND ----------

from importlib import reload
import epic_py; reload(epic_py)
import config;  reload(config)

from epic_py.tools import dirfiles_df

# cambiar a epic-Py mode. 🙏
from config import (ConfigEnviron, 
    ENV, SERVER, RESOURCE_SETUP, DATALAKE_PATHS as paths)

app_env = ConfigEnviron(ENV, SERVER, spark)
app_env.set_credential()
app_env.sparktransfer_credential()

resources = RESOURCE_SETUP[ENV]
pre_write = paths['prepared']

abfss_brz = paths['abfss'].format('bronze', resources['storage'])
blob_path = paths['blob' ].format(resources['storage'])

# Las carpetas claves.  Put attention. 
abfss_read  = f"{abfss_brz}/{paths['from-cms']}"
abfss_write = f"{abfss_brz}/{paths['prepared']}"

# COMMAND ----------

#%% About the files in question: 

#%% Local temporary download. 
to_download = "/dbfs/FileStore/transformation-layer/tmp_download.zip"
to_unzip    = "/dbfs/FileStore/transformation-layer/tmp_unzipped"
# Hint for files: EN DBUTILS no usar /DBFS.  En [Python] with(file_path) sí usar DBFS. 

#%% Working with the datalake. 

blob_container = ContainerClient(blob_path, 'bronze', app_env.credential) 

read_df = dirfiles_df(abfss_read, spark)
read_df

# COMMAND ----------

def path_2_blob(abs_path): 
    return re.sub(r".*windows\.net/", "", abs_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Ejecución

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Checar nombres

# COMMAND ----------

# Blobs migrados previamente. 

blobs_paths = [b_name
    for b_name in blob_container.list_blob_names(name_starts_with=paths['prepared'])
    if  b_name.endswith('.txt')]

blobs_df = (pd.DataFrame(data={
        'abs_path': pd.Series(blobs_paths, dtype='str')})
    .assign(rel_path = lambda df: 
        df['abs_path'].str.replace(f"{paths['prepared']}/", '')))

print(f"Prepared: {paths['prepared']}")
blobs_df

# COMMAND ----------

reg_labels = {
    'ZATPTX01' : ('atpt',   'ATPXT'),
    'DAMNA001' : ('damna',  'DAMNA'), 
    'DAMBS101' : ('dambs',  'DAMBS1'),  
    'DAMBS201' : ('dambs2', 'DAMBS2'), 
    'DAMBSC01' : ('dambsc', 'DAMBSC')}

file_regex = r"UAT_(?P<tag_1>[A-Z]*)_(?P<tag_2>[A-Z0-9]*?)_(?P<tag_date>20[0-9\-]{8})\.ZIP"
labels = read_df['name'].str.extract(file_regex)
def brz_name(a_row): 
    dir_label = reg_labels[a_row['tag_2']][0]
    return f"{dir_label}/{a_row['tag_date']}.txt"


raw_files = (pd.concat([read_df, labels], axis=1)   # Ubicado en BRZ por error de Diseño. 
    .query('size > 0')
    .rename(
        columns = {'name': 'raw_name', 'path': 'raw_path'})
    .assign(
        raw_name = lambda df: df['tag_2'].str.cat(df['tag_date'], sep='_'), 
        brz_name = lambda df: df.apply(brz_name, axis=1),
        in_blobs = lambda df: df['brz_name'].isin(blobs_df['rel_path'])))

watch_tags = np.any(raw_files['tag_date'].isnull())
raw_files.query("tag_1 == 'CUENTAS'").sort_values('tag_date', ascending=False)

# COMMAND ----------

def download_blob(abs_path, at_local): 
    rel_path = path_2_blob(abs_path)
    the_blob = blob_container.get_blob_client(rel_path)
    with open(at_local, 'wb') as _f: 
        _f.write(the_blob.download_blob().readall())
    return

def extract_file(f_name, f_src, f_tgt): 
    with ZipFile(f"{f_src}", 'r') as _z: 
        z_ls = _z.namelist()
        if f_name not in z_ls: 
            raise Exception(f"Filename {f_name} no in Zip.")
        if len(z_ls) != 1: 
            print(f"\nZip for {f_name} has > 1 candidates:")
            for ff in z_ls: 
                print(f"\t{ff}")
        _z.extract(f_name, f_tgt)
        dbutils.fs.rm(f"file:{f_src}")
    return 
    

def upload_blob(f_name, blob_path): 
    blob_to = blob_container.get_blob_client(blob_path)
    with open(f_name, 'rb') as _f: 
        blob_to.upload_blob(_f)
    dbutils.fs.rm(f"file:{f_name}")
    return

# COMMAND ----------

for ii, f_row in raw_files.iterrows():
    if f_row['in_blobs']: 
        continue
    
    print(ii, f_row['raw_name'])
    download_blob(f_row['raw_path'], to_download)
    extract_file( f_row['raw_name'], to_download, to_unzip)
    upload_blob(f"{to_unzip}/{f_row['raw_name']}", 
                f"{paths['prepared']}/{f_row['brz_name']}")
    
    

# COMMAND ----------

# MAGIC %md
# MAGIC # Ajustes

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Encoding

# COMMAND ----------

debugging = False

if debugging:
    from chardet import detect
    a_path = Path("/dbfs/FileStore/transformation-layer/tmp_unzipped/DAMNA001_2022-12-18" )
    
    # We must read as binary (bytes) because we don't yet know encoding
    blob = a_path.read_bytes()
    a_detection = detect(blob)
    an_enc  = a_detection["encoding"]
    a_confd = a_detection["confidence"]
    print(f"""{an_enc} encoding at {a_confd}""")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## (Re)iniciar carpetas

# COMMAND ----------

debugging = False
if debugging: 
    dbutils.fs.rm(f"{abfss_write}/atptx", True)
