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

# COMMAND ----------

# MAGIC %md
# MAGIC # Preparación
# MAGIC La preparación tiene tres partes.  
# MAGIC `i)`&ensp;&ensp; Librerías y utilidades.  
# MAGIC `ii)`&ensp; Variables del proyecto de alto nivel, que se intercambian con el equipo de Infraestructura.  
# MAGIC `iii)` Definición de variables internas de bajo nivel, que se utilizan para el desarrollo posterior. 

# COMMAND ----------

# MAGIC %pip install -r ../reqs_dbks.txt

# COMMAND ----------

from azure.storage.blob import ContainerClient
from pathlib import Path
import re
from zipfile import ZipFile


# COMMAND ----------

from src.utilities import dirfiles_df
from config import (ConfigEnviron, 
    ENV, SERVER, RESOURCE_SETUP, DATALAKE_PATHS as paths)

app_env = ConfigEnviron(ENV, SERVER, spark)
app_env.set_credential()
app_env.sparktransfer_credential()

resources = RESOURCE_SETUP[ENV]
pre_write = paths['prepared']

abfss_brz = paths['abfss'].format('bronze', resources['storage'])
blob_path = paths['blob' ].format(resources['storage'])

abfss_read  = f"{abfss_brz}/{paths['from-cms']}"
abfss_write = f"{abfss_brz}/{paths['prepared']}"


def dbks_path(a_path: Path): 
    a_str = str(a_path)
    if re.match(r'^(abfss|dbfs|file):', a_str): 
        b_str = re.sub(':/', '://', a_str)
    else: 
        b_str = a_str 
    return b_str
    

# COMMAND ----------

#%% About the files in question: 

reg_labels = {
    'UAT_CLIENTES_DAMNA001': 'DAMNA', 
    'UAT_CUENTAS_DAMBS101' : 'DAMBS1',  
    'UAT_CUENTAS_DAMBS201' : 'DAMBS2', 
    'UAT_CUENTAS_DAMBSC01' : 'DAMBSC', 
    'UAT_TRXS_ZATPTX01'    : 'ATPTX'}

#%% Local temporary download. 
to_download = "/dbfs/FileStore/transformation-layer/tmp_download.zip"
to_unzip    = "/dbfs/FileStore/transformation-layer/tmp_unzipped"
# Hint for files: EN DBUTILS no usar /DBFS.  En [Python] with(file_path) sí usar DBFS. 

#%% Working with the datalake. 

blob_container = ContainerClient(blob_path, 'bronze', app_env.credential) 
dirfiles_df(abfss_read, spark)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Ejecución
# MAGIC 
# MAGIC Se comparan los fólders de lectura y escritura (`.../unzipped-ready/{ATPTX|DAMNA|DAMBS[12C]}`) para identificar archivos faltantes.  

# COMMAND ----------

# Writeable candidates. 
filenames_0 = [filish.name 
    for filish in dbutils.fs.ls(abfss_read)]

# Detect
file_regex = r"([A-Z_0-9]*?)_([0-9\-]*)\.ZIP"
filenames_1 = [(a_name,) + re.match(file_regex, a_name).groups() 
    for a_name in filenames_0 if re.match(file_regex, a_name)]

filenames_2 = {label[0]: f"{reg_labels[label[1]]}/{re.sub('-', '/', label[2])}.txt" 
    for label in filenames_1 if label[1] in reg_labels}


labels_0 = set(fl_lb[1] for fl_lb in filenames_1)

if set(reg_labels) != labels_0: 
    raise "File Labels are not as expected."

    
blobnames_0 = [re.sub(paths['prepared']+'/', '', b_name)
    for b_name in blob_container.list_blob_names(name_starts_with=paths['prepared'])
    if  b_name.endswith('.txt')]

blobnames_1 = {z_name: t_name
    for z_name, t_name in filenames_2.items()
    if  t_name not in blobnames_0}

# COMMAND ----------

# Rutas de archivo temporales: variables globales
# TO_DOWNLOAD, TO_UNZIP.  

def _download_blob(a_file): 
    zip_regex  = r"[A-Z]*_[A-Z]*_([A-Z0-9]*_[0-9\-]*)\.ZIP"
    
    try: 
        a_match = re.match(zip_regex, a_file)
    
        blob_in = f"{paths['from-cms']}/{a_file}"
        the_blob = blob_container.get_blob_client(blob_in)

        with open(f"{to_download}", 'wb') as _f: 
            _f.write(the_blob.download_blob().readall())
        return 1
    
    except Exception as expt: 
        return -1
        
        
def _extract_file(a_file): 
    try: 
        with ZipFile(f"{to_download}", 'r') as _unz:
            _unz.extractall(to_unzip)
        dbutils.fs.rm(f"file:{to_download}")
        return 1
    
    except Exception as expt: 
        return -2
    

def _upload_to_blob(a_file): 
    try: 
        write_as = filenames_2[a_file]
        a_match  = re.match(zip_regex, a_file)
        b_file   = a_match.group(1)

        the_blob = blob_container.get_blob_client(f"{paths['prepared']}/{write_as}")
        with open(f"{to_unzip}/{b_file}", 'rb') as _f: 
            the_blob.upload_blob(_f)
        dbutils.fs.rm(f"file:{to_unzip}/{b_file}") 
        return 1
    except Exception as expt: 
        return -3
    

# COMMAND ----------

detect_encoding = True
if detect_encoding:
    from pathlib import Path
    from chardet import detect
    a_path = Path("/dbfs/FileStore/transformation-layer/tmp_unzipped/DAMNA001_2022-12-18" )
    
    # We must read as binary (bytes) because we don't yet know encoding
    blob = a_path.read_bytes()
    a_detection = detect(blob)
    an_enc = a_detection["encoding"]
    a_confd = a_detection["confidence"]
    print(f"""{an_enc} encoding at {a_confd}""")

# COMMAND ----------

# MAGIC %md 
# MAGIC El siguiente _loop_ ejecuta la función para todos los archivos.  

# COMMAND ----------

unprocessed = []
nn = len(blobnames_1)

(ii, a_file) = list(enumerate(blobnames_1))[1]
for (ii, a_file) in enumerate(blobnames_1): 
    print(f"{(ii+1):6} de {nn} ... procesando archivo : {a_file}")
    
    status_1 = _download_blob(a_file)
    if status_1 != 1: 
        unprocessed.append((status_1, a_file))
        continue
    
    status_2 = _extract_file(a_file)
    if status_2 != 1: 
        unprocessed.append((status_2, a_file))
        continue
    
    status_3 = _upload_to_blob(a_file)
    if status_3 != 1: 
        unprocessed.append((status_3, a_file))


# COMMAND ----------

    (ii, a_file) = list(enumerate(blobnames_1))[10]
    
    status_1 = _download_blob(a_file)

    status_2 = _extract_file(a_file)
 
