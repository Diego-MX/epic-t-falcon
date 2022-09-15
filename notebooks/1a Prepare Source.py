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

from datetime import datetime as dt, date, timedelta as delta
from os import listdir
import pandas as pd
from pathlib import Path
import re
from zipfile import ZipFile, BadZipFile

from azure.identity import ClientSecretCredential
from azure.storage.blob import ContainerClient

dbks_scope = 'eh-core-banking'
blob_url = 'https://stlakehyliaqas.blob.core.windows.net/'

cred_keys = {
    'tenant_id'        : 'aad-tenant-id', 
    'subscription_id'  : 'sp-core-events-suscription', 
    'client_id'        : 'sp-core-events-client', 
    'client_secret'    : 'sp-core-events-secret'}

read_container = 'bronze'

read_path = "ops/regulatory/card-management/transformation-layer"
pre_write = "ops/regulatory/card-management/transformation-layer/unzipped-ready"

read_from = f"abfss://bronze@stlakehyliaqas.dfs.core.windows.net/{read_path}"
mid_write = f"abfss://bronze@stlakehyliaqas.dfs.core.windows.net/{pre_write}"

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
    'UAT_TRXS_ZATPTX01'    : 'ATPTX',
    'UAT_CUENTAS_DAMBS101' : 'DAMBS1',  
    'UAT_CUENTAS_DAMBS201' : 'DAMBS2', 
    'UAT_CUENTAS_DAMBSC01' : 'DAMBSC'}

file_regex = r"([A-Z_0-9]*?)_([0-9\-]*)\.ZIP"
zip_regex  = r"([A-Z0-9]*?)_([0-9\-]*)"

the_filenames = [filish.name for filish in dbutils.fs.ls(read_from)]

files_and_labels = [(a_name,) + re.match(file_regex, a_name).groups() 
    for a_name in the_filenames if re.match(file_regex, a_name)]

observed_labels = set(fl_lb[1] for fl_lb in files_and_labels)

if set(reg_labels) != observed_labels: 
    raise "File Labels are not as expected."


#%% About Local temporary download. 
to_download = "/dbfs/FileStore/transformation-layer/tmp_download.zip"
to_unzip    = "/dbfs/FileStore/transformation-layer/tmp_unzipped"
# Hint for files: EN DBUTILS no usar /DBFS.  En [Python] with(file_path) sí usar DBFS. 


#%% Working with the datalake. 
get_secret = lambda a_key: dbutils.secrets.get(dbks_scope, a_key)

the_credential = ClientSecretCredential(**{k: get_secret(v) for (k, v) in cred_keys.items()})

blob_container = ContainerClient(blob_url, 'bronze', the_credential) 



# COMMAND ----------

# MAGIC %md 
# MAGIC # Ejecución
# MAGIC 
# MAGIC Se comparan los fólders de lectura y escritura (`.../unzipped-ready/{ATPTX|DAMNA|DAMBS[12C]}`) para identificar archivos faltantes.  
# MAGIC Una vez identificados se descomprimen uno por uno ...

# COMMAND ----------

previous_blobs = [re.sub(f"{pre_write}/", '', a_blob.name) 
    for a_blob in blob_container.list_blobs(name_starts_with=pre_write)
    if a_blob.name.endswith('.txt')]

mid_write_names = {
    label[0]: f"{reg_labels[label[1]]}/{re.sub('-', '/', label[2])}.txt" 
        for label in files_and_labels 
        if label[1] in reg_labels}

only_process_these = {zip: txt 
    for zip, txt in mid_write_names.items()
    if  txt not in previous_blobs}


# COMMAND ----------

# Uses... 
read_path       = read_path
pre_write       = pre_write
mid_write_names = mid_write_names
to_download     = to_download
to_unzip        = to_unzip

def download_extract_upload(a_file, verbose=1): 
    try: 
        blob_in = f"/{read_path}/{a_file}"
        write_to = mid_write_names[a_file]
        
        the_blob = blob_container.get_blob_client(blob_in)
        
        with open(f"{to_download}", 'wb') as _f: 
            _f.write(the_blob.download_blob().readall())

        with ZipFile(f"{to_download}", 'r') as _unz:
            _unz.extractall(to_unzip)
    
        at_unzip = listdir(to_unzip)
        
        if (len(at_unzip) != 1): 
            print(f"\t¡¡¡ Múltiples o ningún archivo descomprimido: \n\t{a_file}".expandtabs(4))    
            raise "Uncompress Error"
        
        if not re.match(zip_regex, at_unzip[0]): 
            print("\tArchivo no admite REGEX: \n\t{a_file}".expandtabs(4))
            raise "File Name unfit for Regex"
            
        if verbose >= 1: 
            print(f"\tSubiendo archivo a blob: \n\t{pre_write}/{write_to}".expandtabs(4))
        
        the_blob = blob_container.get_blob_client(f"{pre_write}/{write_to}")
        
        with open(f"{to_unzip}/{at_unzip[0]}", 'rb') as _f: 
            the_blob.upload_blob(_f)
    
    except Exception as ex: 
        print(str(ex))
        return (a_file, -1)
        
    else: 
        dbutils.fs.rm(f"file:{to_download}")
        dbutils.fs.rm(f"file:{to_unzip}/{at_unzip[0]}")
    
    return (a_file, 1)
    

# COMMAND ----------

unprocessed = []
nn = len(only_process_these)

(ii, a_file) = list(enumerate(only_process_these))[1]
for (ii, a_file) in enumerate(only_process_these): 
    print(f"{(ii+1):6} de {nn} ... procesando archivo : {a_file}")
    
    (catch, status) = download_extract_upload(a_file)
    
    if status == -1: 
        unprocessed.append(catch)
        
    
