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
import re

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from pyspark.sql import functions as F, types as T



# COMMAND ----------

# Variables de alto nivel.  (Revisar archivo CONFIG.PY)

from config import UAT_SPECS

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

read_from = f"abfss://bronze@{storage_ext}/{mid_read}"
write_to  = f"abfss://silver@{storage_ext}/{write_path}"



# COMMAND ----------

get_secret = lambda a_key: dbutils.secrets.get(dbks_scope, a_key)


spark.conf.set(f"fs.azure.account.auth.type.{storage_ext}", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_ext}", 
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_ext}", 
    get_secret(cred_keys['client_id']))
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_url}", 
    get_secret(cred_keys['client_secret']))
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_url}", 
    f"https://login.microsoftonline.com/{get_secret(cred_keys['tenant_id'])}/oauth2/token")


map_types = {
    'integer': lambda a_col: F.col(a_col).cast(T.IntegerType()), 
    'double' : lambda a_col: F.col(a_col).cast(T.DoubleType()), 
    'long'   : lambda a_col: F.col(a_col).cast(T.LongType()), 
    'date'   :(lambda a_col: 
        F.to_date( F.when(F.col(a_col) == 0, None)
                      .otherwise(F.col(a_col))
                      .cast(T.StringType()), 'yyyyMMdd')), 
    'string' : (lambda a_col: F.col(a_col).cast(T.StringType()))
}

def dict_2_colspecs(a_dict): 
    the_keys = sorted(a_dict.keys(), key = lambda k:a_dict[k])
    the_cols = [(a-1, a+b-1) for (a, b) in sorted(a_dict.values())]
    return (the_keys, the_cols)



# COMMAND ----------

   
        stream_raw = (spk_session.readStream.format(a_format)
            .options(**read_opts).schema(event_schema).load(raw_path))
            
        write_opts = {
            'mergeSchema': True, 
            'checkpointLocation': f'{brz_path}/checkpoints'}
        
        # No hay que de codificar el body en este paso, sino después 
        # en bronze o silver.  
        stream_proc = stream_raw.select(
            F.lit(an_event).alias('source'), 
            'EnqueuedTimeUtc', 'Body', decode_udf('Body').alias('json_body'),  
            F.current_timestamp().alias('ingest_ts'), 
            F.current_timestamp().cast('date').alias('p_ingest_date') )

        stream_table = (stream_proc.writeStream.format('delta')
            .outputMode('append')
            .partitionBy('p_ingest_date')
            .options(**{
                'mergeSchema': False, 
                'checkpointLocation': f'{brz_path}/checkpoints'}))

        stream_table.start(f'{brz_path}/delta')


# COMMAND ----------

the_keys = ['DAMNA', 'ATPTX', 'DAMBS1', 'DAMBS2', 'DAMBSC']


the_dir = f"{read_from}/ATPTX" 

stream_options = {
    'cloudFiles.format'                : 'text',
    'cloudFiles.useIncrementalListing' : 'true', 
    'recursiveFileLookup'              : 'true'
}

pre_atpt = spark.readStream.format('cloudFiles').options(**stream_options).load(the_dir)




# COMMAND ----------

display(pre_table)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Iniciación
# MAGIC Este comando se ejecuta para el registro de tablas en _metastore_.   
# MAGIC Se mantiene ante la condicional `create_tables = False` para no ejecutarse regularmente.  
# MAGIC Pero en caso de requerirlo en procesos de _debugueo_, se puede cambiar temporalmente a `True`.  

# COMMAND ----------

create_tables = False
tables_specs = {
    'ATPTX' : {
        'name'     : "farore_transactions.slv_ops_cms_reports",
        'location' : f"{write_to}/transactions"}, 
    'DAMNA' : {
        'name'     : "din_clients.slv_ops_cms_reports", 
        'location' : f"{write_to}/clients"}, 
    'DAMBS' : {
        'name'     : "nayru_accounts.slv_ops_cms_reports", 
        'location' : f"{write_to}/accounts"
} }

loc_2_delta = (lambda a_dict: 
    """CREATE TABLE {name} USING DELTA LOCATION "{location}";""".format(**a_dict))

if create_tables: 
    for each_one in [ 'ATPTX', 'DAMBS', 'DAMNA']: # 
        spark.sql(loc_2_delta(tables_specs[each_one]))
