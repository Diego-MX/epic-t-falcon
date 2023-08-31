# Databricks notebook source
# MAGIC %md
# MAGIC ## Introducción
# MAGIC El objetivo de este notebook es correr los scripts para ejecutar las conciliaciones. 
# MAGIC El código se divide en las siguientes partes:  
# MAGIC &nbsp; 0. Preparar librerías, variables, funciones, etc.  
# MAGIC 1. Preparar los esquemas y carpetas.  
# MAGIC 2. Lectura de tablas y fuentes.  
# MAGIC 3. Generación y escritura de reportes.    

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Preparación código

# COMMAND ----------

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
import subprocess
import yaml
from collections import OrderedDict
from datetime import datetime as dt, date, timedelta as delta
from pyspark.sql import functions as F, types as T
from pytz import timezone

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

with open("../user_databricks.yml", 'r') as _f: 
    u_dbks = yaml.safe_load(_f)

epicpy_load = {
    'url'   : 'github.com/Bineo2/data-python-tools.git', 
    'branch': 'dev-diego',
    'token' : dbutils.secrets.get(u_dbks['dbks_scope'], u_dbks['dbks_token'])}

url_call = "git+https://{token}@{url}@{branch}".format(**epicpy_load)
subprocess.check_call(['pip', 'install', url_call])

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 0.b Especificar fechas

# COMMAND ----------

manual = False 
imprimir = True

if manual: 
    yday_ish = date(2024, 6, 7)   #  {jul: [3,4,9], jun:[7,30]}
else: 
    now_mx = dt.now(timezone('America/Mexico_City'))
    yday_ish = now_mx.date() - delta(days=1)  

which_files = {
    'cloud-banking': {'date': yday_ish, 'key': 'CC4B3'},  # 'CCB15'
    'subledger'    : {'date': yday_ish, 'key': 'FZE05'}}  # 'FZE03'

key_date_ops  = yday_ish
key_date_spei = yday_ish

# COMMAND ----------

from importlib import reload
import epic_py; reload(epic_py)
import src; reload(src)
import config; reload(config)

from epic_py.delta import EpicDF
from epic_py.tools import dirfiles_df
from src.tools import write_datalake
from src.sftp_sources import process_files
from config import (t_agent, t_resources, 
    DATALAKE_PATHS as paths)

t_storage = t_resources['storage']
t_permissions = t_agent.prep_dbks_permissions(t_storage, 'gen2')

λ_address = (lambda ctner, p_key : t_resources.get_resource_url(
    'abfss', 'storage', container=ctner, blob_path=paths[p_key]))

at_conciliations = λ_address('raw', 'conciliations')
to_reports       = λ_address('gold', 'reports2')

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1. Esquemas y carpetas

# COMMAND ----------

# MAGIC %md  
# MAGIC
# MAGIC - Las fuentes de datos que utilizamos se alojan en carpetas del _datalake_.  
# MAGIC - Los archivos de las carpetas tienen metadatos en sus nombres, que se extraen en la subsección `Regex Carpetas`.  
# MAGIC - Además los archivos consisten de datos tabulares cuyos esquemas se construyen en la sección correspondiente.  
# MAGIC - Finalmente se procesa cada una de las fuentes, de acuerdo a la llave de identificación, su descripción y el sistema al que pertenece.    
# MAGIC   + `subledger`; sistema contable operativo; FPSL de SAP (_financial product subleger_),    
# MAGIC   + `cloud-banking`; sistema operativo general; C4B de SAP (tal cual _cloud for banking_),  
# MAGIC   + `spei-ledger`; sistema contable de transferencias electrónicas;   
# MAGIC     sistema de pagos de transferencias electrónicas por Grupo Financiero Banorte,  
# MAGIC   + `spei-banking`; sistema operativo de transferencias electrónicas;  
# MAGIC     sistema de pagos de transferencias electrónicas en C4B de SAP. 
# MAGIC
# MAGIC Se muestra el contenido de las fuentes, a partir de los archivos correspondientes.   
# MAGIC La lectura de las mismas se pospone a la siguiente sección.   

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Manipulación de columnas

# COMMAND ----------

ops_fpsl = {
    'name': 'subledger',
    'alias': 'fpsl',  
    'options': dict([
        ('mode', 'PERMISIVE'), 
        ('sep', '|'), ('header', True), ('nullValue', 'null'), 
        ('dateFormat', 'd.M.y'), ('timestampFormat', 'd.M.y H:m:s')]),
    'schema' : OrderedDict({ 
        'C55POSTD' : 'date', 'C55YEAR'    : 'int',
        'C35TRXTYP': 'str' , 'C55CONTID'  : 'str',
        'C11PRDCTR': 'str' , 'K5SAMLOC'   : 'str',  
        'LOC_CURR' : 'str' , 'IGL_ACCOUNT': 'long'}), 
    'mutate' : {
        'K5SAMLOC'    : (F.regexp_replace(F.col('K5SAMLOC'), r"(\-?)([0-9\.])(\-?)", "$3$1$2")
                       .cast('double')), 
        'txn_valid'   : F.col('C11PRDCTR').isNotNull() 
                      & F.col('C11PRDCTR').startswith('EPC') 
                      &~F.col('C35TRXTYP').startswith('S'), 
        'num_cuenta'  : F.substring(F.col('C55CONTID'), 1, 11),  # JOIN
        'clave_txn'   : F.col('C35TRXTYP').cast('int'),
        'moneda'      : F.col('LOC_CURR'), 
        'monto_txn'   : F.col('K5SAMLOC'),     # Suma y compara. 
        'cuenta_fpsl' : F.col('IGL_ACCOUNT')}, # Referencia extra, asociada a NUM_CUENTA.  
    'match': {
        'where': [F.col('txn_valid')], 
        'by'   : ['num_cuenta', 'clave_txn', 'moneda'], 
        'agg'  : {
            'fpsl_num_txns': F.count('*'), 
            'fpsl_monto'   : F.round(F.sum('monto_txn'), 2)}}
}

ops_c4b = {
    'name': 'cloud-banking',
    'alias': 'c4b',  
    'options': dict([
        ('mode', 'PERMISIVE'), 
        ('sep', '|'), ('header', True), ('nullValue', 'null'), 
        ('dateFormat', 'd.M.y'), ('timestampFormat', 'd.M.y H:m:s')]), 
    'schema' : OrderedDict({
        'ACCOUNTID': 'str', 'TRANSACTIONTYPENAME': 'str', 'ACCOUNTHOLDERID': 'long', 
        'POSTINGDATE':'date', 'AMOUNT': 'dbl', 'CURRENCY': 'str', 
        'VALUEDATE': 'date', 'STATUSNAME': 'str', 'COUNTERPARTYACCOUNTHOLDER': 'str', 
        'COUNTERPARTYBANKACCOUNT': 'str', 'CREATIONUSER': 'str', 
        'TRANSACTIONID': 'str', 'TYPECODE': 'int', 'TYPENAME': 'str', 'PAYMENTTYPECODE': 'int', 
        'PAYMENTTYPENAME': 'str', 'TRANSACTIONTYPECODE':'int', 
        'COMMUNICATIONCHANNELCODE': 'int', 'COMMUNICATIONCHANNELNAME': 'str', 
        'ACCOUNTPROJECTION': 'str', 
        'ACCOUNTPRODUCTID': 'str', 'DEBITINDICATOR': 'str', 
        'EXCHANGERATETYPECODE': 'str', 'EXCHANGERATETYPENAME': 'str', 'EXCHANGERATE': 'str', 
        'AMOUNTAC': 'dbl', 'CURRENCYAC': 'str', 'PRENOTEID': 'str', 'STATUSCODE': 'int', 
        'COUNTERPARTYBANKCOUNTRY': 'str', 'COUNTERPARTYBANKID': 'str', 
        'COUNTERPARTYBANKACCOUNTID': 'str', 'PAYMENTTRANSACTIONORDERID': 'str', 
        'PAYMENTTRANSACTIONORDERITEMID': 'str', 'REFADJUSTMENTTRANSACTIONID': 'str', 
        'CANCELLATIONDOCUMENTINDICATOR': 'str', 'CANCELLEDENTRYREFERENCE': 'str',
        'CANCELLATIONENTRYREFERENCE': 'str', 'PAYMENTNOTES': 'str', 
        'CREATIONDATETIME': 'ts', 'CHANGEDATETIME': 'ts', 'RELEASEDATETIME': 'ts',
        'CHANGEUSER': 'str', 'RELEASEUSER': 'str', 'COUNTER': 'int'}), 
    'mutate': {
        'txn_valid'   : F.lit(True),  # Filtrar
        'num_cuenta'  : F.split(F.col('ACCOUNTID'), '-')[0], # JOIN
        'clave_txn'   : F.col('TRANSACTIONTYPECODE'),
        'moneda'      : F.col('CURRENCY'), 
        'monto_txn'   : F.col('AMOUNT'),  # Suma y compara
        'tipo_txn'    : F.col('TYPENAME')},  # Referencia, asociada a CLAVE_TXN.   
    'match': {
        'where': [F.col('txn_valid')], 
        'by'   : ['num_cuenta', 'clave_txn', 'moneda'], 
        'agg'  : {
            'c4b_num_txns': F.count('*'), 
            'c4b_monto'   : F.round(F.sum('monto_txn'), 2)}}
}


# COMMAND ----------

# MAGIC %md 
# MAGIC #### a. Subldedger (FPSL)

# COMMAND ----------

since_when = yday_ish - delta(5)
data_src   = 'subledger'
pre_files  = dirfiles_df(f"{at_conciliations}/{data_src}", spark)
fpsl_files = process_files(pre_files, data_src)
print(which_files['subledger'])
fpsl_files.sort_values(['date'], ascending=False).query(f"date >= '{since_when}'")

# COMMAND ----------

# MAGIC %md
# MAGIC #### b. Cloud Banking (C4B)

# COMMAND ----------

data_src  = 'cloud-banking'
pre_files = dirfiles_df(f"{at_conciliations}/{data_src}", spark)
c4b_files = process_files(pre_files, data_src)
print(which_files['cloud-banking'])
c4b_files.sort_values('date', ascending=False).query(f"date >= '{since_when}'")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2. Preparación de fuentes

# COMMAND ----------

from importlib import reload
from src import conciliation; reload(conciliation)
from src.conciliation import Sourcer, Conciliator as conciliate, get_source_path


# COMMAND ----------

# MAGIC %md  
# MAGIC
# MAGIC Utilizamos una llave general de fecha `key_date` para leer los archivos de las 4 fuentes.  
# MAGIC Para cada fuente seguimos el procedimiento:  
# MAGIC     - Identificar un archivo, y sólo uno, con la fecha proporcionada.  
# MAGIC     - Leer los datos de acuerdo a las especificaciones definidas en la sección anterior, y mostrar la tabla correspondiente.  
# MAGIC     - Definir modificaciones de acuerdo a los propios reportes de conciliación, y aplicarlos para alistar las tablas.  

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Subledger (FPSL)

# COMMAND ----------

fpsl_path = get_source_path(fpsl_files, which_files['subledger'])

try: 
    fpsl_src = Sourcer(fpsl_path, **ops_fpsl)
    fpsl_prep = fpsl_src.start_data(spark)
    fpsl_data = fpsl_src.setup_data(fpsl_prep)

    fpsl_data.display()
except: 
    fpsl_data = None


# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Cloud Banking (C4B)

# COMMAND ----------

c4b_path = get_source_path(c4b_files, which_files['cloud-banking'])

try: 
    c4b_src = Sourcer(c4b_path, **ldgr_c4b)
    c4b_prep = c4b_src.start_data(spark)
    c4b_data = c4b_src.setup_data(c4b_prep)

    c4b_data.display()
except: 
    c4b_data = None


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reportes y escrituras

# COMMAND ----------

# MAGIC %md 
# MAGIC ### a. (036) Operativa

# COMMAND ----------

dir_036  = f"{to_reports}/operational"

check_txns = OrderedDict({
    'valida': (F.col('fpsl_num_txns') == F.col('c4b_num_txns')) 
            & (F.col('fpsl_monto') + F.col('c4b_monto') == 0), 
    'opuesta':(F.col('fpsl_num_txns') == F.col('c4b_num_txns')) 
            & (F.col('fpsl_monto') == F.col('c4b_monto')), 
    'c4b':  (F.col( 'c4b_num_txns') == 0) | (F.col( 'c4b_num_txns').isNull()), 
    'fpsl': (F.col( 'c4b_num_txns') == 0) | (F.col( 'c4b_num_txns').isNull()), 
    'indeterminada': None})

report_036 = Conciliator(ldgr_src, c4b_src, check_txns)
base_036   = report_036.base_match(ldgr_data, c4b_data)
diffs_036  = report_036.filter_checks(base_036, '~valida')
fpsl_036   = report_036.filter_checks(base_036, ['fpsl', 'indeterminada'])
c4b_036    = report_036.filter_checks(base_036, ['c4b',  'indeterminada'])

if imprimir: 
    write_datalake(base_036, spark=spark, overwrite=True, 
            a_path=f"{dir_036}/compare/{key_date_ops}_036_comparativo.csv")
    write_datalake(diffs_036, spark=spark, overwrite=True, 
            a_path=f"{dir_036}/discrepancies/{key_date_ops}_036_discrepancias.csv")
    write_datalake(fpsl_036, spark=spark, overwrite=True,
            a_path=f"{dir_036}/subledger/{key_date_ops}_036_fpsl.csv")
    write_datalake(c4b_036, spark=spark, overwrite=True,
            a_path=f"{dir_036}/cloud-banking/{key_date_ops}_036_c4b.csv")

base_036.display()



# COMMAND ----------

# MAGIC %md 
# MAGIC ### Resultados
# MAGIC
# MAGIC Aquí vemos algunos de los resultados claves de le ejecución:  
# MAGIC - Procesos que no se concluyeron.  
# MAGIC - Resumenes de los que sí.  

# COMMAND ----------

from json import dumps
dumps2 = lambda xx: dumps(xx, default=str)

for kk, vv in which_files.items(): 
    print(f"{kk}\t:{dumps2(vv)}")

if ldgr_data is None: 
    print(f"No se encontró FPSL correspondiente a {dumps2(which_files['subledger'])}.")

if c4b_data is None: 
    print(f"No se encontró C4B correspondiente a {dumps2(which_files['cloud-banking'])}.")

if base_036 is None: 
    print(f"No se concluyó el reporte 036.")
    
