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

from collections import OrderedDict
from datetime import datetime as dt, date, timedelta as delta
from pyspark.sql import functions as F, types as T
from pytz import timezone

now_mx = dt.now(timezone('America/Mexico_City'))
yday_ish = now_mx.date() - delta(days=1)  
yday_ish = date(2024, 6, 7)   #  {jul: [3,4,9], jun:[7,30]}
# falta 7 de junio. 

# ya se hizo un pequeño desorden cuando empezaron a cambiar fechas, y áreas bancarias.  
which_files = {
    'cloud-banking' : {'date': yday_ish, 'key' : 'CC4B3'},  # 'CCB15'
    'subledger'     : {'date': yday_ish, 'key' : 'FZE05'}}  # 'FZE03'

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
to_reports       = λ_address('gold',   'reports2')

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

# For Subledger

ops_fpsl = {
    'base' : OrderedDict({ 
        'C55POSTD' : 'date', 'C55YEAR'    : 'int',
        'C35TRXTYP': 'str' , 'C55CONTID'  : 'str',
        'C11PRDCTR': 'str' , 'K5SAMLOC'   : 'str',  
        'LOC_CURR' : 'str' , 'IGL_ACCOUNT': 'long'}), 
    'read' : {
        'K5SAMLOC' : (F.regexp_replace(F.col('K5SAMLOC'), r"(\-?)([0-9\.])(\-?)", "$3$1$2")
                       .cast(T.DoubleType()))}, 
    'mod' : {
        'txn_valid'   : F.col('C11PRDCTR').isNotNull() 
                      & F.col('C11PRDCTR').startswith('EPC') 
                      &~F.col('C35TRXTYP').startswith('S'), 
        'num_cuenta'  : F.substring(F.col('C55CONTID'), 1, 11),  # JOIN
        'clave_txn'   : F.col('C35TRXTYP').cast(T.IntegerType()),
        'moneda'      : F.col('LOC_CURR'), 
        'monto_txn'   : F.col('K5SAMLOC'),     # Suma y compara. 
        'cuenta_fpsl' : F.col('IGL_ACCOUNT')}, # Referencia extra, asociada a NUM_CUENTA.  
    'post': {
        'where': ['txn_valid'], 
        'by'   : ['num_cuenta', 'clave_txn', 'moneda'], 
        'agg'  : {
            'fpsl_num_txns': F.count('*'), 
            'fpsl_monto'   : F.round(F.sum('monto_txn'), 2)}}
}

ops_c4b = {
    'base' : OrderedDict({
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
    'mod': {
        'txn_valid'   : F.lit(True),  # Filtrar
        'num_cuenta'  : F.split(F.col('ACCOUNTID'), '-')[0], # JOIN
        'clave_txn'   : F.col('TRANSACTIONTYPECODE'),
        'moneda'      : F.col('CURRENCY'), 
        'monto_txn'   : F.col('AMOUNT'),  # Suma y compara
        'tipo_txn'    : F.col('TYPENAME')},  # Referencia, asociada a CLAVE_TXN.   
    'post': {
        'where': ['txn_valid'], 
        'by'   : ['num_cuenta', 'clave_txn', 'moneda'], 
        'agg'  : {
            'c4b_num_txns': F.count('*'), 
            'c4b_monto'   : F.round(F.sum('monto_txn'), 2)}}
}


# COMMAND ----------

### Parsing and Schemas.  

# From Columns
base_cols = {
    'subledger'    : ops_fpsl['base'], 
    'cloud-banking': ops_c4b ['base']}

read_cols = {
    'subledger'    : ops_fpsl.get('read', {}), 
    'cloud-banking': ops_c4b.get ('read', {})}

mod_cols = {
    'subledger'    : ops_fpsl.get('mod', {}), 
    'cloud-banking': ops_c4b.get ('mod', {})}


# General
tsv_options = {
    'subledger' : dict([
        ('mode', 'PERMISIVE'), 
        ('sep', '|'), ('header', True), ('nullValue', 'null'), 
        ('dateFormat', 'd.M.y'), ('timestampFormat', 'd.M.y H:m:s')]), 
    'cloud-banking' : dict([
        ('mode', 'PERMISIVE'), 
        ('sep', '|'), ('header', True), ('nullValue', 'null'), 
        ('dateFormat', 'd.M.y'), ('timestampFormat', 'd.M.y H:m:s')]),
}

schema_types = {
    'int' : T.IntegerType, 'long': T.LongType,   'ts'   : T.TimestampType, 
    'str' : T.StringType,  'dbl' : T.DoubleType, 'date' : T.DateType, 
    'bool': T.BooleanType, 'flt' : T.FloatType,  'null' : T.NullType}

schemas = {
    'subledger'     : T.StructType([
            T.StructField(f"/BA1/{kk}", schema_types[vv](), True) 
            for kk, vv in base_cols['subledger'].items()]), 
    'cloud-banking' : T.StructType([
            T.StructField(kk, schema_types[vv](), True) 
            for kk, vv in base_cols['cloud-banking'].items()]), 
}

renamers = {
    'subledger'     : [F.col(f"/BA1/{kk}").alias(kk) for kk in base_cols['subledger']], 
    'cloud-banking' : [kk for kk in base_cols['cloud-banking']], 
}


# COMMAND ----------

# MAGIC %md 
# MAGIC #### a. Subldedger (FPSL)

# COMMAND ----------

since_when = yday_ish - delta(5)
data_src   = 'subledger'
pre_files  = dirfiles_df(f"{at_conciliations}/{data_src}", spark)
ldgr_files = process_files(pre_files, data_src)
print(which_files['subledger'])
ldgr_files.sort_values(['date'], ascending=False).query(f"date >= '{since_when}'")

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

# MAGIC %md  
# MAGIC
# MAGIC Utilizamos una llave general de fecha `key_date` para leer los archivos de las 4 fuentes.  
# MAGIC Para cada fuente seguimos el procedimiento:  
# MAGIC     - Identificar un archivo, y sólo uno, con la fecha proporcionada.  
# MAGIC     - Leer los datos de acuerdo a las especificaciones definidas en la sección anterior, y mostrar la tabla correspondiente.  
# MAGIC     - Definir modificaciones de acuerdo a los propios reportes de conciliación, y aplicarlos para alistar las tablas.  

# COMMAND ----------

def read_source_table(src_key, dir_df, file_keys, output=None, verbose=False): 
    q_str = ' & '.join(f"({k} == '{v}')" 
        for k, v in file_keys.items())
    
    if verbose: 
        print(q_str)

    path_df = dir_df.query(q_str)
    if path_df.shape[0] != 1: 
        print(f"File keys match is not unique.")
        return None
    
    src_path = path_df['path'].iloc[0]        

    if output == 0: 
        table_0 = (spark.read.format('text')
            .load(src_path))
        return table_0
        
    table_0 = EpicDF(spark
            .read.format('csv')
            .options(**tsv_options[src_key])
            .schema(schemas[src_key])
            .load(src_path))
    
    table_1 = table_0.select(*renamers[src_key])
    
    trim_str = {a_col: F.trim(a_col) 
        for a_col, a_type in base_cols[src_key].items() if a_type == 'str'}
    table_11 = table_1.with_column_plus(trim_str)
    
    if output == 1: 
        return table_11
    
    table_2 = table_11.with_column_plus(read_cols[src_key])
    if output == 2: 
        return table_2
    
    table_3 = table_2.with_column_plus(mod_cols[src_key])
    if output == 3 or output is None: 
        return table_3


# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Subledger (FPSL)

# COMMAND ----------

# Columnas: [txn_valid, num_cuenta, cuenta_fpsl, clave_trxn, moneda, monto_trxn]

ldgr_tbl = read_source_table('subledger', ldgr_files, which_files['subledger'])

if ldgr_tbl is not None: 
    ldgr_grp = (ldgr_tbl
        .filter(F.col('txn_valid'))
        .fillna(0, subset='monto_txn')
        .groupby('num_cuenta', 'clave_txn', 'moneda', 
                 'cuenta_fpsl') # Esta columna es de referencia, supuestamente 1-a-1 con NUM_CUENTA. 
        .agg(F.count('*').alias('fpsl_num_txns'), 
             F.round(F.sum(F.col('monto_txn')), 2).alias('fpsl_monto'))) 
    
    ldgr_tbl.display()
else: 
    ldgr_grp = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Cloud Banking (C4B)

# COMMAND ----------

c4b_tbl = read_source_table('cloud-banking', c4b_files, 
        which_files['cloud-banking'], verbose=True)

if c4b_tbl is not None: 
    c4b_grp = (c4b_tbl
        .filter(F.col('txn_valid'))  
        .fillna(0, subset='monto_txn')
        .groupby('num_cuenta', 'clave_txn', 'moneda', 
                 'tipo_txn')  # Columna de referencia, se asume 1-a-1 con TIPO_TRXN
        .agg(F.count('*').alias('c4b_num_txns'), 
             F.round(F.sum(F.col('monto_txn')), 2).alias('c4b_monto'))) 

    c4b_grp.display()
else: 
    c4b_grp = None


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reportes y escrituras

# COMMAND ----------

# MAGIC %md 
# MAGIC ### a. (036) Operativa

# COMMAND ----------

dir_036  = f"{to_reports}/operational"

try:
    base_036 = (c4b_grp
        .join(ldgr_grp, how='full', on=['num_cuenta', 'clave_txn', 'moneda'])
        .withColumn('check_key', 
            F.when((F.col('fpsl_num_txns') == F.col('c4b_num_txns')) 
                 & (F.col('fpsl_monto') + F.col('c4b_monto') == 0), 'valida')
             .when((F.col('fpsl_num_txns') == F.col('c4b_num_txns')) 
                 & (F.col('fpsl_monto') == F.col('c4b_monto')),     'opuesta')
             .when((F.col( 'c4b_num_txns') == 0) | (F.col( 'c4b_num_txns').isNull()), 'c4b')
             .when((F.col('fpsl_num_txns') == 0) | (F.col('fpsl_num_txns').isNull()), 'fpsl')
             .otherwise('indeterminada')))

    discrp_036 = (base_036
        .filter(F.col('check_key') != 'valida'))

    fpsl_036 = (base_036
        .filter(F.col('check_key').isin(['fpsl', 'indeterinada'])) 
        .join(ldgr_tbl, how='left', 
            on=['num_cuenta', 'clave_txn', 'moneda', 'cuenta_fpsl']))

    c4b_036 = (base_036
        .filter(F.col('check_key').isin(['c4b', 'indeterminada']))
        .join(how='left', on=['num_cuenta', 'clave_txn', 'moneda', 'tipo_txn'], 
            other=c4b_tbl))
    
    write_datalake(base_036, spark=spark, overwrite=True, 
            a_path=f"{dir_036}/compare/{key_date_ops}_036_comparativo.csv")
    write_datalake(discrp_036, spark=spark, overwrite=True, 
            a_path=f"{dir_036}/discrepancies/{key_date_ops}_036_discrepancias.csv")
    write_datalake(fpsl_036, spark=spark, overwrite=True,
            a_path=f"{dir_036}/subledger/{key_date_ops}_036_fpsl.csv")
    write_datalake(c4b_036, spark=spark, overwrite=True,
            a_path=f"{dir_036}/cloud-banking/{key_date_ops}_036_c4b.csv")

    base_036.display()   
except Exception as expt: 
    print(str(expt))
    base_036 = None

# COMMAND ----------

if base_036 is not None: 
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

if ldgr_grp is None: 
    print(f"No se encontró FPSL correspondiente a {dumps2(which_files['subledger'])}.")

if c4b_grp is None: 
    print(f"No se encontró C4B correspondiente a {dumps2(which_files['cloud-banking'])}.")

if base_036 is None: 
    print(f"No se concluyó el reporte 036.")
    
