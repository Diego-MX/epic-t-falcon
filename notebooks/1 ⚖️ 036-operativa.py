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

# MAGIC %run ./0_install_nb_reqs

# COMMAND ----------

# pylint: disable=wrong-import-position,missing-module-docstring,wrong-import-order
from importlib import reload
from src import conciliation; reload(conciliation)      # pylint: disable=multiple-statements

from collections import OrderedDict
from datetime import datetime as dt, timedelta as delta
from json import dumps
from operator import add, itemgetter, methodcaller as ϱ
from pytz import timezone as tz

from pyspark.dbutils import DBUtils     # pylint: disable=import-error,no-name-in-module
from pyspark.sql import functions as F, Row, SparkSession
from toolz import compose_left, identity, juxt

from epic_py.tools import dirfiles_df, partial2
from src.conciliation import Sourcer, Conciliator, files_matcher, process_files
from config import t_agent, t_resourcer, DATALAKE_PATHS as paths
from refs.layouts import conciliations as c_layouts

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------

dbutils.widgets.text('date', 'yyyy-mm-dd')
dbutils.widgets.combobox('c4b',  'CC4B5', 
    ['CC4B2', 'CC4B3', 'CC4B5', 'CCB14', 'CCB15', 'CS4B1', 'FZE02'])
dbutils.widgets.combobox('fpsl', 'FZE07', 
    ['FZE01', 'FZE02', 'FZE03', 'FZE04', 'FZE05', 'FZE06', 'FZE07', 'FZE08', 'F1106'])

# COMMAND ----------

t_storage = t_resourcer['storage']
t_permissions = t_agent.prep_dbks_permissions(t_storage, 'gen2')
t_resourcer.set_dbks_permissions(t_permissions)
λ_address = (lambda ctner, p_key : t_resourcer.get_resource_url(
    'abfss', 'storage', container=ctner, blob_path=paths[p_key]))

at_conciliations = λ_address('raw', 'conciliations')
to_reports = λ_address('gold', 'reports2')

dumps2 = lambda xx: dumps(xx, default=str)
tmp_parent = compose_left(
    ϱ('split', '/'), itemgetter(slice(0, -1)),
    partial2(add, ..., ['tmp',]),
    '/'.join)

c4b_key = dbutils.widgets.get('c4b')
fpsl_key = dbutils.widgets.get('fpsl')
w_date = dbutils.widgets.get('date')

if w_date == 'yyyy-mm-dd': 
    now_mx = dt.now(tz('America/Mexico_City'))
    r_date = now_mx.date() - delta(days=1) 
    s_date = r_date.strftime('%Y-%m-%d')
else: 
    r_date = dt.strptime(w_date, '%Y-%m-%d')
    s_date = w_date

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1. Esquemas y carpetas

# COMMAND ----------

# MAGIC %md  
# MAGIC
# MAGIC - Las fuentes de datos que utilizamos se alojan en carpetas del _datalake_.  
# MAGIC - Los archivos de las carpetas tienen metadatos en sus nombres, que se extraen en 
# MAGIC la subsección `Regex Carpetas`.  
# MAGIC - Además los archivos consisten de datos tabulares cuyos esquemas se construyen en 
# MAGIC la sección correspondiente.  
# MAGIC - Finalmente se procesa cada una de las fuentes, de acuerdo a la llave de identificación, 
# MAGIC su descripción y el sistema al que pertenece.    
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
# MAGIC #### a. Subldedger (FPSL)

# COMMAND ----------

data_src   = 'subledger'    # pylint: disable=invalid-name 

files_0 = dirfiles_df(f"{at_conciliations}/{data_src}", spark)
files_1 = process_files(files_0, data_src)
ldgr_results = files_matcher(files_1, dict(date=r_date, key=fpsl_key))
(ldgr_files, ldgr_path, ldgr_status) = ldgr_results
ldgr_files.query('matcher > 0')

# COMMAND ----------

ldgr_src = Sourcer(ldgr_path, **c_layouts.fpsl_specs)
ldgr_prep = ldgr_src.start_data(spark)
ldgr_data = ldgr_src.setup_data(ldgr_prep)
ldgr_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### b. Cloud Banking (C4B)

# COMMAND ----------

data_src  = 'cloud-banking'     # pylint: disable=invalid-name
files_0 = dirfiles_df(f"{at_conciliations}/{data_src}", spark)
files_1 = process_files(files_0, data_src)

c4b_results = files_matcher(files_1, dict(date=r_date, key=c4b_key))
(c4b_files, c4b_path, c4b_status) = c4b_results
(c4b_files.query('matcher > 0')     # pylint: disable=expression-not-assigned
    .reset_index()
    .loc[:, ['name', 'key', 'date', 'matcher', 'size', 'modificationTime']])

# COMMAND ----------

# MAGIC %md 
# MAGIC ### ii. Subldedger (FPSL)

# COMMAND ----------

src_1 = 'subledger'    # pylint: disable=invalid-name 

files_0 = dirfiles_df(f"{at_conciliations}/{src_1}", spark)
files_1 = process_files(files_0, src_1)
ldgr_results = files_matcher(files_1, dict(date=r_date, key=fpsl_key))
(ldgr_files, ldgr_path, ldgr_status) = ldgr_results
(ldgr_files.query('matcher > 0')        # pylint: disable=expression-not-assigned
    .reset_index()
    .loc[:,['name', 'key', 'date', 'matcher', 'size', 'modificationTime']])

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2. Preparación de fuentes

# COMMAND ----------

# MAGIC %md  
# MAGIC
# MAGIC Utilizamos una llave general de fecha `key_date` para leer los archivos de las 4 fuentes.  
# MAGIC Para cada fuente seguimos el procedimiento:  
# MAGIC     - Identificar un archivo, y sólo uno, con la fecha proporcionada.  
# MAGIC     - Leer los datos de acuerdo a las especificaciones definidas en la sección anterior, 
# MAGIC y mostrar la tabla correspondiente.  
# MAGIC     - Definir modificaciones de acuerdo a los propios reportes de conciliación, 
# MAGIC y aplicarlos para alistar las tablas.  

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Cloud Banking (C4B)

# COMMAND ----------

def name_item(names): 
    λ_name = lambda k_v: dict(zip(names, k_v))
    return λ_name 

prod_name = lambda k_v: Row(tipo_prod=k_v[0], ACCOUNTPRODUCTID=k_v[1])
prod_dict = {
    'EPC_OP_MAX': 'EPC_OP_MAX', 
    'EPC_TA_MAX': 'EPC_TA_MA1',
    'EPC_LA_PER': 'EPC_LA_PE1'}

prod_df = spark.createDataFrame([name_item(('tipo_prod', 'ACCOUNTPRODUCTID'))(p_item) 
        for p_item in prod_dict.items()])

c4b_src = Sourcer(c4b_path, **c_layouts.c4b_specs)
c4b_prep = c4b_src.start_data(spark)
c4b_data = (c4b_src.setup_data(c4b_prep)
    .join(prod_df, on='ACCOUNTPRODUCTID', how='left'))

c4b_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Subledger (FPSL)

# COMMAND ----------

ldgr_src = Sourcer(ldgr_path, **c_layouts.fpsl_specs)
ldgr_prep = ldgr_src.start_data(spark)
ldgr_data = ldgr_src.setup_data(ldgr_prep)

ldgr_data.filter('txn_valid').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reportes y escrituras

# COMMAND ----------

dir_036  = f"{to_reports}/operational"

check_txns = OrderedDict({
    'valida': (F.col('fpsl_num_txns') == F.col('c4b_num_txns')) 
            & (F.col('fpsl_monto') + F.col('c4b_monto') == 0), 
    'opuesta':(F.col('fpsl_num_txns') == F.col('c4b_num_txns')) 
            & (F.col('fpsl_monto') == F.col('c4b_monto')), 
    'c4b':    (F.col( 'c4b_num_txns') == 0) | (F.col( 'c4b_num_txns').isNull()), 
    'fpsl':   (F.col( 'c4b_num_txns') == 0) | (F.col( 'c4b_num_txns').isNull()), 
    'indeterminada': None})

# Extras: no es muy formal, pero es muy práctico. 
fpsl_cuenta = (ldgr_data
    .filter('txn_valid')
    .select('cuenta_fpsl', 'num_cuenta', 'clave_txn', 'tipo_prod')
    .distinct())

report_036 = Conciliator(c4b_src, ldgr_src, check_txns)
base_036   = report_036.base_match(c4b_data, ldgr_data)
diffs_036  = report_036.filter_checks(base_036, '~valida')
fpsl_036   = report_036.filter_checks(base_036, ['fpsl', 'indeterminada'], 
    join_alias='subledger')
c4b_036    = report_036.filter_checks(base_036, ['c4b',  'indeterminada'], 
    join_alias='cloud-banking')

base_adj = (fpsl_cuenta
    .join(base_036, how='right', 
        on=['num_cuenta', 'clave_txn', 'tipo_prod']))

two_paths = juxt(identity, tmp_parent)
base_adj.save_as_file(*two_paths(f"{dir_036}/compare/{s_date}_036_comparativo.csv"),
    header=True)
diffs_036.save_as_file(*two_paths(f"{dir_036}/discrepancies/{s_date}_036_discrepancias.csv"),
    header=True)
fpsl_036.save_as_file(*two_paths(f"{dir_036}/subledger/{s_date}_036_fpsl.csv"),
    header=True)
c4b_036.save_as_file(*two_paths(f"{dir_036}/cloud-banking/{s_date}_036_c4b.csv"),
    header=True)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Resultados

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Revisar conteo de trxns válidas. 

# COMMAND ----------

(base_036
    .groupBy('check_key')
    .count()
    .display())

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Revisar Archivos

# COMMAND ----------

a_dir = f"{dir_036}/compare/processed"
print(a_dir)
dirfiles_df(a_dir, spark)
