# Databricks notebook source    # pylint: disable=missing-module-docstring
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
from collections import OrderedDict
from datetime import datetime as dt, timedelta as delta
from json import dumps
from pytz import timezone as tz

from pyspark.dbutils import DBUtils     # pylint: disable=import-error,no-name-in-module
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from epic_py.tools import dirfiles_df

from src.sftp_sources import process_files
from src.conciliation import Sourcer, Conciliator, get_source_path
from config import t_agent, t_resourcer, DATALAKE_PATHS as paths
from refs.layouts import conciliations as c_layouts

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------

now_mx = dt.now(tz('America/Mexico_City'))
yday = now_mx.date() - delta(days=1) 
dbutils.widgets.text('date', yday.strftime('%Y-%m-%d'))
dbutils.widgets.text('c4b',  'CC4B5')
dbutils.widgets.text('fpsl', 'FZE07')

# COMMAND ----------

r_date = dt.strptime(dbutils.widgets.get('date'), '%Y-%m-%d')
c4b_key = dbutils.widgets.get('c4b')
fpsl_key = dbutils.widgets.get('fpsl')

which_files = {
    'cloud-banking': dict(date=r_date, key=c4b_key),  
    'subledger'    : dict(date=r_date, key=fpsl_key)} 

# COMMAND ----------

t_storage = t_resourcer['storage']
t_permissions = t_agent.prep_dbks_permissions(t_storage, 'gen2')
t_resourcer.set_dbks_permissions(t_permissions)

λ_address = (lambda ctner, p_key : t_resourcer.get_resource_url(
    'abfss', 'storage', container=ctner, blob_path=paths[p_key]))

at_conciliations = λ_address('raw', 'conciliations')
to_reports       = λ_address('gold', 'reports2')

dumps2 = lambda xx: dumps(xx, default=str)

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

since_when = r_date - delta(5)
data_src   = 'subledger'    # pylint: disable=invalid-name 
pre_files  = dirfiles_df(f"{at_conciliations}/{data_src}", spark)
ldgr_files = process_files(pre_files, data_src)
print(which_files['subledger'])
(ldgr_files
    .sort_values(['date'], ascending=False)
    .query(f"(date >= '{since_when}') & (key == 'FZE07')"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### b. Cloud Banking (C4B)

# COMMAND ----------

data_src  = 'cloud-banking'     # pylint: disable=invalid-name
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
# MAGIC     - Leer los datos de acuerdo a las especificaciones definidas en la sección anterior, 
# MAGIC y mostrar la tabla correspondiente.  
# MAGIC     - Definir modificaciones de acuerdo a los propios reportes de conciliación, 
# MAGIC y aplicarlos para alistar las tablas.  

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Subledger (FPSL)

# COMMAND ----------

ldgr_path = get_source_path(ldgr_files, which_files['subledger'])

try: 
    ldgr_src = Sourcer(ldgr_path, **c_layouts.fpsl_specs)
    ldgr_prep = ldgr_src.start_data(spark)
    ldgr_data = ldgr_src.setup_data(ldgr_prep)

    ldgr_data.display()
except:                     # pylint: disable=bare-except
    ldgr_data = None        # pylint: disable=invalid-name


# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Cloud Banking (C4B)

# COMMAND ----------

c4b_path = get_source_path(c4b_files, which_files['cloud-banking'])

try: 
    c4b_src = Sourcer(c4b_path, **c_layouts.c4b_specs)
    c4b_prep = c4b_src.start_data(spark)
    c4b_data = c4b_src.setup_data(c4b_prep)

    c4b_data.display()
except:                 # pylint: disable=bare-except 
    c4b_data = None     # pylint: disable=invalid-name


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
    'c4b':    (F.col( 'c4b_num_txns') == 0) | (F.col( 'c4b_num_txns').isNull()), 
    'fpsl':   (F.col( 'c4b_num_txns') == 0) | (F.col( 'c4b_num_txns').isNull()), 
    'indeterminada': None})

report_036 = Conciliator(ldgr_src, c4b_src, check_txns)
base_036   = report_036.base_match(ldgr_data, c4b_data)
diffs_036  = report_036.filter_checks(base_036, '~valida')
fpsl_036   = report_036.filter_checks(base_036, ['fpsl', 'indeterminada'])
c4b_036    = report_036.filter_checks(base_036, ['c4b',  'indeterminada'])

base_036.save_as_file(f"{dir_036}/compare/{r_date}_036_comparativo.csv")
diffs_036.save_as_file(f"{dir_036}/discrepancies/{r_date}_036_discrepancias.csv")
fpsl_036.save_as_file(f"{dir_036}/subledger/{r_date}_036_fpsl.csv")
c4b_036.save_as_file(f"{dir_036}/cloud-banking/{r_date}_036_c4b.csv")

base_036.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Resultados
# MAGIC
# MAGIC Aquí vemos algunos de los resultados claves de le ejecución:  
# MAGIC - Procesos que no se concluyeron.  
# MAGIC - Resumenes de los que sí.  

# COMMAND ----------

for kk, vv in which_files.items(): 
    print(f"{kk}\t:{dumps2(vv)}")

if ldgr_data is None: 
    print(f"No se encontró FPSL correspondiente a {dumps2(which_files['subledger'])}.")

if c4b_data is None: 
    print(f"No se encontró C4B correspondiente a {dumps2(which_files['cloud-banking'])}.")

if base_036 is None: 
    print("No se concluyó el reporte 036.")
    
