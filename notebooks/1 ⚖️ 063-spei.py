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
# MAGIC ## 0. Preparar código

# COMMAND ----------

# MAGIC %run ./0_install_nb_reqs

# COMMAND ----------

# pylint: disable=wrong-import-order,wrong-import-position
from datetime import datetime as dt, timedelta as delta
from operator import add, itemgetter, methodcaller as ϱ
from pytz import timezone as tz

from pyspark.dbutils import DBUtils     # pylint: disable=no-name-in-module,import-error
from pyspark.sql import SparkSession, functions as F
from toolz import compose_left, identity, juxt, pipe

from epic_py.tools import dirfiles_df, partial2
from config import t_agent, t_resourcer, DATALAKE_PATHS as paths
from refs.layouts import conciliations as c_layouts
from src.conciliation import (Sourcer, Conciliator,
    files_matcher, get_source_path, process_files)

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 0.b Especificar fechas

# COMMAND ----------

dbutils.widgets.text('date', 'yyyy-mm-dd')
dbutils.widgets.combobox('c4b_spei', 'C4B5_01', 
    ['S4B1_01', 'S4B1_02', 'C4B5_01', 'C4B5_02'])
dbutils.widgets.text('recoif', '900002')

# COMMAND ----------

t_storage = t_resourcer['storage']
t_permissions = t_agent.prep_dbks_permissions(t_storage, 'gen2')
# t_resourcer.set_dbks_permissions(t_permissions)
<<<<<<< HEAD
=======

>>>>>>> 0d63371b8477e34121abc6fcf4f4329238a0ba36
λ_address = (lambda cc, pp : t_resourcer.get_resource_url(
    'abfss', 'storage', container=cc, blob_path=pp))

at_banking = λ_address('bronze', paths['spei-c4b'])
at_ledger  = λ_address('bronze', paths['spei-gfb'])
to_reports = λ_address('gold',   paths['reports2'])

tmp_parent = compose_left(
    ϱ('split', '/'), itemgetter(slice(0, -1)), 
    partial2(add, ..., ['tmp',]), 
    '/'.join)

c4b_key = dbutils.widgets.get('c4b_spei')
recoif_key = dbutils.widgets.get('recoif')
w_date = dbutils.widgets.get('date')

if w_date == 'yyyy-mm-dd':
    now_mx = dt.now(tz('America/Mexico_City'))      # pylint: disable=invalid-name
    r_date = now_mx.date() - delta(days=1) 
    s_date = r_date.strftime('%Y-%m-%d')
else:
    s_date = w_date
    r_date = dt.strptime(s_date, '%Y-%m-%d')

print(f"""
SPEI-C4B : {at_banking}
SPEI-GFB : {at_ledger}
Reports  : {to_reports}"""[1:])

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1. Esquemas y carpetas

# COMMAND ----------

# MAGIC %md  
# MAGIC
# MAGIC - Las fuentes de datos que utilizamos se alojan en carpetas del _datalake_.  
# MAGIC - Los archivos de las carpetas tienen metadatos en sus nombres, que se extraen en la 
# MAGIC subsección `Regex Carpetas`.  
# MAGIC - Además los archivos consisten de datos tabulares cuyos esquemas se construyen en la 
# MAGIC sección correspondiente.  
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
# MAGIC ## 2. Preparación de fuentes

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 0. SPEI-C4B

# COMMAND ----------

src_0 = 'spei-banking'    # pylint: disable=invalid-name 

pre_files = pipe(at_banking, 
    partial2(dirfiles_df, ..., spark), 
    partial2(process_files, ..., src_0))

(c4b_files, c4b_path, c4b_status) = pipe(pre_files, 
    partial2(files_matcher, ..., dict(date=s_date, key2=c4b_key)))
(c4b_files.query('matcher > 0')
    .reset_index()
    .sort_values(['matcher', 'date', 'modificationTime'], ascending=[False, False, False])
    .loc[:, ['matcher', 'key1', 'date', 'key2', 'modificationTime', 'size', 'name']])

# COMMAND ----------

c4b_src  = Sourcer(c4b_path, **c_layouts.c4b_spei_specs)
c4b_prep = c4b_src.start_data(spark)
c4b_data = c4b_src.setup_data(c4b_prep)
c4b_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### i. SPEI-GFB

# COMMAND ----------

# MAGIC %md 
# MAGIC **Nota**  
# MAGIC + Llamamos _ledger_ a la fuente de transacciones SPEI de Banorte (GFB), 
# MAGIC aunque no sea registro contable.  
# MAGIC   Esta fuente contiene el backlog de las transferencias SPEI.  
# MAGIC + La clave `900002` significa algo muy importante, es el número que nos asigna Banorte.  
# MAGIC + Sobre la hora, en el ejercicio inicial nos da todo `160323`.  
# MAGIC   Esto es por ser SPEI indirecto, pero cambiará cuando tengamos todo en SPEI directo.  

# COMMAND ----------

src_1 = 'spei-ledger'    # pylint: disable=invalid-name 

files_0 = dirfiles_df(at_ledger, spark)
files_1 = process_files(files_0, src_1)
(gfb_files, gfb_path, gfb_status) = pipe(files_1, 
    partial2(files_matcher, ..., dict(date=s_date, key=recoif_key))) 

(gfb_files.query('matcher > 1')
    .reset_index()
    .sort_values(['matcher', 'date', 'modificationTime'], ascending=[False, False, False])
    .loc[:, ['matcher', 'key', 'date', 'modificationTime', 'size', 'name']])

# COMMAND ----------

gfb_src  = Sourcer(gfb_path, **c_layouts.gfb_spei_specs)
gfb_prep = gfb_src.start_data(spark)
gfb_data = gfb_src.setup_data(gfb_prep)

print(gfb_path)
gfb_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reportes y escrituras

# COMMAND ----------

dir_063 = f"{to_reports}/electronic-transfers"

check_txns = {
    'valida': (F.col('gfb_num_txns') == F.col('c4b_num_txns')) 
            & (F.col('gfb_monto'   ) == F.col('c4b_monto')), 
    'gfb'   : (F.col('gfb_num_txns') == 0) | (F.col('gfb_num_txns').isNull()), 
    'c4b'   : (F.col('c4b_num_txns') == 0) | (F.col('c4b_num_txns').isNull()), 
    'indeterminada': None}

report_063 = Conciliator(gfb_src, c4b_src, check_txns)
base_063   = report_063.base_match(gfb_data, c4b_data)
diffs_063  = report_063.filter_checks(base_063, '~valida')
gfb_063    = report_063.filter_checks(base_063, ['gfb', 'indeterminada'])
c4b_063    = report_063.filter_checks(base_063, ['c4b', 'indeterminada'])

two_paths = juxt(identity, tmp_parent)

base_063.save_as_file(*two_paths(f"{dir_063}/compare/{s_date}_063_comparativo.csv"), 
        header=False)
diffs_063.save_as_file(*two_paths(f"{dir_063}/discrepancies/{s_date}_063_discrepancias.csv"), 
        header=False)
gfb_063.save_as_file(*two_paths(f"{dir_063}/subledger/{s_date}_063_spei-gfb.csv"), 
        header=False)
c4b_063.save_as_file(*two_paths(f"{dir_063}/cloud-banking/{s_date}_063_spei-c4b.csv"), 
        header=False)


# COMMAND ----------

base_063.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 4. Resultados
# MAGIC Aquí vemos algunos de los resultados claves de le ejecución:  
# MAGIC - Procesos que no se concluyeron.  
# MAGIC - Resumenes de los que sí.  

# COMMAND ----------

# MAGIC %md 
# MAGIC #### i. Totales de txns validadas. 

# COMMAND ----------

(base_063
    .groupBy('check_key')
    .count()
    .display())

# COMMAND ----------

# MAGIC %md 
# MAGIC #### ii. Revisión de archivos

# COMMAND ----------

check_dir = f"{dir_063}/compare"      # {s_date}_063_comparativo.csv"
(dirfiles_df(check_dir, spark)
    .sort_values('modificationTime', ascending=False))
