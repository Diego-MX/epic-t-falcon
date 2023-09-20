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
# MAGIC ## 0. Preparar código

# COMMAND ----------

# MAGIC %run ./0_install_nb_reqs

# COMMAND ----------

# pylint: disable=wrong-import-order,wrong-import-position
from datetime import datetime as dt, timedelta as delta
from pytz import timezone as tz

from pyspark.dbutils import DBUtils     # pylint: disable=no-name-in-module,import-error
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 0.b Especificar fechas

# COMMAND ----------

now_mx = dt.now(tz('America/Mexico_City'))      # pylint: disable=invalid-name
yday = now_mx.date() - delta(days=1) 
dbutils.widgets.text('date', yday.strftime('%Y-%m-%d'))
dbutils.widgets.text('c4b_spei', 'C4B5_01')
dbutils.widgets.text('recoif', '900002')

# COMMAND ----------

r_date = dt.strptime(dbutils.widgets.get('date'), '%Y-%m-%d')
c4b_key = dbutils.widgets.get('c4b_spei')
recoif_key = dbutils.widgets.get('recoif')

which_files = {
    'cloud-banking': dict(date=r_date, key=c4b_key),  
    'subledger'    : dict(date=r_date, key=recoif_key)} 

# COMMAND ----------

from importlib import reload
import epic_py; reload(epic_py)     # pylint: disable=multiple-statements
import src; reload(src)     # pylint: disable=multiple-statements
import config; reload(config)     # pylint: disable=multiple-statements

from epic_py.tools import dirfiles_df
from src.sftp_sources import process_files
from src.conciliation import Sourcer, Conciliator, get_source_path

from config import t_agent, t_resourcer, DATALAKE_PATHS as paths
from refs.layouts import conciliations as c_layouts

t_storage = t_resourcer['storage']
t_permissions = t_agent.prep_dbks_permissions(t_storage, 'gen2')
t_resourcer.set_dbks_permissions(t_permissions)

λ_address = (lambda cc, pp : t_resourcer.get_resource_url(
    'abfss', 'storage', container=cc, blob_path=pp))

at_banking = λ_address('bronze', paths['spei-c4b'])
at_ledger  = λ_address('bronze', paths['spei-gfb'])
to_reports = λ_address('gold',   paths['reports2'])

print(f"""
    SPEI-C4B : {at_banking}
    SPEI-GFB : {at_ledger}
    Reports  : {to_reports}
""")


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
# MAGIC #### Manipulación de columnas

# COMMAND ----------
# MAGIC %md
# MAGIC #### c. SPEI-GFB

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

since_when = r_date - delta(5)

pre_files_1   = dirfiles_df(at_ledger, spark)
speigfb_files = process_files(pre_files_1, 'spei-ledger')
print(which_files['spei-ledger'])


# COMMAND ----------

# MAGIC %md 
# MAGIC #### d. SPEI-C4B

# COMMAND ----------

# MAGIC %md
# MAGIC Tenemos varias carpetas con datos de SPEI, 
# MAGIC + La más correcto y menos incorrecta es  
# MAGIC   `spei-c4b: "ops/core-banking/conciliations/spei"`.   
# MAGIC + Se intentó usar `spei2: "ops/fraude/bronze/spei"`, pero no jala tan bien.  
# MAGIC   Esta última tiene archivos con las siguientes llaves.  
# MAGIC | key1        | key2   | | key1        | key2   |
# MAGIC |-------------|--------|-|-------------|--------|
# MAGIC |CONCILIACION |      01| |DATALAKE     |        |
# MAGIC |CONCILIACION |      02| |DATALAKE     |        |
# MAGIC |CONCILIACION | C4B6_01| |DATALAKE     | C4B6   |
# MAGIC |             |        | |DATALAKE     | CB11   |
# MAGIC |CONCILIACION | CB12_01| |DATALAKE     | CB12   |
# MAGIC |             |        | |DATALAKE     | CB13   |
# MAGIC |CONCILIACION | CB14_01| |DATALAKE     | CB14   |
# MAGIC |CONCILIACION | S4B1_01| |DATALAKE     | S4B1   |
# MAGIC |CONCILIACION | S4B1_02| |DATALAKE     | S4B1*  |
# MAGIC
# MAGIC `*`: Se repite la llave2 de acuerdo al match `CONCILIACION-DATALAKE`. 

# COMMAND ----------

# MAGIC %md 
# MAGIC **Notas**  
# MAGIC + Los archivos para conciliación dicen `CONCILIACION`, no `DATALAKE`.  
# MAGIC + La llave para estos archivos es: `S4B1`.  
# MAGIC + Cuando se ejecute el SPEI, llegará la llave `C4B2` ... para Akya.  
# MAGIC + Revisar ruta de archivos:  `<bronze>/ops/core-banking/conciliations/spei`.  

# COMMAND ----------

print(at_banking)
pre_files_1 = dirfiles_df(at_banking, spark)
pre_files_0 = process_files(pre_files_1, 'spei-banking')
speic4b_files = pre_files_0.loc[pre_files_0['key1'] == 'CONCILIACION']
print(which_files['spei-banking'])
pre_files_0.sort_values('date', ascending=False)  #.query(f"date >= '{since_when}'")

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
# MAGIC ### c. SPEI-GFB

# COMMAND ----------

speigfb_files.sort_values('date', ascending=False)

# COMMAND ----------

gfb_path = get_source_path(speigfb_files, which_files['spei-ledger'])

gfb_src  = Sourcer(gfb_path, **c_layouts.gfb_spei_specs)
gfb_prep = gfb_src.start_data(spark)
gfb_data = gfb_src.setup_data(gfb_prep)

print(gfb_path)
gfb_data.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### d. SPEI-C4B

# COMMAND ----------

c4b_path = get_source_path(speic4b_files, which_files['spei-banking'])

c4b_src  = Sourcer(c4b_path, **c_layouts.c4b_spei_specs)
c4b_prep = c4b_src.start_data(spark)
c4b_data = c4b_src.setup_data(c4b_prep)
c4b_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reportes y escrituras

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. (063) SPEI

# COMMAND ----------

dir_063 = f"{to_reports}/electronic-transfers/"

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

base_063.save_as_file(f"{dir_063}/compare/{r_date}_063_comparativo.csv")
diffs_063.save_as_file(f"{dir_063}/discrepancies/{r_date}_063_discrepancias.csv")
gfb_063.save_as_file(f"{dir_063}/subledger/{r_date}_063_spei-gfb.csv")
c4b_063.save_as_file(f"{dir_063}/cloud-banking/{r_date}_063_spei-c4b.csv")


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
    print(f"{kk}\t~ {dumps2(vv)}")

if gfb_data is None: 
    print(f"No se encontró SPEI-GFB correspondiente a {dumps2(which_files['spei-ledger'])}.")

if c4b_data is None: 
    print(f"No se encontró SPEI-C4B correspondiente a {dumps2(which_files['spei-banking'])}.")

if base_063 is None: 
    print("No se concluyó el reporte 063.")
