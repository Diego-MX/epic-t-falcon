# Databricks notebook source
# MAGIC %md 
# MAGIC # Introducción
# MAGIC
# MAGIC En este reporte veremos el proceso de la aplicación de comisiones por retiros de cajeros.  
# MAGIC Como descripción, las comisiones se aplican sólo a ciertos retiros de cajeros:  aquellos que exedieron el número permitido mensual en cajeros propios de la red (_inhouse_), o bien que se realizaron en cajeros externos a la red.  
# MAGIC La fuente de estos retiros a los que les aplicamos una comisión son los archivos ATPT que nos proporciona Fiserv, el sistema de manejo de tarjetas.  
# MAGIC Estos ATPTs tienen todas las transacciones de las cuentas -retiros y no retiros-.  Entonces los pasos para las comisiones son los siguientes:  
# MAGIC &ensp;0. Leer el/los archivos ATPTs como tabla.  
# MAGIC 1. Identificar las transacciones que corresponden a retiros de ATM.  
# MAGIC 2. Identificar los retiros que son susceptibles de una comisión.  
# MAGIC 3. Aplicar la comisión de esos retiros _comisionables_.  
# MAGIC 4. Rectificar que la comisión de los retiros se aplicó exitosamente.  
# MAGIC
# MAGIC En la última reunión del equipo se discutieron resultados sobre la aplicación de comisiones en tiempos de pruebas.   
# MAGIC El enfoque de aplicación de pruebas no era del todo compatible con el enfoque de desarrollo en camino a la puesta en producción.  
# MAGIC Mientras que las pruebas se realizan de manera local, con tomas de pantalla y archivos compartidos por correo; los ambientes de desarrollo no admiten archivos personales, y se ejecutan en los servidores de la nube.  
# MAGIC
# MAGIC A continuación veremos la aplicación de comisiones a los retiros de pruebas, a su vez que describimos el proceso general de comisiones en el ambiente de nube.  

# COMMAND ----------

# MAGIC %md 
# MAGIC # 0. Preparación de archivos y lectura. 
# MAGIC
# MAGIC Para las pruebas utilizamos el archivo `ZATPTX01_EPIC_UAX_11012023.txt`.   
# MAGIC Aunque parecido, es de resaltar que el formato de nombre es distinto a los que recibimos regularmente, ej. `UAT_TRXS_ZATPTX01_2023-01-31.ZIP`, o bien `ZATPTX01_2023-01-31` al descomprimirlo. 
# MAGIC
# MAGIC Preparamos carpetas donde se leen los directorios y mostramos la tabla correspondiente al archivo de pruebas. 
# MAGIC
# MAGIC

# COMMAND ----------

from collections import OrderedDict
import pandas as pd
from pyspark.sql import functions as F, Window as W, types as T

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------

from epic_py.delta import EpicDF

from src import sftp_sources as sftp_src
from src.core_banking import SAPSession
from config import (ConfigEnviron, 
    ENV, SERVER, RESOURCE_SETUP,  
    DATALAKE_PATHS as paths)


app_environ = ConfigEnviron(ENV, SERVER, spark)
core_starter = app_environ.prepare_coresession('qas-sap')
core_session = SAPSession(core_starter)

# COMMAND ----------

local_layouts = "/dbfs/FileStore/transformation-layer/layouts"
spk_test = 'dbfs:/FileStore/transformation-layer/samples/ZATPTX01_EPIC_UAX_11012023.txt'

atpt_srcr = sftp_src.prepare_sourcer('atpt', local_layouts)
pre_test  = sftp_src.read_delta_basic(spark, spk_test)
ref_test  = sftp_src.delta_with_sourcer(pre_test, atpt_srcr)

ref_test.display()


# COMMAND ----------

# MAGIC %md 
# MAGIC Por su parte la tabla completa de ATPTs, es decir de todos los archivos compartidos, se visualiza brevemente a continuación. 

# COMMAND ----------

resources = RESOURCE_SETUP[ENV]
app_environ = ConfigEnviron(ENV, SERVER, spark)
app_environ.sparktransfer_credential()

slv_path    = paths['abfss'].format('silver', resources['storage'])
at_datasets = f"{slv_path}/{paths['datasets']}"  
atptx_loc   = f"{at_datasets}/atpt/delta"

ref_atpt = EpicDF(spark, atptx_loc)
ref_atpt.display()


# COMMAND ----------

print(f"""
Total de filas: \t{ref_atpt.count()}
Num de columnas: \t{len(ref_atpt.columns)}
Diferentes ATPT_ACCT: \t{ref_atpt.select('atpt_acct').distinct().count()}
Diferentes ATPT_MT_REF_NBR: \t\t{ref_atpt.select('atpt_mt_ref_nbr').distinct().count()}
Diferentes ATPT_MT_INTERCHG_REF: \t{ref_atpt.select('atpt_mt_interchg_ref').distinct().count()}
""")

# COMMAND ----------

# MAGIC %md 
# MAGIC # 1. Transacciones individuales y transacciones de retiros
# MAGIC Las primeras dos dudas sobre la calidad de los datos son las siguientes:  
# MAGIC - ¿Al tomar diferentes archivos ATPTs, es cierto que las transacciones no se repiten?  
# MAGIC - ¿Podemos tomar la identificación de transacciones como retiros a partir del campo `atpt_mt_category_code` como se indica en la historia de usuario?  
# MAGIC Creemos que ambas preguntas tienen respuesta negativa, pero aprovechamos para indicar un poco de detalle al respecto.   
# MAGIC - Para las transacciones repetidas, utilizamos los campos `atpt_mt_ref_nbr` y `atpt_mt_interchg_ref` esperando que sean identificadores únicos de las transacciones, pero obtenemos repetidos.  
# MAGIC - En la historia de usuario se definieron los valores `6010`, `6011` como identificadores de retiros.  Pero tras una observación del área funcional se determinó que no es así.  

# COMMAND ----------

w_txn_ref = (W.partitionBy('atpt_mt_ref_nbr', 'atpt_mt_interchg_ref')
    .orderBy(F.col('atpt_mt_eff_date').desc()))

prepare_cmsns = (ref_atpt
    .withColumn('b_rk_txns', F.row_number().over(w_txn_ref)))

max_ranks = (prepare_cmsns
    .agg({'b_rk_txns': 'max'})
    .collect()[0][0])

unique_cmsns = (prepare_cmsns
    .filter(F.col('b_rk_txns') == 1)
    .filter(F.col('atpt_mt_category_code').isin([6010, 6011])))

print(f"""
Max txns por identificadores:\t{max_ranks} (>> 1!!)
Número de 'comisiones':\t\t{unique_cmsns.count()}""")

unique_cmsns.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC En la tabla de pruebas dado que sólo hay 8 transacciones se pueden inspeccionar las columnas correspondientes.  
# MAGIC En `atpt_mt_interchg_ref` hay un valor repetido.  
# MAGIC En `atpt_mt_category_code` todas son `6011`.  
# MAGIC En `atpt_mt_ref_nbr` no hay repetidas. 

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Retiros comisionables 
# MAGIC
# MAGIC Se mencionaron las características para cobrar comisiones a un retiro:  que supere el límite de retiros mensual para cajeros _inhouse_ de la red propia, o que se efectue en cajeros externos.    
# MAGIC
# MAGIC Entonces calculamos las siguientes variables:  
# MAGIC - `w_month` el mes de la transacción de acuerdo a `atpt_mt_eff_date`
# MAGIC - `w_acq_code` la clave de banco adquirente de acuerdo a `atpt_mt_interchg_ref`  
# MAGIC - `w_is_inhouse` si la clave anterior corresponde al banco Banorte (11072)  
# MAGIC - `w_rk_acct` el número de retiro en el mes agrupado por cuenta  
# MAGIC - `w_rk_inhouse`  el número de retiro en cajeros de la red propio (también por cuenta)  
# MAGIC - `w_is_commissionable` tecnicamente `w_rk_inhouse > 3` o `NOT(w_is_inhouse)`. 
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

purchase_to_savings = (lambda a_col: 
    F.concat_ws('-', F.substring(a_col, 1, 11), F.substring(a_col, 12, 3), F.lit('MX')))
   
w_month_acct = (W.partitionBy('atpt_acct', 'b_wdraw_month')
    .orderBy(F.col('atpt_mt_eff_date').desc()))

w_inhouse    = (W.partitionBy('atpt_acct', 'b_wdraw_month', 'b_wdraw_is_inhouse')
    .orderBy(F.col('atpt_mt_eff_date').desc()))

wdraw_withcols = OrderedDict({
    'b_core_acct'        : purchase_to_savings('atpt_mt_purchase_order_nbr'),
    'b_wdraw_month'      : F.date_trunc('month', F.col('atpt_mt_eff_date')).cast(T.DateType()), 
    'b_wdraw_acq_code'   : F.substring(F.col('atpt_mt_interchg_ref'), 2, 6), 
    'b_wdraw_is_inhouse' : F.col('b_wdraw_acq_code') == 11072,
    'b_wdraw_rk_acct'    : F.row_number().over(w_month_acct), 
    'b_wdraw_rk_inhouse' : F.when(F.col('b_wdraw_is_inhouse'), 
            F.row_number().over(w_inhouse)).otherwise(-1), 
    'b_wdraw_is_commissionable': ~F.col('b_wdraw_is_inhouse') | (F.col('b_wdraw_rk_inhouse') > 3)
})


final_cmsns = (unique_cmsns
    .with_column_plus(wdraw_withcols)
    .filter(F.col('b_wdraw_is_commissionable')))

test_cmsns = (ref_test
    .with_column_plus(wdraw_withcols)
    .filter(F.col('b_wdraw_is_commissionable')))

print(f"""
Admiten comisión: {final_cmsns.count()}
En test: {test_cmsns.count()}
""")

final_cmsns.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 3. Fees application
# MAGIC Create a SAP-session object to apply the transactions, and then call the corresponding API.  

# COMMAND ----------

run_it = False
if run_it: 
    responses = core_session.process_commissions_atpt(spark, 
        test_cmsns, 'atm', update=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 4. Fees verification

# COMMAND ----------

f_args = {"ExternalID": "a1443d87-643c-44ba-9bb5-9c62663f3005"}
verify_1 = core_session.verify_commissions_atpt(**f_args)
spark.createDataFrame(pd.DataFrame(verify_1)).display()
