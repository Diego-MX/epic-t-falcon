# Databricks notebook source
# MAGIC %md 
# MAGIC # Introducción
# MAGIC
# MAGIC Este _notebook_ es para ejecutarse como _job_ de manejo de comisiones (de retiros).  
# MAGIC Potencialmente gestionaría retiros en general, u otras comisiones.  
# MAGIC Consta de los siguientes secciones:  
# MAGIC &nbsp; 0. Leer las transacciones de ATPT de acuerdo con el sistema de tarjetas Fiserv.  
# MAGIC 1. Identificar las txns correspondientes a retiros de cajero, y los estatus de comisiones.  
# MAGIC 2. Comparar con las comisiones existentes 
# MAGIC 3. Aplicar las tarifas correspondientes.  
# MAGIC 4. Actualizar la tabla de comisiones.  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explicación y puntos de fricción  
# MAGIC
# MAGIC - La tabla de transacciones ATPT se conforma de archivos enviados diariamente.   
# MAGIC   Se asume que cada todas las transacciones se almacenan en los archivos, y que cada una pertenece a sólo un archivo.  
# MAGIC   ... peeeero eso no se ha verificado.  
# MAGIC
# MAGIC - La tabla de comisiones (registradas) se crea a partir de las respuestas de la API de SAP.  
# MAGIC   
# MAGIC - La API de comisiones requiere el número de cuenta en formato SAP, la tabla de transacciones lo tiene en formato Fiserv.  
# MAGIC   La traducción de una a la otra se hacía originalmente mediante la tabla `dambs` de Fiserv, pero ahora por medio de la columna `atpt_mt_purchase_order_nbr`.  
# MAGIC   
# MAGIC - Además del número de cuenta, la traducción de una entrada en `atpt` y luego como retiro de ATM, utiliza la clase de objetos tipo Python `core_models.Fee` y `core_models.FeeSet`.  
# MAGIC   Se definieron a partir de la API. 
# MAGIC   El _swagger_ correspondiente se puede acceder [desde fiori][fiori] o [directo en código JSON][json].  
# MAGIC   Para la segunda referencia se puede utilizar [este visualizador en la web][web-editor], o con herramientas de VS-Code. 
# MAGIC   
# MAGIC
# MAGIC Por el momento, eso es todo.  
# MAGIC
# MAGIC [json]: https://apidev.apimanagement.us21.hana.ondemand.com/s4b/v1/oapi/oAPIDefinitionSet('000D3A57CECB1EED869D37419484D2F90002610')/$value
# MAGIC [fiori]: https://qas-c4b-bdp.launchpad.cfapps.us21.hana.ondemand.com/site/c4b#C4BOpenAPIDirectory-Display?sap-ui-app-id-hint=saas_approuter_c4b.openAPI.baobcoapi00&/APIServiceSet/000D3A57CECB1EED869D37419484D2F90002610
# MAGIC [web-editor]: https://editor.swagger.io/

# COMMAND ----------

# MAGIC %md 
# MAGIC # Coding

# COMMAND ----------

from src.setup import dependencies as deps 
deps.from_reqsfile('../reqs_dbks.txt')
deps.gh_epicpy('meetme-1', '../user_databricks.yml', typing=False, verbose=True)

# COMMAND ----------

# Algunas constantes de negocio.  
COMSNS_FRAME = 300   # Max número de días para cobrar comisiones. 
COMSNS_APPLY = 100   # Max número de comisiones para mandar en un llamado. 
PAGE_MAX     = 200   # Max número de registros (eg. PersonSet) para pedir de un llamado. 

DEBUG = False
PAST_MISSED = True

# pylint: disable=wrong-import-position
# pylint: disable=wrong-import-order
# pylint: disable=multiple-statements

# COMMAND ----------

from collections import OrderedDict
from datetime import datetime as dt, timedelta as delta
from functools import reduce
from itertools import chain
from operator import methodcaller as ϱ, or_
from warnings import warn

from delta.tables import DeltaTable as Δ
import pandas as pd
from pyspark.sql import functions as F, SparkSession, types as T, utils, Window as W
from pytz import timezone
from toolz import pipe

spark = SparkSession.builder.getOrCreate()

from importlib import reload
import epic_py; reload(epic_py)
import config ; reload(config)

from epic_py.delta import EpicDF
from epic_py.partners.core import SAPSession

from config import t_agent, t_resourcer, t_core, BLOB_PATHS as paths

(datalake, lake_type) = (t_resourcer['storage'], 'gen2')
permissions = t_agent.prep_dbks_permissions(datalake, lake_type)
t_resourcer.set_dbks_permissions(permissions)

core_session = SAPSession(t_core)

slv_path = t_resourcer.get_resource_url('abfss', datalake, container='silver') 
at_datasets = f"{slv_path}/{paths['cms-data']}"  
at_commissions = f"{slv_path}/{paths['commissions']}/delta"

atptx_loc = f"{at_datasets}/atpt/delta"


# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1. Update Previous Commissions
# MAGIC
# MAGIC Estatus en comisiones:   
# MAGIC `'0.0' : '0.0'`    
# MAGIC ` '1'  : 'Creado'`     
# MAGIC ` '2'  : 'Procesado'`  
# MAGIC ` '3'  : 'No procesado'`  

# COMMAND ----------

def update_commissions(cmsn_api:pd.DataFrame, at_commissions, cmsn_df=None): 
    # Check that is non-empty cmsns_df with
    #     acct_id, ext_id, stats = Fees Uploaded for Postprocessi
    #    (cmsn_df, cmsn_api) = sub_comsns, api_comsns
    
    if (cmsn_df is not None) and (cmsn_api.shape[0] != cmsn_df.count()): 
        print(f"Corresponding Delta and API size don't match.")
        #raise Exception(f"Corresponding Delta and API size don't match.")
    
    if (cmsn_df is not None) and (1 != cmsn_df.select('transaction_id').distinct().count()):
        print(f"There are repeated transactions in the given commission and account.")
        #raise Exception(f"There are repeated transactions in the given commission and account.")
    
    cmsn_api = spark.createDataFrame(cmsn_api)
    
    on_str = " AND ".join(f"(t1.`{col}` = t0.`{col}`)" 
        for col in ['pos_fee', 'external_id', 'account_id'])
    
    set_update = {col: f't1.`{col}`' for col in [
        'amount', 'status_process', 'status_descr', 'log_msg']}
    
    insert_vals = {col: f't1.{col}' for col in cmsn_api.columns}
    
    (Δ.forPath(spark, at_commissions).alias('t0')
        .merge(cmsn_api.alias('t1'), on_str)
        .whenMatchedUpdate(set=set_update)
        .whenNotMatchedInsert(values=insert_vals)
        .execute())
    return

# COMMAND ----------

unproc_comsns = (spark.read
    .load(at_commissions)
    .filter(~F.col('status_process').isin(['2', '3'])))

unproc_ids = (unproc_comsns
    .select('external_id')
    .distinct()
    .collect())

summaries = {
    'n_commissions': F.count('transaction_id'), 
    'n_txns'       : F.countDistinct('transaction_id'), 
    'n_feecalls'   : F.countDistinct('external_id')}

print(f"There are {len(unproc_ids)} external IDs not processed.")
(EpicDF(spark, at_commissions)
    .groupBy('status_process', 'status_descr')
    .agg_plus(summaries)
    .orderBy('status_process')
    .show())

# COMMAND ----------

if DEBUG: 
    for ii, id_row in enumerate(unproc_ids):
        print(ii, id_row.external_id)
        sub_comsns = (EpicDF(spark, at_commissions)
            .filter(F.col('external_id') == id_row.external_id))
        
        api_comsns = (core_session
            .verify_commissions(ExternalID=id_row.external_id))
        
        if api_comsns.shape[0] == 0: 
            print(id_row.b_core_acct)
            continue
            
        update_commissions(api_comsns, at_commissions, sub_comsns)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Withdrawals Table
# MAGIC Starting from the transactions universe, consider the ones that are withdrawals.  
# MAGIC Prepare the corresponging attributes to manage them.  

# COMMAND ----------

# MAGIC %md 
# MAGIC Ejemplo de retiro para comprobar que está en ATPT.  
# MAGIC - `Cuenta`:  `02020000967`
# MAGIC
# MAGIC **Nota** Los ATPTs no distinguen diferentes BPAs.   
# MAGIC O sea, pueden venir cuentas y txns de diferentes de ellos en un mismo archivo. 

# COMMAND ----------

# atpt_mt_category_code: {
#    5045, 5311, 5411, 5661, 7994
#    6010: cash, 6011: atm, 
#    (596[02456789], 7995): high-risk-merchant}

# b_wdraw_acquirer_code:  {110072: Banorte, ...}

purchase_to_savings = (lambda a_col: 
    F.concat_ws('-', F.substring(a_col, 1, 11), 
                     F.substring(a_col, 12, 3), 
                     F.lit('MX')))

w_txn_ref =    (W.partitionBy('atpt_mt_ref_nbr')
    .orderBy(F.col('atpt_mt_eff_date').desc()))
w_month_acct = (W.partitionBy('atpt_acct', 'b_wdraw_month')
    .orderBy(F.col('atpt_mt_eff_date').desc()))
w_inhouse    = (W.partitionBy('atpt_acct', 'b_wdraw_month', 'b_wdraw_is_inhouse')
    .orderBy(F.col('atpt_mt_eff_date').desc()))

wdraw_withcols = [{
    'b_core_acct'        : purchase_to_savings('atpt_mt_purchase_order_nbr'), 
    'b_wdraw_acq_code'   : F.substring(F.col('atpt_mt_interchg_ref'), 2, 6),
    'b_wdraw_rk_txns'    : F.row_number().over(w_txn_ref),
    'b_wdraw_month'      : F.date_trunc('month', F.col('atpt_mt_eff_date')).cast(T.DateType())
    }, {   
    'b_wdraw_rk_acct'    : F.row_number().over(w_month_acct),
    'b_wdraw_is_inhouse' : F.col('b_wdraw_acq_code') == F.lit('11072'),
    }, {
    'b_wdraw_rk_inhouse' : F.when(F.col('b_wdraw_is_inhouse'), 
        F.row_number().over(w_inhouse)).otherwise(-1)
    }, { 
    'b_wdraw_is_commissionable': ~F.col('b_wdraw_is_inhouse') 
            | (F.col('b_wdraw_rk_inhouse') > 3)
    }, {  
    'b_wdraw_commission_status':  (F.when(~F.col('b_wdraw_is_commissionable'), -1)
        .when(F.col('atpt_mt_posting_date').isNull(), -2)
        .otherwise(0))}] 
    
wdraw_allcols = ['atpt_acct', 'atpt_mt_eff_date', 'atpt_mt_category_code',  
    'atpt_mt_card_nbr', 'atpt_mt_desc', 'atpt_mt_amount', 'atpt_mt_purchase_order_nbr', 
    'atpt_mt_posting_date', 'atpt_mt_ref_nbr', 'atpt_mt_interchg_ref'
    ] + list(chain(*wdraw_withcols))

wdraw_txns = (EpicDF(spark, atptx_loc)
    .filter(F.col('atpt_mt_category_code').isin([6010, 6011]))
    .with_column_plus(wdraw_withcols))

wdraw_txns.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC `ATPT`: es la clave utiliza Fiserv -el sistema de tarjetas CMS- para sus archivos de txns;  equivalente al `Transaction Set` de SAP. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Commissions
# MAGIC From `withdraw_txns` (current) filter those that are subject to a transaction fee.  
# MAGIC Also consider the previously processed ones in `withdrawals`, so that they aren't charged double.  

# COMMAND ----------

# Obtener las comisiones cobrables.  
now_mx = dt.now(tz=timezone('America/Mexico_City'))
today_date = now_mx.date()

# since_date = today_date - delta(days=COMSNS_FRAME)
since_date = dt(2023, 11, 15).date()

pre_commissions = (spark.read
    .load(at_commissions)
    .withColumnRenamed('transaction_id', 'atpt_mt_ref_nbr'))

friendly_cols = ['atpt_acct', 'atpt_mt_eff_date', 'atpt_mt_posting_date', 
    'atpt_mt_ref_nbr', 'atpt_mt_desc', 'atpt_mt_card_nbr', 'atpt_mt_interchg_ref',
    'b_wdraw_is_commissionable', 'b_wdraw_commission_status', 'b_wdraw_rk_txns', 
    'b_core_acct', 'b_wdraw_acq_code', 'b_wdraw_month', 'b_wdraw_rk_acct', 
    'b_wdraw_is_inhouse', 'b_wdraw_rk_inhouse']

_commissions = (wdraw_txns
    .filter_plus(*[
        F.col('b_wdraw_is_commissionable'), 
        F.col('b_wdraw_commission_status') == 0, 
        F.col('b_wdraw_rk_txns') == 1, 
        F.col('atpt_mt_posting_date') >= since_date])
    .select(friendly_cols))

commissions = (_commissions if not PAST_MISSED 
                else _commissions.join(pre_commissions, how='anti', on='atpt_mt_ref_nbr') )
    
commissions.display()


# COMMAND ----------

if DEBUG: 
    (pre_commissions
        .join(commissions, how='semi', on='atpt_mt_ref_nbr')
        .display())


# COMMAND ----------

# MAGIC %md 
# MAGIC ## 3. Fees application
# MAGIC Create a SAP-session object to apply the transactions, and then call the corresponding API.  

# COMMAND ----------

process_df = (core_session
    .process_commissions_atpt(commissions, 
           cmsn_key='atm', out='response', **{'how-many': 50}))

# COMMAND ----------

if DEBUG: 
    process_df.text

# COMMAND ----------

# Inicialmente necesita 
#     'atpt_mt_ref_nbr': 'transaction_id'; 
#     'b_core_acct': 'account_id'. 
# Y genera:   'process_date', 'external_id'; 
#     'type_code', 'currency', 'payment_note', 'posting_date', 'value_date'
#     'status' 
# Y se le agregan: VERFY_COLS

verfy_cols = ['status_process', 'status_descr', 'log_msg']

process_df = (core_session
    .process_commissions_atpt(commissions, 
           cmsn_key='atm', out='dataframe', **{'how-many': 50})
    .assign(**dict.fromkeys(verfy_cols)))

# COMMAND ----------

if not process_df.empty: 
    process_spk = spark.createDataFrame(process_df)
    (process_spk.write
        .mode('append')
        .save(at_commissions))
    process_spk.display()
else: 
    print(f"Commissions DF is empty.")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Manage Tables

# COMMAND ----------

# Iniciar la tabla. 
# debug = False
# if debug: 
#     comisiones.write.save(at_commissions)

# COMMAND ----------

# Borrar la tabla.
# debug = False
# if debug: 
#     dbutils.fs.rm(at_commissions, True)

# COMMAND ----------

# Recalculate Commissions Table
if DEBUG: 
    on_accounts = wdraw_txns.select('b_core_acct').distinct().collect()
    for a_row in on_accounts: 
        if a_row.b_core_acct == '000--MX': 
            continue 
        print(a_row.b_core_acct)

        sub_comsns = (spark.read.load(at_commissions)
            .filter(F.col('account_id') == a_row.b_core_acct))
        api_comsns = (core_session
            .verify_commissions(AccountID=a_row.b_core_acct))

        if api_comsns.shape[0] == 0: 
            print(a_row.b_core_acct)
            continue
        update_commissions(at_commissions, sub_comsns, api_comsns)


# COMMAND ----------

# Alter Table 
if DEBUG:  
    a_df = (spark.read
        .load(at_commissions)  # convert r"STATUS(_DESCR|_PROCESS)?" to STRINGTYPE. 
        .withColumn('status', F.col('status').cast(T.StringType()))
        .withColumn('status_descr', F.col('status_descr').cast(T.StringType()))
        .withColumn('status_process', F.col('status_process').cast(T.StringType()))
        .withColumn('log_msg', F.col('log_msg').cast(T.StringType())))
    (a_df.write
        .mode('overwrite')
        .option('overwriteSchema', True)
        .save(at_commissions))


# COMMAND ----------

# MAGIC %md 
# MAGIC # Pruebas 
# MAGIC
# MAGIC Pruebas nivel medio técnico.  
# MAGIC 1.  Los retiros que se hicieron 'funcionalmente' aparecen en el archivo ATPT.  
# MAGIC 2.  Los retiros que aparecen en ATPT, se clasificaron como `comisionables`/`no-comisionables`.  
# MAGIC 3.  Los retiros `comisionables` se les aplicó una comisión, y se ve reflejada en el núcleo bancario SAP. 
# MAGIC 4.  Se actualizó el estatus de la comisión, como cobrada en las tablas del Δ-lake. 
# MAGIC
# MAGIC
# MAGIC Pruebas funcionales.  
# MAGIC 1.  Sr. A. recibió una notificación después de hacer un retiro del cajero.    
# MAGIC     a) La notificación decía lo que tiene que decir.   
# MAGIC     b) (La notificación llegó a una hora apropiada, y no a las 12AM cuando se procesó por el sistema)  
# MAGIC     c) **Caso extremo**, Si L.A. hizo muchas retiros en un día, recibió una sóla notificación con información de las 3 txns.  
