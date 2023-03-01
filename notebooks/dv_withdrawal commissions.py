# Databricks notebook source
# MAGIC %md 
# MAGIC # Introducción
# MAGIC 
# MAGIC Este _notebook_ es para ejecutarse como _job_ de manejo de comisiones (de retiros).  
# MAGIC Potencialmente podrá gestionar retiros en general, u otras comisiones.  
# MAGIC Consta de los siguientes secciones:  
# MAGIC &nbsp; 0. Leer las transacciones de ATPT de acuerdo con el sistema de tarjetas Fiserv.  
# MAGIC 1. Identificar las txns correspondientes a retiros de cajero, y los estatus de comisiones.  
# MAGIC 2. Comparar con las comisiones existentes 
# MAGIC 3. Aplicar las tarifas correspondientes.  
# MAGIC 4. Actualizar la tabla de comisiones correspondiente.  

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

# MAGIC %md 
# MAGIC ## 0. Ingredients
# MAGIC Prepare libraries, functions and variables.  
# MAGIC Some parameters, are to be set on the functional side.  
# MAGIC ... 

# COMMAND ----------

# MAGIC %pip install -r ../reqs_dbks.txt

# COMMAND ----------

# Algunas constantes de negocio.  
COMSNS_FRAME = 300   # Max número de días para cobrar comisiones. 
COMSNS_APPLY = 100   # Max número de comisiones para mandar en un llamado. 
PAGE_MAX     = 200   # Max número de registros (eg. PersonSet) para pedir de un llamado. 


# COMMAND ----------

from collections import OrderedDict
from datetime import datetime as dt, date, timedelta as delta
from delta.tables import DeltaTable as Δ
from functools import reduce
import pandas as pd
from pandas import DataFrame as pd_DF, Series as pd_S
from pyspark.sql import (DataFrame as spk_DF, 
    functions as F, types as T, Window as W)
from pytz import timezone

# COMMAND ----------

# Para desbichar el código, este bloque que actualiza los módulos de importación/modificación.  
# A veces también se encuentra como: 
#>> %load_ext autoreload
#>> %autoreload 2

from importlib import reload
from src import core_banking; reload(core_banking)
from src import tools, utilities; reload(tools); reload(utilities)
import config; reload(config)

# COMMAND ----------

from src import tools, utilities as src_utils
from src.core_banking import SAPSession
from config import (ConfigEnviron, 
    ENV, SERVER, RESOURCE_SETUP,  
    DATALAKE_PATHS as paths)

resources = RESOURCE_SETUP[ENV]
app_environ = ConfigEnviron(ENV, SERVER, spark)
app_environ.sparktransfer_credential()

slv_path       = paths['abfss'].format('silver', resources['storage'])
at_datasets    = f"{slv_path}/{paths['datasets']}"  
at_withdrawals = f"{slv_path}/{paths['withdrawals']}/delta"

atptx_loc      = f"{at_datasets}/atpt/delta"
dambs_loc      = f"{at_datasets}/dambs/delta"


# COMMAND ----------

print(dambs_loc)
spark.read.load(dambs_loc).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 1. Withdrawals Table
# MAGIC Starting from the transactions universe, consider the ones that are withdrawals.  
# MAGIC Prepare the corresponging attributes to manage them.  

# COMMAND ----------

# Preparamos las transacciones que corresponden a los retiros (withdrawals/wdraw).  

# Alguna información complementaria: 

# b_wdraw_acquirer_code:  {110072: Banorte, ...}
# b_wdraw_commission_status: {
#     -1: not-commissionable
#     -2: not-posted (via date)
#      0: not-applied
#      1: sent to commission
#      2: applies}

# atpt_mt_category_code: {
#    5045, 5311, 5411, 5661, 7994
#    6010: cash, 6011: atm, 
#    (596[02456789], 7995): high-risk-merchant}

spk_types = {
    'str': T.StringType, 'datetime': T.TimestampType, 'dbl': T.DecimalType}
  
sap_resp_cols = {
    #'fee_account_id'      : 'str',
    #'fee_txn_ref_number'  : 'str',   
    #'feemass_status'      : 'str'
    'fee_type_code'        : 'str',       
    'fee_posting_date'     : 'datetime', 
    'fee_value_date'       : 'datetime',
    'fee_amount'           : 'dbl',
    'fee_currency'         : 'str',  
    'fee_payment_note'     : 'str',  
    'feemass_external_id'  : 'str',  
    'feemass_process_date' : 'datetime'}


w_txn_ref = (W.partitionBy('atpt_mt_ref_nbr')
    .orderBy(F.col('atpt_mt_eff_date').desc()))
w_month_acct = (W.partitionBy('atpt_acct', 'b_wdraw_month')
    .orderBy(F.col('atpt_mt_eff_date').desc()))
w_inhouse    = (W.partitionBy('atpt_acct', 'b_wdraw_month', 'b_wdraw_is_inhouse')
    .orderBy(F.col('atpt_mt_eff_date').desc()))

purchase_to_savings = (lambda a_col: 
    F.concat_ws('-', F.substring(a_col, 1, 11), F.substring(a_col, 12, 3), F.lit('MX')))
        
wdraw_status = (F.when(~F.col('b_wdraw_is_commissionable'), -1)
                 .when(F.col('atpt_mt_posting_date').isNull(), -2)
                 .otherwise(0))

wdraw_withcols = OrderedDict({
    'b_core_acct'        : purchase_to_savings('atpt_mt_purchase_order_nbr'), 
    'b_wdraw_acq_code'   : F.substring(F.col('atpt_mt_interchg_ref'), 2, 6), 
    'b_wdraw_is_inhouse' : F.col('b_wdraw_acq_code') == 11072,
    'b_wdraw_month'      : F.date_trunc('month', F.col('atpt_mt_eff_date')).cast(T.DateType()), 
    'b_wdraw_rk_txns'    : F.row_number().over(w_txn_ref), 
    'b_wdraw_rk_acct'    : F.row_number().over(w_month_acct), 
    'b_wdraw_rk_inhouse' : F.when(F.col('b_wdraw_is_inhouse'), 
                                  F.row_number().over(w_inhouse)).otherwise(-1), 
    'b_wdraw_is_commissionable': ~F.col('b_wdraw_is_inhouse') | (F.col('b_wdraw_rk_inhouse') > 3),  
    'b_wdraw_commission_status': wdraw_status, 
    })

wdraw_withcols.update({kk: F.lit(None).cast(spk_types[vv]()) 
    for kk, vv in sap_resp_cols.items()})

wdraw_allcols = ['atpt_acct', 'atpt_mt_eff_date', 'atpt_mt_category_code',  
    'atpt_mt_card_nbr', 'atpt_mt_desc', 'atpt_mt_amount', 'atpt_mt_purchase_order_nbr', 
    'atpt_mt_posting_date', 'atpt_mt_ref_nbr', 'atpt_mt_interchg_ref'
    ] + list(wdraw_withcols.keys())

wdraw_txns_0 = (spark.read.format('delta')
    .load(atptx_loc)
    .filter(F.col('atpt_mt_category_code').isin([6010, 6011])))

wdraw_txns = (tools.with_columns(wdraw_txns_0, wdraw_withcols)
    .select(wdraw_allcols)
    .filter(F.col('b_wdraw_rk_txns') == 1))

display(wdraw_txns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Commissions
# MAGIC From `withdraw_txns` (current) filter those that are subject to a transaction fee.  
# MAGIC Also consider the previously processed ones in `withdrawals`, so that they aren't charged double.  

# COMMAND ----------

# MAGIC %md
# MAGIC **Nota:** Consideremos obtener las comisiones cobradas de SAP en vez de gestionar nuestra tabla.  

# COMMAND ----------

# Obtener las comisiones cobrables.  
cdmx_tz = timezone('America/Mexico_City')
today_date = dt.now(tz=cdmx_tz).date()
since_date = today_date - delta(COMSNS_FRAME)

# Since there is nothing to compare against on the first date,  
# just write it down and check that logic is idempotent. 
if not Δ.isDeltaTable(spark, at_withdrawals): 
    wdraw_txns.write.format('delta').save(at_withdrawals)

miscommissions = (spark.read
    .load(at_withdrawals)
    .filter(F.col('b_wdraw_is_commissionable') 
         & (F.col('b_wdraw_commission_status') == 0))  # Por alguna razón se repiten. 
    .withColumnRenamed('b_wdraw_commission_status', 'status_0'))  

# Se toma primero el nuevo dato. 
join_on     = ['atpt_mt_interchg_ref', 'atpt_mt_ref_nbr']
join_diff   = ['status_0', 'status_1']
join_select = [F.coalesce(wdraw_txns[a_col], miscommissions[a_col]).alias(a_col)
    for a_col in miscommissions.columns 
    if  a_col not in join_on + join_diff]

pre_commissions = (wdraw_txns
    .filter(F.col('b_wdraw_is_commissionable') 
         & (F.col('b_wdraw_commission_status') == 0)
         & (F.col('b_wdraw_rk_txns') == 1))
    .withColumnRenamed('b_wdraw_commission_status', 'status_1')              
    .join(miscommissions, how='outer', on=join_on)
    .filter(wdraw_txns['atpt_mt_posting_date'] >= since_date)
    .select(*(join_on + join_diff + join_select)))

commissions = pre_commissions
    
cmsns_summary = pre_commissions.select(['status_0', 'status_1']).summary('count')
cmsns_summary.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 3. Fees application
# MAGIC Create a SAP-session object to apply the transactions, and then call the corresponding API.  

# COMMAND ----------

from importlib import reload
import config; reload(config)
from src import core_models; reload(core_models)
from src import core_banking; reload(core_banking)
from src import tools; reload(tools)

from src.core_banking import SAPSession

core_starter = app_environ.prepare_coresession('qas-sap')
core_session = SAPSession(core_starter)

# COMMAND ----------

responses = core_session.process_commissions_atpt(spark, 
    commissionable, 'atm', **{'how-many': 50})

# COMMAND ----------

responses[1].json()
