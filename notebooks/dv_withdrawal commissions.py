# Databricks notebook source
# MAGIC %md 
# MAGIC # Introducción
# MAGIC 
# MAGIC Este _notebook_ se ejecuta como _job_ llamado `Withdrawal Commission Management`.  
# MAGIC La función principal / inicial / única-(dic '22) es para el cobro de manejo de comisiones.  
# MAGIC Consta de los siguientes secciones:  
# MAGIC &nbsp; 0. Leer las transacciones de ATPT, de acuerdo con el sistema de gestión de tarjetas Fiserv.  
# MAGIC 1. Identificar las correspondientes a retiros de cajero, y los estatus de comisiones.  
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
# MAGIC   Estas no se han completado, por lo que sigue en standby.   
# MAGIC   
# MAGIC - La API de comisiones requiere el número de cuenta en formato SAP, la tabla de transacciones lo tiene en formato Fiserv.  
# MAGIC   La traducción de una a la otra se hace mediante la tabla `dambs` de Fiserv, pero no se ha comprobado.  
# MAGIC   
# MAGIC - Además del número de cuenta, la traducción de una entrada en `atpt` y luego como retiro de ATM, utiliza la clase de objetos (Py) `core_models.Fee` y `core_models.FeeSet`.  Se definieron simplemente a partir de la API, pero no han pasado las pruebas de completez.   
# MAGIC 
# MAGIC Por el momento, eso es todo.  

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

# Al ejecutarse como _notebook_, instalamos las librerías necesarias.  
%pip install -r ../reqs_dbks.txt

# COMMAND ----------

# Se definen algunas constantes de negocio.  

COMSNS_FRAME = 15    # Max Days to charge commissions. 
COMSNS_APPLY = 100   # Max number of commissions to apply at once. 
PAGE_MAX     = 500   # Max number of records (eg. Person-Set) to request at once. 


# COMMAND ----------

# Para desbichar el código, este bloque que actualiza los módulos de importación/modificación.  
# El equivalente a veces se encuentra como: 
# %load_ext autoreload
# %autoreload 2

from importlib import reload
from src import core_banking; reload(core_banking)
import config; reload(config)


# COMMAND ----------

# Importamos los módulos; y definir funciones y variables más específicas. 

from collections import OrderedDict
from datetime import datetime as dt, date, timedelta as delta
from delta.tables import DeltaTable
from functools import reduce
import pandas as pd
from pandas.core.frame import DataFrame as pd_DataFame 
from pyspark.sql import functions as F, types as T
from pyspark.sql.dataframe import DataFrame as spk_DataFrame
from pyspark.sql.window import Window as W

import src.schemas as src_spk
from src.core_banking import SAPSession
from config import (ConfigEnviron, 
    ENV, SERVER, RESOURCE_SETUP, CORE_ENV, 
    DATALAKE_PATHS as paths, 
    DELTA_TABLES as delta_keys)

resources = RESOURCE_SETUP[ENV]
app_environ = ConfigEnviron(ENV, SERVER, spark)
app_environ.sparktransfer_credential()

at_datasets    = f"{paths['abfss'].format('silver', resources['storage'])}/{paths['datasets']}"  
at_commissions = f"{paths['abfss'].format('silver', resources['storage'])}/{paths['commissions']}"  
atptx_loc      = f"{at_datasets}/atpt/delta"
dambs_loc      = f"{at_datasets}/dambs/delta"


def spk_withcolumns(a_df: spk_DataFrame, cols_dict: dict) -> spk_DataFrame: 
    func = lambda x_df, col_item: x_df.withColumn(col_item[0], col_item[1])
    return reduce(func, cols_dict.items(), a_df)


def pd_print(a_df: pd_DataFame, width=180): 
    options = ['display.max_rows',    None, 
               'display.max_columns', None, 
               'display.width',       width]
    with pd.option_context(*options):
        print(a_df)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 1. Withdrawals Table
# MAGIC Starting from the transactions universe, consider the ones that are withdrawals.  
# MAGIC Prepare the corresponging attributes to manage them.  

# COMMAND ----------

experimental = False
if experimental: 
    wdraw_txns_0 = (spark.read.format('delta')
        .load(atptx_loc)
        .withColumn('b_wdraw_acquirer_code', F.substring(F.col('atpt_mt_interchg_ref'), 2, 6))
        .select('b_wdraw_acquirer_code')
        .distinct())
    display(wdraw_txns_0)

# COMMAND ----------

# Datos correspondientes a las cuentas/accounts/dambs complementan la información de las transacciones. 

wdw_account =  W.partitionBy('ambs_acct').orderBy(F.col('file_date').desc())
dambs_cols = ['file_date', 'b_account_num', 'b_rank_acct', 'b_sap_savings', 'ambs_sav_rtng_nbr',  'ambs_sav_acct_nbr']

dambs_ref = (spark.read.format('delta')
    .load(dambs_loc)
    .withColumn('b_account_num', F.col('ambs_acct'))
    .withColumn('b_rank_acct', F.row_number().over(wdw_account))
    .withColumn('b_sap_savings', F.concat_ws('-', 
        F.col('ambs_sav_acct_nbr'), F.col('ambs_sav_rtng_nbr').cast(T.IntegerType()), F.lit('MX')))
    .filter(F.col('b_rank_acct') == 1)
    .select(*dambs_cols))

display(dambs_ref)

# COMMAND ----------

expr = "^[0-9]{11}\-[0-9]{3}\-[aA-zZ]{2}$"
dambs_ref = dambs_ref.filter(F.col("b_sap_savings").rlike(expr))

# COMMAND ----------

display(dambs_ref)

# COMMAND ----------

# Preparamos las transacciones que corresponden a los retiros (withdrawals/wdraw).  

# Alguna información complementaria: 

# b_wdraw_acquirer_code:  {110072: Banorte, ...}
# b_wdraw_commission_status: {
#     -1: not-commissionable
#     -2: not-posted (via date)
#      0: not-applied
#      1: applied
#      2: other status}

# atpt_mt_category_code: {
#    5045, 5311, 5411, 5661, 7994
#    6010: cash, 6011: atm, 
#    (596[02456789], 7995): high-risk-merchant}

wdw_account =  W.partitionBy('atpt_acct', 'b_wdraw_month').orderBy('atpt_mt_eff_date')
wdw_inhouse = (W.partitionBy('atpt_acct', 'b_wdraw_month', 'b_wdraw_is_inhouse')
    .orderBy('atpt_mt_eff_date'))

wdraw_withcols = OrderedDict({
    'b_account_num'         : F.col('atpt_acct'), 
    'b_wdraw_month'         : F.date_trunc('month', F.col('atpt_mt_eff_date')).cast(T.DateType()), 
    'b_wdraw_acquirer_code' : F.substring(F.col('atpt_mt_interchg_ref'), 2, 6), 
    'b_wdraw_is_inhouse'    : F.col('b_wdraw_acquirer_code') == 11072,
    'b_wdraw_rk_overall'    : F.row_number().over(wdw_account), 
    'b_wdraw_rk_inhouse'    : F.when(F.col('b_wdraw_is_inhouse'), F.row_number().over(wdw_inhouse)).otherwise(-1), 
    'b_wdraw_is_commissionable': ~F.col('b_wdraw_is_inhouse') | (F.col('b_wdraw_rk_inhouse') > 3),  
    'b_wdraw_commission_status': F.when(~F.col('b_wdraw_is_commissionable'), -1)
                .when(F.col('atpt_mt_posting_date').isNull(), -2).otherwise(0)})

wdraw_cols = ['atpt_mt_eff_date', 'atpt_mt_category_code', 'atpt_acct', 
    'atpt_mt_card_nbr', 'atpt_mt_desc', 'atpt_mt_amount', 
    'atpt_mt_posting_date', 'atpt_mt_interchg_ref', 'atpt_mt_ref_nbr'
    ] + list(wdraw_withcols.keys())

wdraw_txns_0 = (spark.read.format('delta')
    .load(atptx_loc)
    .filter(F.col('atpt_mt_category_code').isin([6010, 6011])))

# This is potentially a big Set. 
wdraw_txns = (spk_withcolumns(wdraw_txns_0, wdraw_withcols)
    .select(wdraw_cols))

display(wdraw_txns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Commissions
# MAGIC From `withdraw_txns` (current) filter those that are subject to a transaction fee.  
# MAGIC Also consider the previously processed ones in `withdrawals`, so that they aren't charged double.  

# COMMAND ----------

# Restablecer la tabla de comisiones en caso de experimentación.  

reset_commissions = True
if reset_commissions: 
    dbutils.fs.rm(at_commissions, True)

# COMMAND ----------

# Obtener las comisiones cobrables.  

# Date frame to consider commissions:  15 days
since_date = date.today() - delta(COMSNS_FRAME)

# Since there is nothing to compare against on the first date,  
# just write it down and check that logic is idempotent. 
if not DeltaTable.isDeltaTable(spark, at_commissions): 
    wdraw_txns.write.format('delta').save(at_commissions)

miscommissions = (spark.read.format('delta').load(at_commissions)
    .filter(F.col('b_wdraw_commission_status') == 0)
    .withColumnRenamed('b_wdraw_commission_status', 'status_store'))  # {not-applied, not(-previously)-posted}

join_select = [F.coalesce(wdraw_txns[a_col], miscommissions[a_col]).alias(a_col)
    for a_col in miscommissions.columns 
    if a_col not in ['atpt_mt_interchg_ref', 'atpt_mt_ref_nbr', 'status_store']]

pre_commissionable = (wdraw_txns
    .filter(F.col('b_wdraw_commission_status') == 0)
    .withColumnRenamed('b_wdraw_commission_status', 'status_base')
    .join(miscommissions, how='outer', 
          on=['atpt_mt_interchg_ref', 'atpt_mt_ref_nbr'])
    #.filter(wdraw_txns['atpt_mt_posting_date'] >= since_date)
    .select(*join_select, 'status_store', 'status_base'))
   
commissionable = (pre_commissionable
    .join(dambs_ref, on='b_account_num', how='left'))
    
cmsns_summary = pre_commissionable.select(['status_store', 'status_base']).summary('count')
cmsns_summary.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 3. Fees application
# MAGIC Create a SAP-session object to apply the transactions, and then call the corresponding API.  

# COMMAND ----------

core_starter = app_environ.prepare_coresession('qas-sap')
core_session = SAPSession(core_starter)

# COMMAND ----------

persons = core_session.call_person_set({'how_many':50})
pd_print(persons)

# COMMAND ----------

responses = core_session.call_txns_commissions(commissionable, 'atm', **{'how-many': 20})


# COMMAND ----------

# Debugging
self, accounts_df, cmsn_key, kwargs = core_session, commissionable, 'atm', {'how-many': 5}

from datetime import date
from itertools import groupby
from math import ceil
from pandas.core import frame
from pyspark.sql import dataframe as spk_df
from pytz import timezone
from src.core_models import Fee, FeeSet

API_LIMIT = 200
cmsn_id, cmsn_name = self.commission_labels[cmsn_key]

by_k = kwargs.get('how-many', API_LIMIT)
date_str = date.today().strftime('%Y-%m-%d')

if isinstance(accounts_df, frame.DataFrame):
    row_itr = accounts_df.iterrows()
    len_df = len(accounts_df)

elif isinstance(accounts_df, spk_df.DataFrame): 
    row_itr = enumerate(accounts_df.rdd.toLocalIterator())
    len_df = accounts_df.count()

iter_key = lambda ii_row: ii_row[0]//by_k
n_grps = ceil(len_df/by_k)

responses = []
for kk, sub_itr in groupby(row_itr, iter_key): 
    
    print(f'Calling group {kk} of {n_grps}.')
    fees_set   = [Fee(**{
        'AccountID'  : row['atpt_acct'], 
        'TypeCode'   : cmsn_id, 
        'PostingDate': date_str}) for _, row in sub_itr]
    feeset_obj = FeeSet(**{
        'ProcessDate': date_str, 
        'ExternalID' : cmsn_name, 
        'FeeDetail'  : fees_set})
    posters = {
        'url' : f"{self.base_url}/{self.api_calls['fees-apply']}", 
        'data': feeset_obj.json(exclude_unset=True)}
    the_resp = self.post(**posters)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Log table
