# Databricks notebook source
# MAGIC %md 
# MAGIC # Introduction
# MAGIC 
# MAGIC This notebook is to be run as a Job for Withdrawal Commission Management:  
# MAGIC 1. We read transactions from ATPT (Card Management System - Fiserv).  
# MAGIC 2. Identify those corresponding to ATM Withdrawals, and their commission status.  
# MAGIC 3. Comparing to existing commissions, apply the corresponding fees.  
# MAGIC 4. Update withdrawal tables altogether.  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingredients
# MAGIC Prepare libraries, functions and variables.  
# MAGIC A few (or one) parameters, are to be set on the functional side.  
# MAGIC ... 

# COMMAND ----------

# MAGIC %pip install -r ../reqs_dbks.txt

# COMMAND ----------

TIMEFRAME = 90    # Max Days to charge commissions. 
API_LIMIT = 100   # Max number of commissions to apply at once. 

# COMMAND ----------

from importlib import reload
from src import core_banking
import config
reload(core_banking)
reload(config)


# COMMAND ----------

from collections import OrderedDict
from datetime import datetime as dt, date, timedelta as delta
from delta.tables import DeltaTable

import pandas as pd
from pandas.core.frame import DataFrame as pd_DataFame 
import re

from pyspark.sql import functions as F, types as T
from pyspark.sql.dataframe import DataFrame as spk_DataFrame
from pyspark.sql.window import Window as W

import src.schemas as src_spk
from src.core_banking import SAPSession
from config import (ConfigEnviron, 
    ENV, SERVER,                     
    RESOURCE_SETUP, CORE_ENV, 
    DATALAKE_PATHS as paths, 
    DELTA_TABLES as delta_keys)

resources = RESOURCE_SETUP[ENV]
app_environ = ConfigEnviron(ENV, SERVER, spark)
app_environ.sparktransfer_credential()

at_datasets    = f"{paths['abfss'].format('silver', resources['storage'])}/{paths['datasets']}"  
at_commissions = f"{paths['abfss'].format('silver', resources['storage'])}/{paths['commissions']}"  
atptx_loc      = f"{at_datasets}/atpt/delta"


def df_withcolumns(a_df: spk_DataFrame, cols_dict: dict) -> spk_DataFrame: 
    b_df = a_df
    for c_name, c_def in cols_dict.items(): 
        b_df = b_df.withColumn(c_name, c_def)
    return b_df


def pd_print(a_df: pd_DataFame, width=180): 
    options = ['display.max_rows', None, 
               'display.max_columns', None, 
               'display.width', width]
    with pd.option_context(*options):
        print(a_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Withdrawals Table
# MAGIC Starting from the transactions universe, consider the ones that are withdrawals.  
# MAGIC Prepare the corresponging attributes to manage them.  

# COMMAND ----------

experimental = True
if experimental: 
    wdraw_txns_0 = (spark.read.format('delta')
        .load(atptx_loc)
        .withColumn('b_wdraw_acquirer_code', F.substring(F.col('atpt_mt_interchg_ref'), 2, 6))
        .select('b_wdraw_acquirer_code')
        .distinct())
    display(wdraw_txns_0)

# COMMAND ----------

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
and_inhouse = (W.partitionBy('atpt_acct', 'b_wdraw_month', 'b_wdraw_is_inhouse')
    .orderBy('atpt_mt_eff_date'))

wdraw_withcols = OrderedDict({
    'b_wdraw_month'         : F.date_trunc('month', F.col('atpt_mt_eff_date')).cast(T.DateType()), 
    'b_wdraw_acquirer_code' : F.substring(F.col('atpt_mt_interchg_ref'), 2, 6), 
    'b_wdraw_is_inhouse'    : F.col('b_wdraw_acquirer_code') == 11072,
    'b_wdraw_rk_overall'    : F.row_number().over(wdw_account), 
    'b_wdraw_rk_inhouse'    : F.when(F.col('b_wdraw_is_inhouse'), F.row_number().over(and_inhouse)).otherwise(-1), 
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
wdraw_txns = (df_withcolumns(wdraw_txns_0, wdraw_withcols)
    .select(wdraw_cols))

display(wdraw_txns)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Commissions

# COMMAND ----------

# MAGIC %md 
# MAGIC From `withdraw_txns` (current) filter those that are subject to a transaction fee.  
# MAGIC Also consider the previously processed ones in `withdrawals`, so that they aren't charged double.  
# MAGIC Create a SAP-session object to apply the transactions, and then merge back to the `withdrawals`.  

# COMMAND ----------

reset_commissions = False
if reset_commissions: 
    dbutils.fs.rm(at_commissions, True)

# COMMAND ----------

# Date frame to consider commissions:  15 days
since_date = date.today() - delta(COMMISSION_FRAME)

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

commissionable = (wdraw_txns
    .filter(F.col('b_wdraw_commission_status') == 0)
    .withColumnRenamed('b_wdraw_commission_status', 'status_base')
    .join(miscommissions, how='outer', 
          on=['atpt_mt_interchg_ref', 'atpt_mt_ref_nbr'])
    .filter(wdraw_txns['atpt_mt_posting_date'] >= since_date)
    .select(*join_select, 'status_store', 'status_base'))
   
commissionable.select(['status_store', 'status_base']).summary('count').show()

# COMMAND ----------

core_starter = app_environ.prepare_coresession('qas-sap')
core_session = SAPSession(core_starter)


# COMMAND ----------


commissionable_pd = commissionable.toPandas()

for _, cmsn in commissionable_pd.iterrows(): 
    pass

_, cmsn = list(commissionable_pd.iterrows())[2]
cmsn

# COMMAND ----------

self = core_session

from src.core_models import Fee, FeeSetRequest
from datetime import date
from json import loads

atm_code    = '600405'
external_id = 'ops-commissions-atm-001'
the_date    = date.today().strftime('%Y-%m-%d')

the_commissions = [Fee(AccountID=row['atpt_acct'], TypeCode=atm_code) 
    for _, row in commissionable_pd.iterrows()]

fees_data = FeeSetRequest(ExternalID=external_id, ProcessDate=the_date, FeeDetail=the_commissions)

params = {'url': f"{self.config['main']['url']}/{self.api_calls['fees-apply']}", 
          'data': fees_data.json()}
        
a_response = self.post(f"{self.config['main']['url']}/{self.api_calls['fees-apply']}", 
                      data = loads(fees_data.json()))


#fees_data
loads(a_response.text)

# COMMAND ----------

self = core_session

#from src.core_models import Fee, FeeSetRequest
from datetime import date
import requests

some_params = {'$top': 100, '$skip': 0}
        
#a_response = self.post(**params)

r_response = requests.get(url=f"{self.config['main']['url']}/{self.api_calls['person-set']}", params=some_params, auth=core_session.auth, headers={'Content-Type': 'application/json', 'format': 'json'})
s_response = core_session.get(url=f"{self.config['main']['url']}/{self.api_calls['person-set']}", params=some_params)

# COMMAND ----------

print(f"Request Request:\n{r_response.request.headers}\n\nRequest Reponse:\n{r_response.headers}\n")
print(f"Session Request:\n{s_response.request.headers}\n\nSession Reponse:\n{s_response.headers}")

# COMMAND ----------

r_response.raise_for_status()


# COMMAND ----------

core_session.hooks
