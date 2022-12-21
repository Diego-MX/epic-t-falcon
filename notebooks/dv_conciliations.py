# Databricks notebook source
# MAGIC %md
# MAGIC # Descripción  
# MAGIC El objetivo de este notebook es correr los scripts para ejecutar las conciliaciones. 
# MAGIC El código se divide en las siguientes partes:  
# MAGIC &nbsp; 0. Preparar librerías, variables, funciones, etc.  
# MAGIC 1. Revisar las fuentes.  
# MAGIC 2. Preparar las 'tablas' comparativas.  
# MAGIC 3. Ejecutar las conciliaciones.  
# MAGIC 4. Depositar los resultados.  

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Preparación código

# COMMAND ----------

from importlib import reload

import config; reload(config)
from src import utilities; reload(utilities)
from src import schema_tools; reload(schema_tools)

# COMMAND ----------

from datetime import datetime as dt, date, timedelta as delta
from operator import attrgetter
import pandas as pd
from pandas.core import frame as pd_DF
from pyspark.sql import functions as F, types as T
from src.utilities import dirfiles_df
from src.schema_tools import with_columns
from config import (ConfigEnviron, 
    ENV, SERVER, RESOURCE_SETUP, CORE_ENV, 
    DATALAKE_PATHS as paths, 
    DELTA_TABLES as delta_keys)

resources = RESOURCE_SETUP[ENV]
app_environ = ConfigEnviron(ENV, SERVER, spark)
app_environ.sparktransfer_credential()

raw_base = paths['abfss'].format('raw', resources['storage'])
brz_base = paths['abfss'].format('bronze', resources['storage'])
at_conciliations = f"{raw_base}/{paths['conciliations']}"
at_spei          = f"{brz_base}/{paths['spei2']}"

def pd_print(a_df: pd_DF, width=180): 
    options = ['display.max_rows',    None, 
               'display.max_columns', None, 
               'display.width',       width]
    with pd.option_context(*options):
        print(a_df)


# COMMAND ----------

# Nos apoyamos en estos formatos de archivos predefinidos. 
file_formats = {
    'subledger'    : r'CONCILIA(?P<key>\w+)(?P<date>\d{8})\.txt', 
    'Cloud-Banking': r'CONCILIA(?P<key>\w+)(?P<date>\d{8})\.txt', 
    'spei'         : r'SPEI_FILE_(?P<key1>\w+)_(?P<date>\d{8})_(?P<key2>\w+)\.txt'}

def pre_format(file_df, a_dir): 
    reg_grp = (file_df['name'].str.extract(file_formats[a_dir])
        .assign(**{'date': lambda df: pd.to_datetime(df['date'], format='%Y%m%d')})
        .assign(directory= a_dir))
    mod_df = pd.concat([file_df, reg_grp], axis=1)
    return mod_df
    

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1. Directorios y esquemas

# COMMAND ----------

data_src = 'subledger'
pre_files = dirfiles_df(f"{at_conciliations}/{data_src}", spark)
ldgr_files = pre_format(pre_files, data_src)
ldgr_files

# COMMAND ----------

data_src = 'Cloud-Banking'
pre_files  = dirfiles_df(f"{at_conciliations}/{data_src}", spark)
c4b_files = pre_format(pre_files, data_src)
c4b_files

# COMMAND ----------

data_src = 'spei'
pre_files_1 = dirfiles_df(at_spei, spark)
pre_files_0 = pre_format(pre_files, data_src)
spei1_files = pre_files_0.loc[pre_files_0['key1'] == 'DATALAKE']
              
spei1_files

# COMMAND ----------

data_src = 'spei'
pre_files_1 = dirfiles_df(at_spei, spark)
pre_files_0 = pre_format(pre_files, data_src)
spei2_files = pre_files_0.loc[pre_files_0['key1'] == 'CONCILIACION']
              
spei2_files

# COMMAND ----------

### Parsing and Schemas.  

# General
tsv_options = {
    'mode'   : 'PERMISIVE', 
    'sep'    : '|',  
    'header' : True, 
    'nullValue'  : 'null', 
    'dateFormat'      : 'd.M.y', 
    'timestampFormat' : 'd.M.y H:m:s'} 

schema_types = {
    'int' : T.IntegerType, 'long': T.LongType,    'datetime': T.TimestampType, 
    'str' : T.StringType,  'dbl' : T.DecimalType, 'date'    : T.DateType}

# For Subledger
col_types = {
    'subledger': {
        'C55POSTD' : 'date', 'C55YEAR'    : 'int',
        'C35TRXTYP': 'str' , 'C55CONTID'  : 'long',
        'C11PRDCTR': 'str' , 'K5SAMLOC'   : 'str',  # K5SAMLOC is tricky float. 
        'LOC_CURR' : 'str' , 'IGL_ACCOUNT': 'long'}, 
    'Cloud-Banking': {
        'ACCOUNTID': 'str', 'TRANSACTIONTYPENAME': 'str', 'ACCOUNTHOLDERID': 'long', 
        'POSTINGDATE':'date', 'AMOUNT': 'dbl', 'CURRENCY': 'str', 
        'VALUEDATE': 'date', 'STATUSNAME': 'str', 'COUNTERPARTYACCOUNTHOLDER': 'str', 
        'COUNTERPARTYBANKACCOUNT': 'str', 'CREATIONUSER': 'str', 
        'TRANSACTIONID': 'str', 'TYPECODE': 'int', 'TYPENAME': 'str', 'PAYMENTTYPECODE': 'int', 
        'PAYMENTTYPENAME': 'str', 'TRANSACTIONTYPECODE':'int', 
        'COMMUNICATIONCHANNELCODE': 'int', 'COMMUNICATIONCHANNELNAME': 'str', 'ACCOUNTPROJECTION': 'str', 
        'ACCOUNTPRODUCTID': 'str', 'DEBITINDICATOR': 'str', 
        'EXCHANGERATETYPECODE': 'str', 'EXCHANGERATETYPENAME': 'str', 'EXCHANGERATE': 'str', 'AMOUNTAC': 'dbl', 
        'CURRENCYAC': 'str', 'PRENOTEID': 'str', 
        'STATUSCODE': 'int', 'COUNTERPARTYBANKCOUNTRY': 'str', 'COUNTERPARTYBANKID': 'str', 'COUNTERPARTYBANKACCOUNTID': 'str', 'PAYMENTTRANSACTIONORDERID': 'str', 
        'PAYMENTTRANSACTIONORDERITEMID': 'str', 'REFADJUSTMENTTRANSACTIONID': 'str', 'CANCELLATIONDOCUMENTINDICATOR': 'str', 'CANCELLEDENTRYREFERENCE': 'str',
        'CANCELLATIONENTRYREFERENCE': 'str', 'PAYMENTNOTES': 'str', 
        'CREATIONDATETIME': 'datetime', 'CHANGEDATETIME': 'datetime', 'RELEASEDATETIME': 'datetime',
        'CHANGEUSER': 'str', 'RELEASEUSER': 'str', 'COUNTER': 'int'}, 
    'spei-datalake': {
        
    }, 
    'spei-conciliacion': {
        
    }
}

schemas = {
    'subledger': T.StructType([
            T.StructField(f"/BA1/{kk}", schema_types[vv](), True) 
            for kk, vv in col_types['subledger'].items()]), 
    'Cloud-Banking': T.StructType([
            T.StructField(kk, schema_types[vv](), True) 
            for kk, vv in col_types['Cloud-Banking'].items()])
}

renamers = {
    'subledger': [F.col(f"/BA1/{kk}").alias(kk) for kk in col_types['subledger']], 
    'Cloud-Banking': col_types['Cloud-Banking']
}

mod_cols = {
    'subledger': {
        'sign_K5' : F.when(F.substring(F.col('K5SAMLOC'), -1, 1) == '-', -1).otherwise(1), 
        'K5SAMLOC': F.col('sign_K5')
            *F.regexp_replace(F.col('K5SAMLOC'), '[\-\,]', '').cast(T.DoubleType())}
}


# COMMAND ----------

# Revisar que no haya NULLS después del schema y modificaciones. 
core_nulls = ['COUNTERPARTYACCOUNTHOLDER', 'COUNTERPARTYBANKACCOUNT', 'EXCHANGERATETYPECODE', 'EXCHANGERATETYPENAME', 'EXCHANGERATE', 
    'PRENOTEID', 'COUNTERPARTYBANKCOUNTRY' , 'COUNTERPARTYBANKID', 'COUNTERPARTYBANKACCOUNTID', 'REFADJUSTMENTTRANSACTIONID', 'CANCELLATIONDOCUMENTINDICATOR', 
    'CANCELLEDENTRYREFERENCE', 'CANCELLATIONENTRYREFERENCE', 'PAYMENTNOTES', 'CHANGEDATETIME', 'CHANGEUSER']

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2. Preparación de fuentes

# COMMAND ----------

key_date_1 = date(2022, 12, 16)
match_dates = ldgr_files['date'].dt.date == key_date_1
if match_dates.sum() == 1: 
    ldgr_path = ldgr_files.loc[match_dates]['path'].values[0]
else: 
    raise Exception('Check condition condition.')

ldgr_tbl_0 = (spark.read.format('csv')
    .options(**tsv_options)
    .schema(schemas['subledger'])
    .load(ldgr_path)
    .select(*renamers['subledger']))

ldgr_tbl_1 = with_columns(ldgr_tbl_0, mod_cols['subledger'])

ldgr_tbl_2 = (ldgr_tbl_1
    .withColumn())

"""
  SELECT SUBSTRING(fpsl.Contract, 1, 11) AS NoCuenta,  
    TranTy as TransactionTypeCode,   
    '-' as TransactionType, 
    LocalCurrency as Moneda, 
    fpsl.CtaContable as CtaContableFPSL, 
    SUM(IF(bronze.fpsl.Contract IS NULL,0,1)) AS NumeroTotalRegistrosFPSL, 
    IFNULL(ROUND(SUM(bronze.fpsl.AmountInLocalCurrency),2),0) AS MontoTotalRegistrosFPSL
  FROM bronze.fpsl
      GROUP BY SUBSTRING(fpsl.Contract,1,11), TranTy, LocalCurrency, fpsl.CtaContable 
"""

# COMMAND ----------

key_date_1 = date(2023, 1, 31)
match_dates = C4B_files['date'].dt.date == key_date_1
if match_dates.sum() == 1: 
    c4b_path = c4b_files.loc[match_dates]['path'].values[0]
else: 
    raise Exception('Check condition condition.')

c4b_tbl_0 = (spark.read.format('csv')
    .options(**tsv_options)
    .schema(schemas['Cloud-Banking'])
    .load(ldgr_path)
    .select(*renamers['Cloud-Banking']))

c4b_tbl_1 = with_columns(c4b_tbl_0, mod_cols['Cloud-Banking'])

c4b_tbl_2 = (c4b_tbl_1
    .withColumn())

"""
  SELECT C4B.NoCuenta, C4B.TransactionTypeCode, C4B.TransactionType, C4B.Moneda, C4B.CtaContableFPSL, 
    NumeroTotalRegistrosC4B, MontoTotalRegistrosC4B, NumeroTotalRegistrosFPSL, MontoTotalRegistrosFPSL, 
    CASE WHEN MontoTotalRegistrosC4B = 0 AND MontoTotalRegistrosFPSL <> 0 THEN 'C4B'
         WHEN MontoTotalRegistrosC4B <> 0 AND MontoTotalRegistrosFPSL = 0 THEN 'FPSL' 
         WHEN NumeroTotalRegistrosC4B < NumeroTotalRegistrosFPSL THEN 'C4B' 
         WHEN NumeroTotalRegistrosFPSL < NumeroTotalRegistrosC4B THEN 'FPSL' 
         ELSE 'C4B Y FPSL' END Diferencias
  FROM (
    SELECT SUBSTRING(c4b.BankAccountID,1,11) AS NoCuenta, 
      TransactionTypeCode, TransactionType, 
      Currency as Moneda, '-' as CtaContableFPSL, 
      SUM(IF(c4b.BankAccountID IS NULL,0,1)) AS NumeroTotalRegistrosC4B, 
      IFNULL(ROUND(SUM(c4b.AmountBankAccountCurrency),2),0) AS MontoTotalRegistrosC4B
    FROM bronze.c4b
    GROUP BY SUBSTRING(c4b.BankAccountID,1,11), 
    TransactionTypeCode, TransactionType, Currency
"""

# COMMAND ----------

base_036 = c4b_tbl_2.join(ldgr_tbl_2, how='full', on=[''])

"""
ON  C4B.NoCuenta = FPSL.NoCuenta 
  AND C4B.TransactionTypeCode = FPSL.TransactionTypeCode 
  AND C4B.Moneda = FPSL.Moneda
WHERE NumeroTotalRegistrosC4B <> NumeroTotalRegistrosFPSL 
  OR MontoTotalRegistrosC4B + MontoTotalRegistrosFPSL <> 0 
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transformación

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW conciliacion AS (
# MAGIC   SELECT C4B.NoCuenta, C4B.TransactionTypeCode, C4B.TransactionType, C4B.Moneda, C4B.CtaContableFPSL, 
# MAGIC     NumeroTotalRegistrosC4B, MontoTotalRegistrosC4B, NumeroTotalRegistrosFPSL, MontoTotalRegistrosFPSL, 
# MAGIC     CASE WHEN MontoTotalRegistrosC4B = 0 AND MontoTotalRegistrosFPSL <> 0 THEN 'C4B'
# MAGIC          WHEN MontoTotalRegistrosC4B <> 0 AND MontoTotalRegistrosFPSL = 0 THEN 'FPSL' 
# MAGIC          WHEN NumeroTotalRegistrosC4B < NumeroTotalRegistrosFPSL THEN 'C4B' 
# MAGIC          WHEN NumeroTotalRegistrosFPSL < NumeroTotalRegistrosC4B THEN 'FPSL' 
# MAGIC          ELSE 'C4B Y FPSL' END Diferencias
# MAGIC   FROM (
# MAGIC     SELECT SUBSTRING(c4b.BankAccountID,1,11) AS NoCuenta, 
# MAGIC       TransactionTypeCode, TransactionType, 
# MAGIC       Currency as Moneda, '-' as CtaContableFPSL, 
# MAGIC       SUM(IF(c4b.BankAccountID IS NULL,0,1)) AS NumeroTotalRegistrosC4B, 
# MAGIC       IFNULL(ROUND(SUM(c4b.AmountBankAccountCurrency),2),0) AS MontoTotalRegistrosC4B
# MAGIC     FROM bronze.c4b
# MAGIC     GROUP BY SUBSTRING(c4b.BankAccountID,1,11), 
# MAGIC     TransactionTypeCode, TransactionType, Currency
# MAGIC    ) C4B 
# MAGIC FULL JOIN (
# MAGIC   SELECT SUBSTRING(fpsl.Contract, 1, 11) AS NoCuenta, 
# MAGIC     TranTy as TransactionTypeCode, '-' as TransactionType, LocalCurrency as Moneda, 
# MAGIC     fpsl.CtaContable as CtaContableFPSL, 
# MAGIC     SUM(IF(bronze.fpsl.Contract IS NULL,0,1)) AS NumeroTotalRegistrosFPSL, 
# MAGIC     IFNULL(ROUND(SUM(bronze.fpsl.AmountInLocalCurrency),2),0) AS MontoTotalRegistrosFPSL
# MAGIC   FROM bronze.fpsl
# MAGIC       GROUP BY SUBSTRING(fpsl.Contract,1,11), TranTy, LocalCurrency, fpsl.CtaContable 
# MAGIC ) FPSL
# MAGIC ON  C4B.NoCuenta = FPSL.NoCuenta 
# MAGIC   AND C4B.TransactionTypeCode = FPSL.TransactionTypeCode 
# MAGIC   AND C4B.Moneda = FPSL.Moneda
# MAGIC WHERE NumeroTotalRegistrosC4B <> NumeroTotalRegistrosFPSL 
# MAGIC   OR MontoTotalRegistrosC4B + MontoTotalRegistrosFPSL <> 0 
# MAGIC );
# MAGIC 
# MAGIC select * from conciliacion

# COMMAND ----------

_, core_row = list(files_df_cb.iterrows())[0]

core_df = (spark.read.format('csv')
    .options(**tsv_options)
    .schema(schemas['Cloud-Banking'])
    .load(core_row['path'])
    .select(*renamers['Cloud-Banking'])
)
display(core_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 4. Resultados

# COMMAND ----------

# MAGIC %md
# MAGIC ## x. Extra
# MAGIC Este código es auxiliar en el desarrollo, como _debugging_ y pruebas.  
