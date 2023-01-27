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

# MAGIC %md 
# MAGIC Se numera con 0, ya que esta sección contiene puro código preparativo.  
# MAGIC Esto corresponde a la compra de ingredientes y utensilios antes de preparar una receta.  
# MAGIC 
# MAGIC - En la celda inicial cargamos los módulos y funciones que corresponden a librerías públicas.  
# MAGIC - A continuación utilizamos la función `importlib.reload` para recargar módulos locales que se llegan a utilizar en el desarrollo del _notebook_.  
# MAGIC   Otros autores utilizan el comando similar `$ load_ext autoreload; autoreload 2`.  
# MAGIC   Se prefiere `reload` por ser más explícita en cuanto a los módulos que se recargan.  
# MAGIC - Finalmente hacemos la carga de módulos locales.  Estos contienen tanto funciones, como variables de ambiente que se gestionan localmente.  
# MAGIC 
# MAGIC Cabe mencionar que el orden en que se definen los ingredientes es inverso al orden de especificidad de su entendimiento.  
# MAGIC - Por definición:  librerías públicas -> funciones utilitarias -> variables de entorno.  
# MAGIC - Por inteligibilidad:  
# MAGIC   1) Las variables de entorno se requieren por otros equipos, 
# MAGIC   2) Las funciones utilitarias se utilizan internamente, 
# MAGIC   3) Las librerías públicas se mantienen como referencia, aunque por ser públicas serían más fáciles de asumir.  

# COMMAND ----------

from datetime import datetime as dt, date, timedelta as delta
from collections import OrderedDict
from operator import attrgetter
import pandas as pd

from pyspark.sql import functions as F, types as T
from pytz import timezone
import re

# COMMAND ----------

now_mx = dt.now(timezone('America/Mexico_City'))
days_back = 3 if (now_mx == 0) else 1
yday_ish = now_mx.date() - delta(days=days_back)

key_date_ops  = yday_ish
key_date_spei = yday_ish


# COMMAND ----------

# MAGIC %md  
# MAGIC 
# MAGIC Usamos 3 módulos independientes:  `utilities`, `schema_tools`, `config`.  
# MAGIC 
# MAGIC - La distinción entre los primeros dos no es suuuper inminente, pero se mantienen separados para consistencia con otros repositorios similares.   
# MAGIC   En esencia contienen funciones para manipular los objetos que se trabajan.  
# MAGIC - Las variables y objetos de `config` se utilizan en conjunto con otros equipos como x ej. Infraestructura.  

# COMMAND ----------

from src.utilities import dirfiles_df, write_datalake, pd_print, file_exists
from src.schema_tools import with_columns
from src.sftp_sources import process_files, get_match_path
from config import (ConfigEnviron, 
    ENV, SERVER, RESOURCE_SETUP, CORE_ENV, 
    DATALAKE_PATHS as paths, 
    DELTA_TABLES as delta_keys)

resources = RESOURCE_SETUP[ENV]
app_environ = ConfigEnviron(ENV, SERVER, spark)
app_environ.sparktransfer_credential()

raw_base = paths['abfss'].format('raw', resources['storage'])
gld_base = paths['abfss'].format('gold', resources['storage'])
brz_base = paths['abfss'].format('bronze', resources['storage'])

at_conciliations = f"{raw_base}/{paths['conciliations']}"
at_spei_banking  = f"{brz_base}/{paths['spei-c4b']}"
at_spei_ledger   = f"{brz_base}/{paths['spei-gfb']}"
at_reports       = f"{gld_base}/{paths['reports2']}"


# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1. Esquemas y carpetas

# COMMAND ----------

# MAGIC %md  
# MAGIC 
# MAGIC - Las fuentes de datos que utilizamos se alojan en carpetas del _datalake_.  
# MAGIC - Los archivos de las carpetas tienen metadatos en sus nombres, que se extraen en la subsección `Regex Carpetas`.  
# MAGIC - Además los archivos consisten de datos tabulares cuyos esquemas se construyen en la sección correspondiente.  
# MAGIC - Finalmente se procesa cada una de las fuentes, de acuerdo a la llave de identificación, su descripción y el sistema al que pertenece.    
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

# For Subledger

ops_fpsl = {
    'base' : OrderedDict({ 
        'C55POSTD' : 'date', 'C55YEAR'    : 'int',
        'C35TRXTYP': 'str' , 'C55CONTID'  : 'str',
        'C11PRDCTR': 'str' , 'K5SAMLOC'   : 'str',  
        'LOC_CURR' : 'str' , 'IGL_ACCOUNT': 'long'}), 
    'read' : {
        'K5SAMLOC' : (F.regexp_replace(F.col('K5SAMLOC'), r"(\-?)([0-9\.])(\-?)", "$3$1$2")
                       .cast(T.DoubleType()))}, 
    'mod' : {
        'txn_valid'   : F.col('C11PRDCTR').isNotNull() 
                      & F.col('C11PRDCTR').startswith('EPC') 
                      &~F.col('C35TRXTYP').startswith('S'), 
        'num_cuenta'  : F.substring(F.col('C55CONTID'), 1, 11),  #JOIN
        'clave_txn'   : F.col('C35TRXTYP').cast(T.IntegerType()),
        'moneda'      : F.col('LOC_CURR'), 
        'monto_txn'   : F.col('K5SAMLOC'), # Suma y compara. 
        'cuenta_fpsl' : F.col('IGL_ACCOUNT')}, # Referencia extra, asociada a NUM_CUENTA.  
    'post': {
        'where': ['txn_valid'], 
        'by'   : ['num_cuenta', 'clave_txn', 'moneda'], 
        'agg'  : {
            'fpsl_num_txns': F.count('*'), 
            'fpsl_monto'   : F.round(F.sum('monto_txn'), 2)}}
}

ops_c4b = {
    'base' : OrderedDict({
        'ACCOUNTID': 'str', 'TRANSACTIONTYPENAME': 'str', 'ACCOUNTHOLDERID': 'long', 
        'POSTINGDATE':'date', 'AMOUNT': 'dbl', 'CURRENCY': 'str', 
        'VALUEDATE': 'date', 'STATUSNAME': 'str', 'COUNTERPARTYACCOUNTHOLDER': 'str', 
        'COUNTERPARTYBANKACCOUNT': 'str', 'CREATIONUSER': 'str', 
        'TRANSACTIONID': 'str', 'TYPECODE': 'int', 'TYPENAME': 'str', 'PAYMENTTYPECODE': 'int', 
        'PAYMENTTYPENAME': 'str', 'TRANSACTIONTYPECODE':'int', 
        'COMMUNICATIONCHANNELCODE': 'int', 'COMMUNICATIONCHANNELNAME': 'str', 
        'ACCOUNTPROJECTION': 'str', 
        'ACCOUNTPRODUCTID': 'str', 'DEBITINDICATOR': 'str', 
        'EXCHANGERATETYPECODE': 'str', 'EXCHANGERATETYPENAME': 'str', 'EXCHANGERATE': 'str', 
        'AMOUNTAC': 'dbl', 'CURRENCYAC': 'str', 'PRENOTEID': 'str', 'STATUSCODE': 'int', 
        'COUNTERPARTYBANKCOUNTRY': 'str', 'COUNTERPARTYBANKID': 'str', 
        'COUNTERPARTYBANKACCOUNTID': 'str', 'PAYMENTTRANSACTIONORDERID': 'str', 
        'PAYMENTTRANSACTIONORDERITEMID': 'str', 'REFADJUSTMENTTRANSACTIONID': 'str', 
        'CANCELLATIONDOCUMENTINDICATOR': 'str', 'CANCELLEDENTRYREFERENCE': 'str',
        'CANCELLATIONENTRYREFERENCE': 'str', 'PAYMENTNOTES': 'str', 
        'CREATIONDATETIME': 'datetime', 'CHANGEDATETIME': 'datetime', 'RELEASEDATETIME': 'datetime',
        'CHANGEUSER': 'str', 'RELEASEUSER': 'str', 'COUNTER': 'int'}), 
    'mod': {
        'txn_valid'   : F.lit(True),  # Filtrar
        'num_cuenta'  : F.split(F.col('ACCOUNTID'), '-')[0], # JOIN
        'clave_txn'   : F.col('TRANSACTIONTYPECODE'),
        'moneda'      : F.col('CURRENCY'), 
        'monto_txn'   : F.col('AMOUNT'),  # Suma y compara
        'tipo_txn'    : F.col('TYPENAME')},  # Referencia, asociada a CLAVE_TXN.   
    'post': {
        'where': ['txn_valid'], 
        'by'   : ['num_cuenta', 'clave_txn', 'moneda'], 
        'agg'  : {
            'c4b_num_txns': F.count('*'), 
            'c4b_monto'   : F.round(F.sum('monto_txn'), 2)}}
}

ref_regex = r"(MIFELSPEI|\d+ATP\d)(20\d{6})(\d+)"

spei_gfb = {
    'base' : OrderedDict({
        'extra': 'str', 'cep_issuer': 'int', 
        'account_id': 'long', 'account_digital': 'long', 
        'clabe': 'long', 'receiver_channel': 'str', 'receiver_service': 'str', 
        'txn_amount': 'dbl', 'txn_status': 'str', 'rejection_reason': 'str', 
        'sender_bank': 'str', 'date_added': 'str', 'receiver_name': 'str', 'receiver_account': 'long', 
        'receiver_clabe': 'long', 'receiver_rfc': 'str', 'concept': 'str', 'reference': 'str', 
        'tracking_key': 'str', 'uuid': 'str', 'status': 'str'}), 
    'read': {
        'track_type'    : F.regexp_extract('tracking_key', ref_regex, 1), 
        'track_date'    : F.regexp_extract('tracking_key', ref_regex, 2),  #.to_date('yyyyMMdd')
        'track_num2'    : F.regexp_extract('tracking_key', ref_regex, 3),  #.cast(T.Longtype())
    },
    'mod': {
        'txn_valid'     : F.col('tracking_key').rlike(ref_regex), # Filtrar
        'ref_num'       : F.col('tracking_key'), # Join
        'account_num'   : F.col('account_digital'), 
        'txn_type_code' : F.when(F.col('receiver_service') == 'EMISION SPEI' , '500402')
                           .when(F.col('receiver_service') == 'SPEI RECIBIDO', '550403')
                           .otherwise(F.col('receiver_service')), 
        'txn_amount'    : F.col('txn_amount'), # Comparar
        'txn_status'    : F.col('status'), 
        'txn_date_gfb'  : F.to_date('track_date', 'yyyyMMdd')}, 
    'post': {
        'where': ['txn_valid'], 
        'by'   : ['ref_num', 'account_num', 'txn_type_code'], 
        'agg'  : {
            'gfb_num_txns': F.count('*'), 
            'gfb_monto'   : F.round(F.sum('txn_amount'), 2) }}
}

spei_c4b = {
    'base': OrderedDict({
        'account_c4b': 'str', 'txn_type_code': 'str', 
        'txn_type': 'str', 'txn_postdate': 'datetime', 
        'AMOUNT_LOCAL': 'str', 'AMOUNT_CENTS': 'str',  
        # Vienen en formato europeo así que lo separamos y ajustamos. 
        'currency': 'str', 'txn_id': 'str', 
        'type_code': 'int', 'type_name': 'str', 
        'pymt_type': 'str', 'pymt_type_code': 'str', 'comm_channel_code': 'str', 
        'comm_channel': 'str', 'acct_projection': 'str', 'product_id': 'str', 
        'holder_id': 'long', 'debit_indicator': 'str', 
        'value_date': 'datetime', 'creation_user': 'long', 'release_user': 'long', 
        'counter_holder': 'str', 'counter_bank_id': 'int', 'counter_account_id': 'long', 
        'counter_account': 'int', 'counter_bank_country': 'str', 'pymt_order_id': 'str', 
        'pymt_item_id': 'str', 'ref_adjust_txn_id': 'str', 
        'cancel_document': 'str', 'pymt_notes': 'str', 'end_to_end': 'str', 
        'ref_number': 'str', 'txn_status_code': 'int', 
        'txn_status': 'str', 'acct_holder_name': 'str', 
        'acct_holder_tax_id': 'str', 'cntr_party_tax_id': 'str', 
        'fee_amount': 'dbl', 'fee_currency': 'str', 
        'vat_amount': 'dbl', 'vat_currency': 'str'}), 
    'read': {
        'amount_local' : F.concat_ws('.',  
                F.regexp_replace(F.col('AMOUNT_LOCAL'), '\.', ''), 
                F.col('AMOUNT_CENTS')).cast(T.DoubleType()), 
        'txn_postdate'  : F.col('txn_postdate').cast(T.DateType()), 
        'value_date'    : F.col('value_date').cast(T.DateType()), 
        'e2e_type'      : F.regexp_extract('end_to_end', ref_regex, 1), 
        'e2e_date'      : F.regexp_extract('end_to_end', ref_regex, 2),  
        'e2e_num2'      : F.regexp_extract('end_to_end', ref_regex, 3)}, 
    'mod': {
        'txn_valid'      : F.col('end_to_end').rlike(ref_regex)
                        & (F.col('txn_status') != 'Posting Canceled'), # Filtrar
        'ref_num'        : F.col('end_to_end'),
        'account_num'    : F.substring(F.col('account_c4b'), 1, 11).cast(T.LongType()), 
        'txn_type_code'  : F.col('txn_type_code'),  
        'txn_amount'     : F.col('amount_local'), # Comparar. 
        'txn_date'       : F.col('txn_postdate'), 
        'txn_status'     : F.when(F.col('txn_status') == 'Posted', 'P')
                .otherwise(F.col('txn_status'))}, 
    'post': {
        'where': ['txn_valid'], 
        'by'   : ['ref_num', 'account_num', 'txn_type_code'], 
        'agg'  : {
            'c4b_num_txns': F.count('*'), 
            'c4b_monto'   : F.round(F.sum('txn_amount'), 2)}}
}



# COMMAND ----------

### Parsing and Schemas.  

# From Columns
base_cols = {
    'subledger'    : ops_fpsl['base'], 
    'cloud-banking': ops_c4b ['base'],   
    'spei-ledger'  : spei_gfb['base'], 
    'spei-banking' : spei_c4b['base'], 
    'spei-banking2': {  # old-ones
        'txn_code': 'str', 'txn_date': 'date', 'txn_time': 'str', 
        'txn_amount': 'dbl', 'ref_num': 'str', 'trck_code': 'str', 'sender_account': 'long', 
        'clabe_account': 'long', 'sender_name': 'str', 'sender_id': 'long', 'txn_description': 'str', 
        'receiver_bank_id': 'int', 'receiver_account': 'long', 'receiver_name': 'str', 
        'txn_geolocation': 'str', 'txn_status': 'int', 'resp_code': 'int'}
}

read_cols = {
    'subledger'    : ops_fpsl.get('read', {}), 
    'cloud-banking': ops_c4b.get ('read', {}),   
    'spei-ledger'  : spei_gfb.get('read', {}), 
    'spei-banking' : spei_c4b.get('read', {}),     
}

mod_cols = {
    'subledger'    : ops_fpsl.get('mod', {}), 
    'cloud-banking': ops_c4b.get ('mod', {}),   
    'spei-ledger'  : spei_gfb.get('mod', {}), 
    'spei-banking' : spei_c4b.get('mod', {}), 
}


# General
tsv_options = {
    'subledger' : dict([
        ('mode', 'PERMISIVE'), 
        ('sep', '|'), ('header', True), ('nullValue', 'null'), 
        ('dateFormat', 'd.M.y'), ('timestampFormat', 'd.M.y H:m:s')]), 
    'cloud-banking' : dict([
        ('mode', 'PERMISIVE'), 
        ('sep', '|'), ('header', True), ('nullValue', 'null'), 
        ('dateFormat', 'd.M.y'), ('timestampFormat', 'd.M.y H:m:s')]),
    'spei-ledger' : dict([  # GFB
        ('mode', 'PERMISIVE'), ('sep', ';'), ('header', False)]), 
    'spei-banking' : dict([  # C4B
        ('mode', 'PERMISIVE'), ('sep', ','), ('header', False), 
        ('dateFormat', 'd-M-y'), ('timestampFormat', 'd/M/y H:m:s')]), 
    'spei-banking2' : dict([ # Este es viejo. 
        ('mode', 'PERMISIVE'), ('sep', '|'), ('header', False), 
        ('dateFormat', 'd-M-y')])
}

schema_types = {
    'int' : T.IntegerType, 'long': T.LongType,   'datetime': T.TimestampType, 
    'str' : T.StringType,  'dbl' : T.DoubleType, 'date'    : T.DateType, 
    'bool': T.BooleanType, 'flt' : T.FloatType,  'null'    : T.NullType}

schemas = {
    'subledger'     : T.StructType([
            T.StructField(f"/BA1/{kk}", schema_types[vv](), True) 
            for kk, vv in base_cols['subledger'].items()]), 
    'cloud-banking' : T.StructType([
            T.StructField(kk, schema_types[vv](), True) 
            for kk, vv in base_cols['cloud-banking'].items()]), 
    'spei-ledger'   : T.StructType([
            T.StructField(kk, schema_types[vv](), True) 
            for kk, vv in base_cols['spei-ledger'].items()]), 
    'spei-banking'  : T.StructType([
            T.StructField(kk, schema_types[vv](), True) 
            for kk, vv in base_cols['spei-banking'].items()]), 
}

renamers = {
    'subledger'     : [F.col(f"/BA1/{kk}").alias(kk) for kk in base_cols['subledger']], 
    'cloud-banking' : [kk for kk in base_cols['cloud-banking']], 
    'spei-ledger'   : [kk for kk in base_cols['spei-ledger']], 
    'spei-banking'  : [kk for kk in base_cols['spei-banking']], 
}


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Incluimos los queries de referencia. 
# MAGIC + **Operativo**
# MAGIC ```sql
# MAGIC   SELECT
# MAGIC     date_format(to_date(DateAdded, 'dd/MM/yyyy'), 'dd/MM/yyyy') AS Fecha,   
# MAGIC     COUNT(BankAccountID) AS Numero_de_registros_en_C4B,
# MAGIC     ROUND(SUM(AmountBankAccountCurrency), 2) AS Monto_de_registros_en_C4B,
# MAGIC     COUNT(MainAccount) AS Numero_de_registros_en_Banorte,
# MAGIC     ROUND(SUM(TransactionAmount), 2) AS Monto_de_registros_en_Banorte,
# MAGIC     ABS(COUNT(BankAccountID) - COUNT(MainAccount)) AS Numero_de_registros_de_la_diferencia_detectada_entre_CB4_y_Banorte,
# MAGIC     ABS(SUM(AmountBankAccountCurrency) - SUM(TransactionAmount)) AS Monto_de_los_registros_de_la_diferencia_detectada_entre_CB4_y_Banorte,
# MAGIC     TransactionStatus
# MAGIC   FROM bronze.banorte_spei
# MAGIC   FULL JOIN bronze.c4b_spei 
# MAGIC     ON bronze.banorte_spei.TrackingKey = bronze.c4b_spei.End2EndID
# MAGIC     AND bronze.banorte_spei.Reference = bronze.c4b_spei.ReferenceNumber
# MAGIC     AND bronze.banorte_spei.MainAccount = bronze.c4b_spei.BankAccountID
# MAGIC     AND CASE
# MAGIC         WHEN bronze.banorte_spei.ReceptionService = 'EMISION SPEI' THEN '500402'
# MAGIC         WHEN bronze.banorte_spei.ReceptionService = 'SPEI RECIBIDO' THEN '550403'
# MAGIC         END = bronze.c4b_spei.TransactionTypeCode
# MAGIC     --AND bronze.banorte_spei.DateAdded = bronze.c4b_spei.ValueDate
# MAGIC   WHERE
# MAGIC     DateAdded = '03/01/2022'
# MAGIC   GROUP BY
# MAGIC     Fecha,
# MAGIC     TransactionStatus
# MAGIC ```  
# MAGIC   
# MAGIC   
# MAGIC + **De SPEI**
# MAGIC ```sql
# MAGIC   SELECT
# MAGIC     date_format(to_date(DateAdded, 'dd/MM/yyyy'), 'dd/MM/yyyy') AS Fecha,   
# MAGIC     COUNT(BankAccountID) AS Numero_de_registros_en_C4B,
# MAGIC     ROUND(SUM(AmountBankAccountCurrency), 2) AS Monto_de_registros_en_C4B,
# MAGIC     COUNT(MainAccount) AS Numero_de_registros_en_Banorte,
# MAGIC     ROUND(SUM(TransactionAmount), 2) AS Monto_de_registros_en_Banorte,
# MAGIC     ABS(COUNT(BankAccountID) - COUNT(MainAccount)) AS Numero_de_registros_de_la_diferencia_detectada_entre_CB4_y_Banorte,
# MAGIC     ABS(SUM(AmountBankAccountCurrency) - SUM(TransactionAmount)) AS Monto_de_los_registros_de_la_diferencia_detectada_entre_CB4_y_Banorte,
# MAGIC     TransactionStatus
# MAGIC   FROM bronze.banorte_spei
# MAGIC   FULL JOIN bronze.c4b_spei 
# MAGIC     ON bronze.banorte_spei.TrackingKey = bronze.c4b_spei.End2EndID
# MAGIC     AND bronze.banorte_spei.Reference = bronze.c4b_spei.ReferenceNumber
# MAGIC     AND bronze.banorte_spei.MainAccount = bronze.c4b_spei.BankAccountID
# MAGIC     AND CASE
# MAGIC         WHEN bronze.banorte_spei.ReceptionService = 'EMISION SPEI' THEN '500402'
# MAGIC         WHEN bronze.banorte_spei.ReceptionService = 'SPEI RECIBIDO' THEN '550403'
# MAGIC         END = bronze.c4b_spei.TransactionTypeCode
# MAGIC     --AND bronze.banorte_spei.DateAdded = bronze.c4b_spei.ValueDate
# MAGIC   WHERE
# MAGIC     DateAdded = '03/01/2022'
# MAGIC   GROUP BY
# MAGIC     Fecha,
# MAGIC     TransactionStatus
# MAGIC ```

# COMMAND ----------

# MAGIC %md 
# MAGIC #### a. Subldedger (FPSL)

# COMMAND ----------

data_src   = 'subledger'
pre_files  = dirfiles_df(f"{at_conciliations}/{data_src}", spark)
ldgr_files = process_files(pre_files, data_src)
ldgr_files.sort_values('date', ascending=False).filter()

# COMMAND ----------

# MAGIC %md
# MAGIC #### b. Cloud Banking (C4B)

# COMMAND ----------

data_src  = 'cloud-banking'
pre_files = dirfiles_df(f"{at_conciliations}/{data_src}", spark)
c4b_files = process_files(pre_files, data_src)
c4b_files.sort_values('date', ascending=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### c. SPEI-GFB

# COMMAND ----------

# MAGIC %md 
# MAGIC **Nota**  
# MAGIC + Le llamamos _ledger_ a la fuente de transacciones SPEI de Banorte (GFB), pero no es técnicamente un registro contable.  
# MAGIC   Esta fuente contiene el backlog de las transferencias SPEI.  
# MAGIC + La clave `900002` significa algo muy importante, es el número que nos asigna Banorte.  
# MAGIC + Sobre la hora, en el ejercicio inicial nos da todo `160323`.  
# MAGIC   Esto es por ser SPEI indirecto, pero cambiará cuando tengamos todo en SPEI directo.  

# COMMAND ----------

pre_files_1   = dirfiles_df(at_spei_ledger, spark)
speigfb_files = process_files(pre_files_1, 'spei-ledger')
speigfb_files.sort_values('date', ascending=False)


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
# MAGIC + Revisar ruta de archivos:  `CuSn/ops/core-banking/conciliations/spei`.  

# COMMAND ----------

pre_files_1 = dirfiles_df(at_spei_banking, spark)
pre_files_0 = process_files(pre_files_1, 'spei-banking')
speic4b_files = pre_files_0.loc[pre_files_0['key1'] == 'CONCILIACION']

pre_files_0.sort_values('date', ascending=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2. Preparación de fuentes

# COMMAND ----------

# MAGIC %md  
# MAGIC 
# MAGIC Utilizamos una llave general de fecha `key_date` para leer los archivos de las 4 fuentes.  
# MAGIC Para cada fuente seguimos el procedimiento:  
# MAGIC     - Identificar un archivo, y sólo uno, con la fecha proporcionada.  
# MAGIC     - Leer los datos de acuerdo a las especificaciones definidas en la sección anterior, y mostrar la tabla correspondiente.  
# MAGIC     - Definir modificaciones de acuerdo a los propios reportes de conciliación, y aplicarlos para alistar las tablas.  

# COMMAND ----------

def read_source_table(src_key, dir_df, date_key, output=None): 
    src_path = get_match_path(src_key, dir_df, date_key)
    if src_path is None: 
        return None
    
    if output == 0: 
        table_0 = (spark.read.format('text')
            .load(src_path))
        return table_0
        
    table_1  = (spark.read.format('csv')
        .options(**tsv_options[src_key])
        .schema(schemas[src_key])
        .load(src_path)
        .select(*renamers[src_key]))
    
    trim_str = {a_col: F.trim(a_col) 
        for a_col, a_type in base_cols[src_key].items() if a_type == 'str'}
    table_11 = with_columns(table_1, trim_str)
    
    if output == 1: 
        return table_11
    
    table_2 = with_columns(table_11, read_cols[src_key])
    if output == 2: 
        return table_2
    
    table_3 = with_columns(table_2, mod_cols[src_key])
    if output == 3 or output is None: 
        return table_3


# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Subledger (FPSL)

# COMMAND ----------

# Columnas: [txn_valid, num_cuenta, cuenta_fpsl, clave_trxn, moneda, monto_trxn]

ldgr_tbl = read_source_table('subledger', ldgr_files, key_date_ops)

if ldgr_tbl is not None: 
    ldgr_grp = (ldgr_tbl
        .filter(F.col('txn_valid'))
        .fillna(0, subset='monto_txn')
        .groupby('num_cuenta', 'clave_txn', 'moneda', 
                 'cuenta_fpsl') # Esta columna es de referencia, supuestamente 1-a-1 con NUM_CUENTA. 
        .agg(F.count('*').alias('fpsl_num_txns'), 
             F.round(F.sum(F.col('monto_txn')), 2).alias('fpsl_monto'))) 
    
    display(ldgr_tbl)
else: 
    ldgr_grp = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Cloud Banking (C4B)

# COMMAND ----------

c4b_tbl = read_source_table('cloud-banking', c4b_files, key_date_ops)

if c4b_tbl is not None: 
    c4b_grp = (c4b_tbl
        .filter(F.col('txn_valid'))  
        .fillna(0, subset='monto_txn')
        .groupby('num_cuenta', 'clave_txn', 'moneda', 
                 'tipo_txn')  # Columna de referencia, se asume 1-a-1 con TIPO_TRXN
        .agg(F.count('*').alias('c4b_num_txns'), 
             F.round(F.sum(F.col('monto_txn')), 2).alias('c4b_monto'))) 

    display(c4b_grp)
else: 
    c4b_grp = None


# COMMAND ----------

# MAGIC %md 
# MAGIC ### c. SPEI-GFB

# COMMAND ----------

speigfb_tbl = read_source_table('spei-ledger', speigfb_files, key_date_spei)

if speigfb_tbl is not None: 
    speigfb_grp = (speigfb_tbl
        .filter(F.col('txn_valid'))
        .groupby('ref_num', 'account_num', 'txn_type_code')
        .agg(F.count('*').alias('gfb_num_txns'), 
             F.round(F.sum('txn_amount'), 0).alias('gfb_monto')))

    display(speigfb_tbl)
else: 
    speigfb_grp = None


# COMMAND ----------

# MAGIC %md
# MAGIC ### d. SPEI-C4B

# COMMAND ----------

speic4b_tbl = read_source_table('spei-banking', speic4b_files, key_date_spei)

if speic4b_tbl is not None: 
    speic4b_grp = (speic4b_tbl
        .filter(F.col('txn_valid'))
        .groupby('ref_num', 'account_num', 'txn_type_code')
        .agg(F.count('*').alias('c4b_num_txns'), 
             F.round(F.sum('txn_amount'), 0).alias('c4b_monto')))

    display(speic4b_tbl)
else: 
    speic4b_grp = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reportes y escrituras

# COMMAND ----------

dev = True
if dev: 
    report_specs = {
        '036': {
            'join' : ['num_cuenta', 'clave_txn', 'moneda'], 
            'with_cols': F.when((F.col('fpsl_num_txns') == F.col('c4b_num_txns')) 
                     & (F.col('fpsl_monto') + F.col('c4b_monto') == 0), 'valida')
                 .when((F.col('fpsl_num_txns') == F.col('c4b_num_txns')) 
                     & (F.col('fpsl_monto') - F.col('c4b_monto') == 0), 'opuesta')
                 .when((F.col( 'c4b_num_txns') == 0) | (F.col( 'c4b_num_txns').isNull()), 'c4b')
                 .when((F.col('fpsl_num_txns') == 0) | (F.col('fpsl_num_txns').isNull()), 'fpsl')
                 .otherwise('indeterminada')}}

    def conciliate_tables(a_df, b_df, key): 
        pass

# COMMAND ----------

# MAGIC %md 
# MAGIC ### a. (036) Operativa

# COMMAND ----------

# write_036 = 
dir_036  = f"{at_reports}/operational"

try:
    base_036 = (c4b_grp
        .join(ldgr_grp, how='full', on=['num_cuenta', 'clave_txn', 'moneda'])
        .withColumn('check_key', 
            F.when((F.col('fpsl_num_txns') == F.col('c4b_num_txns')) 
                 & (F.col('fpsl_monto') + F.col('c4b_monto') == 0), 'valida')
             .when((F.col('fpsl_num_txns') == F.col('c4b_num_txns')) 
                 & (F.col('fpsl_monto') == F.col('c4b_monto')),     'opuesta')
             .when((F.col( 'c4b_num_txns') == 0) | (F.col( 'c4b_num_txns').isNull()), 'c4b')
             .when((F.col('fpsl_num_txns') == 0) | (F.col('fpsl_num_txns').isNull()), 'fpsl')
             .otherwise('indeterminada')))

    discrp_036 = (base_036
        .filter(F.col('check_key') != 'valida'))

    fpsl_036 = (base_036
        .filter(F.col('check_key').isin(['fpsl', 'indeterinada'])) 
        .join(ldgr_tbl, how='left', 
            on=['num_cuenta', 'clave_txn', 'moneda', 'cuenta_fpsl']))

    c4b_036 = (base_036
        .filter(F.col('check_key').isin(['c4b', 'indeterminada']))
        .join(how='left', on=['num_cuenta', 'clave_txn', 'moneda', 'tipo_txn'], 
            other=c4b_tbl))
    
    write_datalake(base_036, spark=spark, overwrite=True, 
            a_path=f"{dir_036}/compare/{key_date_ops}_036_comparativo.csv")
    write_datalake(discrp_036, spark=spark, overwrite=True, 
            a_path=f"{dir_036}/discrepancies/{key_date_ops}_036_discrepancias.csv")
    write_datalake(fpsl_036, spark=spark, overwrite=True,
            a_path=f"{dir_036}/subledger/{key_date_ops}_036_fpsl.csv")
    write_datalake(c4b_036, spark=spark, overwrite=True,
            a_path=f"{dir_036}/cloud-banking/{key_date_ops}_036_c4b.csv")
except: 
    base_036 = None

# COMMAND ----------

if base_036 is not None: 
    display(base_036)

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. (063) SPEI

# COMMAND ----------

# write_063 = True
dir_063 = f"{at_reports}/electronic-transfers/"

try: 
    base_063 = (speic4b_grp
        .join(speigfb_grp, how='full', on=['ref_num', 'account_num', 'txn_type_code'])
        .withColumn('check_key', 
            F.when((F.col('gfb_num_txns') == F.col('c4b_num_txns')) 
                 & (F.col('gfb_monto')    == F.col('c4b_monto')), 'valida')
             .when((F.col('gfb_num_txns') == 0) | (F.col('gfb_num_txns').isNull()), 'gfb')
             .when((F.col('c4b_num_txns') == 0) | (F.col('c4b_num_txns').isNull()), 'c4b')
             .otherwise('indeterminada')))

    discrp_063 = (base_063
        .filter(F.col('check_key') != 'valida'))

    gfb_063 = (base_063
        .filter(F.col('check_key').isin(['gfb', 'indeterminada']))
        .join(speigfb_tbl, how='left', 
            on=['ref_num', 'account_num', 'txn_type_code']))

    c4b_063 = (base_063
        .filter(F.col('check_key').isin(['c4b', 'indeterminada']))
        .join(speigfb_tbl, how='left', 
            on=['ref_num', 'account_num', 'txn_type_code']))

    write_datalake(discrp_063, spark=spark, 
            a_path=f"{dir_063}/compare/{key_date_spei}_063_comparativo.csv")
    write_datalake(discrp_063, spark=spark, 
            a_path=f"{dir_063}/discrepancies/{key_date_spei}_063_discrepancias.csv")
    write_datalake(gfb_063, spark=spark, 
            a_path=f"{dir_063}/financial/{key_date_spei}_063_spei-gfb.csv")
    write_datalake(c4b_063, spark=spark, 
            a_path=f"{dir_063}/cloud-banking/{key_date_spei}_063_spei-c4b.csv")

except:
    base_063 = None

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Resultados
# MAGIC 
# MAGIC Aquí vemos algunos de los resultados claves de le ejecución:  
# MAGIC - Procesos que no se concluyeron.  
# MAGIC - Resumenes de los que sí.  

# COMMAND ----------

print(f"Dias de reporte (036:{key_date_ops}, 063:{key_date_spei})")

if ldgr_grp is None: 
    print(f"No se encontró FPSL correspondiente a {key_date_ops}.")

if c4b_grp is None: 
    print(f"No se encontró C4B correspondiente a {key_date_ops}.")

if speigfb_grp is None: 
    print(f"No se encontró SPEI-GFB correspondiente a {key_date_spei}.")

if speic4b_grp is None: 
    print(f"No se encontró SPEI-GFB correspondiente a {key_date_spei}.")

if base_036 is None: 
    print(f"No se concluyó el reporte 036.")

if base_063 is None: 
    print(f"No se concluyó el reporte 063.")
