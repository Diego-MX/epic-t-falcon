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

#%pip uninstall -y epic-py

# COMMAND ----------

from collections import OrderedDict
from datetime import datetime as dt, timedelta as delta, date
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession, functions as F, types as T
from pytz import timezone
import subprocess
import yaml

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

with open("../user_databricks.yml", 'r') as _f: 
    u_dbks = yaml.safe_load(_f)

epicpy_load = {
    'url'   : 'github.com/Bineo2/data-python-tools.git', 
    'branch': 'dev-diego', 
    'token' : dbutils.secrets.get(u_dbks['dbks_scope'], u_dbks['dbks_token'])}

url_call = "git+https://{token}@{url}@{branch}".format(**epicpy_load)
subprocess.check_call(['pip', 'install', url_call])

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 0.b Especificar fechas

# COMMAND ----------

fecha_manual = True

if fecha_manual:
    yday_ish = date(2023, 7, 27)   
    print(f"Revisa la fecha: [{yday_ish.strftime('%d-%b-%Y')}]") 
else: 
    now_mx = dt.now(timezone('America/Mexico_City'))
    yday_ish = now_mx.date() - delta(days=1)  


# ya se hizo un pequeño desorden cuando se cambiaron fechas y áreas bancarias.  
which_files = {
    'spei-banking' : {'date': yday_ish, 'key2': 'S4B1_01'},  # 'CB15'
    'spei-ledger'  : {'date': yday_ish, 'key' : '900002'}} # '900002'

key_date_ops  = yday_ish
key_date_spei = yday_ish

# COMMAND ----------

from importlib import reload
import epic_py; reload(epic_py)
import src; reload(src)
import config; reload(config)

from epic_py.delta import EpicDF
from epic_py.tools import dirfiles_df
from src.tools import write_datalake
from src.sftp_sources import process_files
from config import (t_agent, t_resources,
    #ConfigEnviron, ENV, SERVER, RESOURCE_SETUP, 
    DATALAKE_PATHS as paths)

t_storage = t_resources['storage']
t_permissions = t_agent.prep_dbks_permissions(t_storage, 'gen2')

λ_address = (lambda container, p_key : t_resources.get_resource_url(
    'abfss', 'storage', container=container, blob_path=paths[p_key]))

at_banking = λ_address('bronze', 'spei-c4b')
at_ledger  = λ_address('bronze', 'spei-gfb')
to_reports = λ_address('gold',   'reports2')

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

spei_gfb = {  # RECOIF
    'name': 'spei-ledger', 
    'alias': 'gfb', 
    'schema' : OrderedDict({
        'extra': 'str', 'cep_issuer': 'int_2', 'account_id': 'long', 'account_digital': 'long', 
        'clabe': 'long', 'receiver_channel': 'str', 'receiver_service': 'str', 
        'txn_amount': 'dbl', 'txn_status': 'str', 'rejection_reason': 'str', 
        'sender_bank': 'str', 'date_added': 'str', 'receiver_name': 'str', 
        'receiver_account': 'long', 
        'receiver_clabe': 'long', 'receiver_rfc': 'str', 'concept': 'str', 'reference': 'str', 
        'tracking_key': 'str', 'uuid': 'str', 'status': 'str', 'posting_date': 'date'}), 
    'options': dict(mode='PERMISIVE', sep=';', header=False), 
    'mutate': {
        'txn_valid'     : F.lit(True),             
        'ref_num'       : F.col('tracking_key'),   
        'account_num'   : F.col('account_digital'), 
        'txn_type_code' : F.when(F.col('receiver_service') == 'EMISION SPEI' , '500402')
                           .when(F.col('receiver_service') == 'SPEI RECIBIDO', '550403')
                           .otherwise(F.col('receiver_service')), 
        'txn_amount'    : F.col('txn_amount'), 
        'txn_status'    : F.col('status'),  
        'txn_date_gfb'  : F.to_date('posting_date', 'yyyy-MM-dd')}, 
    'match': {
        'where': [F.col('txn_valid')], 
        'by'   : ['ref_num', 'account_num', 'txn_type_code'], 
        'agg'  : {
            'gfb_status'  : F.first('txn_status'), 
            'gfb_num_txns': F.count('*'), 
            'gfb_monto'   : F.round(F.sum('txn_amount'), 2)}}}
    
spei_c4b = {
    'name': 'spei-banking', 
    'alias': 'c4b',
    'schema': OrderedDict({
        'account_c4b': 'str', 'txn_type_code': 'str', 
        'txn_type': 'str', 'txn_postdate': 'ts', 
        'amount_local': 'dbl', # 'AMOUNT_CENTS': 'str',  
        # Vienen en formato europeo así que lo separamos y ajustamos. 
        'currency': 'str', 'txn_id': 'str', 
        'type_code': 'int', 'type_name': 'str', 
        'pymt_type': 'str', 'pymt_type_code': 'str', 'comm_channel_code': 'str', 
        'comm_channel': 'str', 'acct_projection': 'str', 'product_id': 'str', 
        'holder_id': 'long', 'debit_indicator': 'str', 
        'value_date': 'ts', 'creation_user': 'long', 'release_user': 'long', 
        'counter_holder': 'str', 'counter_bank_id': 'int', 'counter_account_id': 'long', 
        'counter_account': 'int', 'counter_bank_country': 'str', 'pymt_order_id': 'str', 
        'pymt_item_id': 'str', 'ref_adjust_txn_id': 'str', 
        'cancel_document': 'str', 'pymt_notes': 'str', 'end_to_end': 'str', 
        'ref_number': 'str', 'txn_status_code': 'int', 
        'txn_status': 'str', 'acct_holder_name': 'str', 
        'acct_holder_tax_id': 'str', 'cntr_party_tax_id': 'str', 
        'fee_amount': 'dbl', 'fee_currency': 'str', 
        'vat_amount': 'dbl', 'vat_currency': 'str'}), 
    'options': dict(mode='PERMISIVE', sep=',', header=False, 
            dateFormat='d-M-y', timestampFormat='d/M/y H:m:s'), 
    'mutate': {
        'txn_postdate'  : F.col('txn_postdate').cast(T.DateType()), 
        'value_date'    : F.col('value_date').cast(T.DateType()),
        'txn_valid'     : F.col('txn_status') != 'Posting Canceled',  # Filtrar
        'ref_num'       : F.col('end_to_end'),
        'account_num'   : F.substring(F.col('account_c4b'), 1, 11).cast(T.LongType()), 
        'txn_type_code' : F.col('txn_type_code'),  
        'txn_amount'    : F.col('amount_local'), # Comparar. 
        'txn_date'      : F.col('txn_postdate'), 
        'txn_status'    : F.when(F.col('txn_status') == 'Posted', 'P')
                .otherwise(F.col('txn_status'))}, 
    'match': {
        'where': [F.col('txn_valid')], 
        'by'   : ['ref_num', 'account_num', 'txn_type_code'], 
        'agg'  : {
            'c4b_num_txns': F.count('*'), 
            'c4b_monto'   : F.round(F.sum('txn_amount'), 2)}}
}

spei_2 = {
    'schema': {  # old-ones
        'txn_code': 'str', 'txn_date': 'date', 'txn_time': 'str', 
        'txn_amount': 'dbl', 'ref_num': 'str', 'trck_code': 'str', 'sender_account': 'long', 
        'clabe_account': 'long', 'sender_name': 'str', 'sender_id': 'long', 'txn_description': 'str', 
        'receiver_bank_id': 'int', 'receiver_account': 'long', 'receiver_name': 'str', 
        'txn_geolocation': 'str', 'txn_status': 'int', 'resp_code': 'int'}, 
    'options': dict(mode='PERMISIVE', sep='|', header=False, dateFormat='d-M-y')
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### c. SPEI-GFB

# COMMAND ----------

# MAGIC %md 
# MAGIC **Nota**  
# MAGIC + Llamamos _ledger_ a la fuente de transacciones SPEI de Banorte (GFB), aunque no sea registro contable.  
# MAGIC   Esta fuente contiene el backlog de las transferencias SPEI.  
# MAGIC + La clave `900002` significa algo muy importante, es el número que nos asigna Banorte.  
# MAGIC + Sobre la hora, en el ejercicio inicial nos da todo `160323`.  
# MAGIC   Esto es por ser SPEI indirecto, pero cambiará cuando tengamos todo en SPEI directo.  

# COMMAND ----------

since_when = yday_ish - delta(5)

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

pre_files_1 = dirfiles_df(at_banking, spark)
pre_files_0 = process_files(pre_files_1, 'spei-banking')
speic4b_files = pre_files_0.loc[pre_files_0['key1'] == 'CONCILIACION']
print(which_files['spei-banking'])
pre_files_0.sort_values('date', ascending=False).query(f"date >= '{since_when}'")

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

from importlib import reload
from src import conciliation; reload(conciliation)

from src.conciliation import Sourcer, Conciliator as conciliate, get_source_path

# COMMAND ----------

# MAGIC %md 
# MAGIC ### c. SPEI-GFB

# COMMAND ----------

speigfb_files.sort_values('date', ascending=False)

# COMMAND ----------

speigfb_path = get_source_path(speigfb_files, which_files['spei-ledger'])

gfb_src  = Sourcer(speigfb_path, **spei_gfb)
gfb_prep = gfb_src.start_data(spark)
gfb_data = gfb_src.setup_data(gfb_prep)

gfb_data.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### d. SPEI-C4B

# COMMAND ----------

speic4b_files.sort_values('date')

# COMMAND ----------

speic4b_path = get_source_path(speic4b_files, which_files['spei-banking'])

c4b_src  = Sourcer(speic4b_path, **spei_c4b)
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

# write_063 = True
dir_063 = f"{to_reports}/electronic-transfers/"

check_txns = {
    'valida': (F.col('gfb_num_txns') == F.col('c4b_num_txns')) 
            & (F.col('gfb_monto'   ) == F.col('c4b_monto')), 
    'gfb'   : (F.col('gfb_num_txns') == 0) | (F.col('gfb_num_txns').isNull()), 
    'c4b'   : (F.col('c4b_num_txns') == 0) | (F.col('c4b_num_txns').isNull()), 
    'indeterminada': None}

report_063 = conciliate(gfb_src, c4b_src, check_txns)
base_063   = report_063.base_match(gfb_data, c4b_data)
diffs_063  = report_063.filter_checks(base_063, '~valida')
gfb_063    = report_063.filter_checks(base_063, ['gfb', 'indeterminada'])
c4b_063    = report_063.filter_checks(base_063, ['c4b', 'indeterminada'])

write_datalake(base_063, spark=spark, overwrite=True, 
        a_path=f"{dir_063}/compare/{key_date_ops}_063_comparativo.csv")
write_datalake(diffs_063, spark=spark, overwrite=True, 
        a_path=f"{dir_063}/discrepancies/{key_date_ops}_063_discrepancias.csv")
write_datalake(gfb_063, spark=spark, overwrite=True,
        a_path=f"{dir_063}/subledger/{key_date_ops}_063_gfb.csv")
write_datalake(c4b_063, spark=spark, overwrite=True,
        a_path=f"{dir_063}/cloud-banking/{key_date_ops}_063_c4b.csv")

base_063.display()

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
    print(f"{kk}: {dumps2(vv)}")

if gfb_data is None: 
    print(f"No se encontró SPEI-GFB correspondiente a {dumps2(which_files['spei-ledger'])}.")

if c4b_data is None: 
    print(f"No se encontró SPEI-C4B correspondiente a {dumps2(which_files['spei-banking'])}.")

if base_063 is None: 
    print(f"No se concluyó el reporte 063.")
