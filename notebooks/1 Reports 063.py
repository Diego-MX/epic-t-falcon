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

now_mx = dt.now(timezone('America/Mexico_City'))
yday_ish = now_mx.date() - delta(days=1)  
yday_ish = date(2023, 7, 28)   

# ya se hizo un pequeño desorden cuando empezaron a cambiar fechas, y áreas bancarias.  
which_files = {
    'spei-banking'  : {'date': yday_ish, 'key2': 'CC4B3'},  # 'CB15'
    'spei-ledger'   : {'date': yday_ish, 'key' : '900002'}} # '900002'

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
SPEI-C4B    : {at_banking}
SPEI-GFB    : {at_ledger}
Reports     : {to_reports}
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

# For Subledger

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
        'txn_type': 'str', 'txn_postdate': 'ts', 
        'AMOUNT_LOCAL': 'str', 'AMOUNT_CENTS': 'str',  
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
    'read': {
        'amount_local' : F.concat_ws('.',  
                F.regexp_replace(F.col('AMOUNT_LOCAL'), '\.', ''), 
                F.col('AMOUNT_CENTS')).cast(T.DoubleType()), 
        'txn_postdate' : F.col('txn_postdate').cast(T.DateType()), 
        'value_date'   : F.col('value_date').cast(T.DateType()), 
        'e2e_type'     : F.regexp_extract('end_to_end', ref_regex, 1), 
        'e2e_date'     : F.regexp_extract('end_to_end', ref_regex, 2),  
        'e2e_num2'     : F.regexp_extract('end_to_end', ref_regex, 3)}, 
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
    'spei-ledger'  : spei_gfb.get('read', {}), 
    'spei-banking' : spei_c4b.get('read', {}),     
}

mod_cols = {
    'spei-ledger'  : spei_gfb.get('mod', {}), 
    'spei-banking' : spei_c4b.get('mod', {}), 
}


# General
tsv_options = {
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
    'int' : T.IntegerType, 'long': T.LongType,   'ts'   : T.TimestampType, 
    'str' : T.StringType,  'dbl' : T.DoubleType, 'date' : T.DateType, 
    'bool': T.BooleanType, 'flt' : T.FloatType,  'null' : T.NullType}

schemas = {
    'spei-ledger'   : T.StructType([
            T.StructField(kk, schema_types[vv](), True) 
            for kk, vv in base_cols['spei-ledger'].items()]), 
    'spei-banking'  : T.StructType(
            T.StructField(kk, schema_types[vv](), True) 
            for kk, vv in base_cols['spei-banking'].items()), 
}

renamers = {
    'spei-ledger'   : [kk for kk in base_cols['spei-ledger']], 
    'spei-banking'  : [kk for kk in base_cols['spei-banking']], 
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
speigfb_files.sort_values('date', ascending=False).query(f"date >= '{since_when}'")


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

def read_source_table(src_key, dir_df, file_keys, output=None, verbose=False): 
    q_str = ' & '.join(f"({k} == '{v}')" 
        for k, v in file_keys.items())
    
    if verbose: 
        print(q_str)

    path_df = dir_df.query(q_str)
    if path_df.shape[0] != 1: 
        print(f"File keys match is not unique.")
        return None
    
    src_path = path_df['path'].iloc[0]        

    if output == 0: 
        table_0 = (spark.read.format('text')
            .load(src_path))
        return table_0
        
    table_0 = EpicDF(spark
            .read.format('csv')
            .options(**tsv_options[src_key])
            .schema(schemas[src_key])
            .load(src_path))
    
    table_1 = table_0.select(*renamers[src_key])
    
    trim_str = {a_col: F.trim(a_col) 
        for a_col, a_type in base_cols[src_key].items() if a_type == 'str'}
    table_11 = table_1.with_column_plus(trim_str)
    
    if output == 1: 
        return table_11
    
    table_2 = table_11.with_column_plus(read_cols[src_key])
    if output == 2: 
        return table_2
    
    table_3 = table_2.with_column_plus(mod_cols[src_key])
    if output == 3 or output is None: 
        return table_3


# COMMAND ----------

# MAGIC %md 
# MAGIC ### c. SPEI-GFB

# COMMAND ----------

speigfb_tbl = read_source_table('spei-ledger', speigfb_files, which_files['spei-ledger'])

if speigfb_tbl is not None: 
    speigfb_grp = (speigfb_tbl
        .filter(F.col('txn_valid'))
        .groupby('ref_num', 'account_num', 'txn_type_code')
        .agg(F.count('*').alias('gfb_num_txns'), 
             F.round(F.sum('txn_amount'), 0).alias('gfb_monto')))

    speigfb_tbl.display()
else: 
    speigfb_grp = None


# COMMAND ----------

# MAGIC %md
# MAGIC ### d. SPEI-C4B

# COMMAND ----------

speic4b_tbl = read_source_table('spei-banking', speic4b_files, which_files['spei-banking'])

if speic4b_tbl is not None: 
    speic4b_grp = (speic4b_tbl
        .filter(F.col('txn_valid'))
        .groupby('ref_num', 'account_num', 'txn_type_code')
        .agg(F.count('*').alias('c4b_num_txns'), 
             F.round(F.sum('txn_amount'), 0).alias('c4b_monto')))

    speic4b_tbl.display()
else: 
    speic4b_grp = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reportes y escrituras

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. (063) SPEI

# COMMAND ----------

# write_063 = True
dir_063 = f"{to_reports}/electronic-transfers/"

try: 
    base_063 = (speic4b_grp
        .join(speigfb_grp, how='full', on=['ref_num', 'account_num', 'txn_type_code'])
        .withColumn('check_key', 
            F.when((F.col('gfb_num_txns') == F.col('c4b_num_txns')) 
                &  (F.col('gfb_monto')    == F.col('c4b_monto')), 'valida')
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

print(f"""
    Reports: {to_reports}
    DIR_063: {dir_063}""")

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
    print(f"{kk}\t:{dumps2(vv)}")

if speigfb_grp is None: 
    print(f"No se encontró SPEI-GFB correspondiente a {dumps2(which_files['spei-ledger'])}.")

if speic4b_grp is None: 
    print(f"No se encontró SPEI-C4B correspondiente a {dumps2(which_files['spei-banking'])}.")

if base_063 is None: 
    print(f"No se concluyó el reporte 063.")
