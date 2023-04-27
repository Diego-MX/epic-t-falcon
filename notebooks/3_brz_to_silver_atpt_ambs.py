# Databricks notebook source
import json
import pandas as pd
from datetime import date
from pathlib import Path
from delta.tables import DeltaTable as Δ
from pyspark.sql import (functions as F, types as T, Row, DataFrame as spk_DF, Window as W)

# COMMAND ----------

from importlib import reload
import config; reload(config)
from src import tools, utilities as utils; reload(tools); reload(utils)
from src import sftp_sources as sftp; reload(sftp)

from config import (ConfigEnviron,
    HASH_OFFSET,
    ENV, SERVER, RESOURCE_SETUP, 
    DATALAKE_PATHS as paths, 
    DELTA_TABLES as delta_keys)

resources = RESOURCE_SETUP[ENV]
app_environ = ConfigEnviron(ENV, SERVER, spark)
app_environ.sparktransfer_credential()

local_layouts = "/dbfs/FileStore/transformation-layer/layouts"

read_from = f"{paths['abfss'].format('bronze', resources['storage'])}/{paths['prepared']}"  
write_to  = f"{paths['abfss'].format('silver', resources['storage'])}/{paths['datasets']}"  


# Se requiere crear esta carpeta si no existe. 
# makedirs(tmp_layouts)

# COMMAND ----------

# Ver las carpetas de la ruta
display(dbutils.fs.ls('abfss://silver@stlakehyliaqas.dfs.core.windows.net/ops/card-management/datasets'))

# COMMAND ----------

# Revisar y guardar contenidos del ATPT
atpt = spark.read.format("delta").load("abfss://silver@stlakehyliaqas.dfs.core.windows.net/ops/card-management/datasets/atpt/delta").cache()
atpt.display()

# COMMAND ----------

# Revisar y guardar contenidos del AMBS
dambs = spark.read.format("delta").load("abfss://silver@stlakehyliaqas.dfs.core.windows.net/ops/card-management/datasets/dambs/delta").cache()
dambs.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de prototablas delta para las entidades faltantes

# COMMAND ----------

# DBTITLE 1,Link Savings Account Map
# MAGIC %sql
# MAGIC -- # Crear tabla del link sav_account_map
# MAGIC -- DROP TABLE lnk_sav_account_map;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS lnk_sav_account_map(
# MAGIC   hash_id STRING NOT NULL,
# MAGIC   source STRING,
# MAGIC   load_date_ts TIMESTAMP NOT NULL,
# MAGIC   load_date DATE NOT NULL,
# MAGIC   cms_bk STRING,
# MAGIC   cms_hash_id BIGINT,
# MAGIC   sap_bk STRING,
# MAGIC   sap_hash_id BIGINT
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparación de los Dataframes bronce

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data para el link_sav_account_map

# COMMAND ----------

dambs_acct_nbr_df = dambs.select("ambs_acct", "ambs_cust_nbr", "ambs_sav_acct_nbr", F.col("ambs_sav_rtng_nbr").cast('integer'), F.lit("MX"))
dambs_acct_nbr_df.display()

# COMMAND ----------

cms_source = 'fiserv.dambs'

link_sav_acct_map_df = (dambs_acct_nbr_df
                        .select((F.lit(None).cast('string')).alias('hash_id'),
                                F.lit(cms_source).alias('source'),
                                (F.current_timestamp()).alias('load_date_ts'),
                                (F.current_date()).alias('load_date'),
                                F.col('ambs_acct').alias('cms_bk'), # ID de cuenta fizerv
                                (F.xxhash64(F.col('ambs_acct'), F.lit(HASH_OFFSET)).alias('cms_hash_id')),
                                # F.col('ambs_sav_acct_nbr').alias('número de cuenta'), # Para revisar que la siguiente línea sea correcta
                                (F.when(F.col('ambs_sav_acct_nbr').isin(""), F.lit(None))
                                .otherwise(F.concat_ws('-', 'ambs_sav_acct_nbr', 'ambs_sav_rtng_nbr', 'MX'))
                                .alias('sap_bk')) # ID de cuenta SAP inferida
                                )
                        .withColumn('sap_hash_id', F.xxhash64(F.col('sap_bk'), F.lit(HASH_OFFSET)))
                        )
                        
link_sav_acct_map_df = link_sav_acct_map_df.withColumn('hash_id', F.sha2(F.concat_ws('|', *['cms_bk','sap_bk']), 256))

link_sav_acct_map_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge

# COMMAND ----------

link_dt = utils.retrieve_delta_table(spark, 'lnk_sav_account_map')
pre_row_count = link_dt.toDF().count()
print(f"Pre Row Count: {pre_row_count}")

link_dt.alias("current").merge(
    link_sav_acct_map_df.alias("updates"),
    "current.hash_id = updates.hash_id"
).whenNotMatchedInsertAll().execute()

pos_row_count = link_dt.toDF().count()
print(f"Pos Row Count: {pos_row_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notas Útiles

# COMMAND ----------

# DBTITLE 1,como obtener la cuenta SAP
#realizar trim
account.select("ambs_acct", "ambs_cust_nbr", "ambs_sav_acct_nbr", "ambs_sav_rtng_nbr", F.lit("MX")).where(F.col("ambs_sav_acct_nbr") != "").display()

# COMMAND ----------

transaction = spark.read.format("delta").load("abfss://silver@stlakehyliaqas.dfs.core.windows.net/ops/card-management/datasets/atpt/delta")
transaction.display()

# COMMAND ----------

sap_client = spark.read.table("hub_client")

sap_client.display()
sap_client.groupBy("id_bk").count().where("count > 1").groupBy().sum("count").show()

# COMMAND ----------

sat_client_address = spark.read.table("sat_client_addr")

sat_client_address.display()
sat_client_address.where(F.col("end_date").isNull()).groupBy("hash_id").count().where("count > 1").groupBy().sum("count").show()

# COMMAND ----------

sat_client_attr = spark.read.table("sat_client_attrs")

sat_client_attr.display()
sat_client_attr.where(F.col("end_date").isNull()).groupBy("hash_id").count().where("count > 1").groupBy().sum("count").show()

# COMMAND ----------

sat_client_flags = spark.read.table("sat_client_flags")

sat_client_flags.display()
sat_client_flags.where(F.col("end_date").isNull()).groupBy("hash_id").count().where("count > 1").groupBy().sum("count").show()

# COMMAND ----------

sap_account = spark.read.table("hub_account_balance")
sap_account.display()
