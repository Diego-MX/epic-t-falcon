# Databricks notebook source
# MAGIC %md
# MAGIC # Descripción
# MAGIC
# MAGIC Se crean tablas `bronze.{extract}` para `{extract}` uno de `dambs`, `damna`, `atptx`.  
# MAGIC La ubicación correspondiente es: `/mnt/lakehylia-bronze/ops/regulatory/card-management/{extract}`.

# COMMAND ----------

from importlib import reload
import config
reload(config)
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
# COMMAND ----------

from config import ENV, RESOURCE_SETUP, DATALAKE_PATHS as paths
resources = RESOURCE_SETUP[ENV]

abfss_loc = paths['abfss'].format('silver', resources['storage'])
at_datasets = f"{abfss_loc}/{paths['datasets']}"


# COMMAND ----------

dbutils.fs.ls(at_datasets)
# ...> atpt, dambs, dambs2, dambsc, damna

# COMMAND ----------

dbutils.fs.rm(f"{at_datasets}/dambsc", True)
