# Databricks notebook source
# MAGIC %md
# MAGIC ## Descripción
# MAGIC Instalar requerimientos de librerías para los _notebooks_. 

# COMMAND ----------

# MAGIC %pip install -q -r ../reqs_dbks.txt

# COMMAND ----------

epicpy_tag = 'gh-1.2'  # pylint: disable=invalid-name
# pylint: disable=wrong-import-position,wrong-import-order
# pylint: disable=no-name-in-module,unspecified-encoding,import-error

# COMMAND ----------

import subprocess

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
import yaml

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

with open("../user_databricks.yml", 'r') as _f:
    u_dbks = yaml.safe_load(_f)

epicpy_load = {
    'url'   : 'github.com/Bineo2/data-python-tools.git', 
    'branch': epicpy_tag, 
    'token' : dbutils.secrets.get(u_dbks['dbks_scope'], u_dbks['dbks_token'])}

url_call = "git+https://{token}@{url}@{branch}".format(**epicpy_load)
subprocess.check_call(['pip', 'install', url_call])
