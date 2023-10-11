# Databricks notebook source
# MAGIC %md
# MAGIC ... instalando requerimientos de librerías para los _notebooks_ ...

# COMMAND ----------

# MAGIC %pip install -q -r ../reqs_dbks.txt

# COMMAND ----------

epicpy_ref = 'gh-1.5'  # gh-1.4 pylint: disable=invalid-name 
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
    'ref'   : epicpy_ref, 
    'token' : dbutils.secrets.get(u_dbks['dbks_scope'], u_dbks['dbks_token'])}

url_call = "git+https://{token}@{url}@{ref}".format(**epicpy_load)
subprocess.check_call(['pip', 'install', url_call])

# COMMAND ----------

import epic_py
print(f"""EpicPy
Referencia\t: {epicpy_ref}
Version\t\t: {epic_py.__version__}""")
