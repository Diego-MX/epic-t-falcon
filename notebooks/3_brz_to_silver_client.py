# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

display(dbutils.fs.ls('abfss://silver@stlakehyliaqas.dfs.core.windows.net/ops/card-management/datasets/'))

# COMMAND ----------

client = spark.read.format("delta").load("abfss://silver@stlakehyliaqas.dfs.core.windows.net/ops/card-management/datasets/damna/delta")

# COMMAND ----------

client.display()

# COMMAND ----------

account = spark.read.format("delta").load("abfss://silver@stlakehyliaqas.dfs.core.windows.net/ops/card-management/datasets/dambs/delta")
account.display()

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
