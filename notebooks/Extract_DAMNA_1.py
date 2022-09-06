# Databricks notebook source
from pyspark.sql.functions import *
from config import DAMNA_SETUP as damna
from config import DAMNA_width_column_defs as damna_col

# COMMAND ----------

# MAGIC %sql
# MAGIC SET TIME ZONE '-06:00';

# COMMAND ----------

origen = damna['paths']['origen']
delta = damna['paths']['delta']
procesados = damna['paths']['procesados']
dateFormat = damna['dateformat']
ts=spark.sql(""" select current_timestamp() - INTERVAL 6 HOUR as ctime """).collect()[0]["ctime"]
damnadia = procesados + 'DAMNA_' + ts.strftime(dateFormat) + '.txt'

# COMMAND ----------

df = spark.read.text(origen)

# COMMAND ----------

 df = (df.coalesce(1)
          .withColumn("index", monotonically_increasing_id()))

# COMMAND ----------

footer_index = df.count() - 1

# COMMAND ----------

df = df.filter((df.index > 0) & (df.index < footer_index))

# COMMAND ----------

df = (df.select("value",*[substring("value",*v).alias(k) 
                     for k,v in damna_col.items()])
             .drop('value')
             .withColumn('date',current_date())
      )

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("append").save(delta)

# COMMAND ----------

# MAGIC %sh
# MAGIC find /dbfs/mnt/lakehylia-bronze/ops/regulatory/card-management -type f -name "*DAMNA*ZIP" -delete

# COMMAND ----------

dbutils.fs.mv(origen, damnadia )
