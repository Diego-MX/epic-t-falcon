# Databricks notebook source
from pyspark.sql.functions import *
from config import DAMBS_SETUP as dambs
from config import DAMBS_width_column_defs as dambs_col
from pyspark.sql.types import IntegerType, FloatType

# COMMAND ----------

# MAGIC %sql
# MAGIC SET TIME ZONE '-06:00';

# COMMAND ----------

origen = dambs['paths']['origen']
delta = dambs['paths']['delta']
procesados = dambs['paths']['procesados']
ts=spark.sql(""" select current_timestamp() - INTERVAL 6 HOUR  as ctime """).collect()[0]["ctime"]
dateFormat = dambs['dateformat']
dambsdia = procesados + 'DAMBS_' + ts.strftime(dateFormat) + '.txt'

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
                     for k,v in dambs_col.items()])
             .withColumn('NumberUnblockedCards',col('NumberUnblockedCards').cast(IntegerType()))
             .withColumn('CurrentBalance',col('CurrentBalance').cast(FloatType()))
             .withColumn('date', current_date())
             .drop('value')
       )

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("append").save(delta)

# COMMAND ----------

# MAGIC %sh
# MAGIC find /dbfs/mnt/lakehylia-bronze/ops/regulatory/card-management -type f -name "*DAMBS*ZIP" -delete

# COMMAND ----------

dbutils.fs.mv(origen, dambsdia )
