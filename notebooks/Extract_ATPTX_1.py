# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
from config import ATPTX_SETUP as atptx
from config import ATPTX_width_column_defs as atptx_col

# COMMAND ----------

print(atptx_col)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET TIME ZONE '-06:00';

# COMMAND ----------

origen = atptx['paths']['origen']
delta = atptx['paths']['delta']
procesados = atptx['paths']['procesados']
ts=spark.sql(""" select current_timestamp() - INTERVAL 6 HOUR  as ctime """).collect()[0]["ctime"]
dateFormat = atptx['dateformat']
atptxdia = procesados + 'ATPTX_' + ts.strftime(dateFormat) + '.txt'

# COMMAND ----------

df = spark.read.text(origen)

# COMMAND ----------

display(df)

# COMMAND ----------

 df = (df.coalesce(1)
          .withColumn("index", monotonically_increasing_id()))

# COMMAND ----------

display(df)

# COMMAND ----------

footer_index = df.count() - 1

# COMMAND ----------

df = df.filter((df.index > 0) & (df.index < footer_index))

# COMMAND ----------

display(df)

# COMMAND ----------

df = (df.select("value",*[substring("value",*v).alias(k) 
                     for k,v in atptx_col.items()])
             .withColumn('TransactionChannel',col('TransactionChannel').cast(IntegerType()))
             .withColumn('date', current_date())
             .drop('value')
       )

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("append").save(delta)

# COMMAND ----------

# MAGIC %sh
# MAGIC find /dbfs/mnt/lakehylia-bronze/ops/regulatory/card-management -type f -name "*ATPTX*ZIP" -delete

# COMMAND ----------

dbutils.fs.mv(origen, atptxdia )
