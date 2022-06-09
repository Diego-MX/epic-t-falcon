# Databricks notebook source
from pyspark.sql.functions import row_number, monotonically_increasing_id, substring, date_format, current_date, current_timestamp, date_add
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %sql
# MAGIC SET TIME ZONE '-06:00';

# COMMAND ----------

origen ='dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/FilesUpload/ATPTX/ATPTX.txt'
path_delta = "/mnt/lakehylia-bronze/ops/regulatory/card-management/atptx"
path_procesados = 'dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/FilesUpload/ATPTX/ATPTX_Processed/'
ts=spark.sql(""" select current_timestamp() - INTERVAL 6 HOUR  as ctime """).collect()[0]["ctime"]
dateFormat = "%Y%m%d"
atptxdia = path_procesados + 'ATPTX_' + ts.strftime(dateFormat) + '.txt'

# COMMAND ----------

type(origen)

# COMMAND ----------

# MAGIC %run ../configuration

# COMMAND ----------

df = spark.read.text(origen)

# COMMAND ----------

 df1 = df.coalesce(1)

# COMMAND ----------

df1 = df.withColumn("index", monotonically_increasing_id())

# COMMAND ----------

footer_index = df1.count() - 1

# COMMAND ----------

df1 = df1.filter((df1.index > 0) & (df1.index < footer_index))

# COMMAND ----------

df2 = df1.withColumn('AccountNumber', substring('value',4,19))\
           .withColumn('EffectiveDate',substring('value',32,8))\
           .withColumn('TransactionType',substring('value',40,1))\
           .withColumn('TransactionSign',substring('value',41,1))\
           .withColumn('TransactionCode',substring('value',42,5))\
           .withColumn('TransactionAmountSign',substring('value',47,1))\
           .withColumn('TransactionAmount',substring('value',48,17))\
           .withColumn('AcceptorCategoryCode',substring('value',157,5))\
           .withColumn('TransactionChannel',substring('value',766,2).cast(IntegerType()))

# COMMAND ----------

df2 = df2.drop('value', 'index').withColumn('date', current_date())

# COMMAND ----------

df2.write.format("delta").mode("append").save(path_delta)

# COMMAND ----------

# MAGIC %sh
# MAGIC find /dbfs/mnt/lakehylia-bronze/ops/regulatory/card-management -type f -name "*ATPTX*ZIP" -delete

# COMMAND ----------

dbutils.fs.mv(origen, atptxdia )
