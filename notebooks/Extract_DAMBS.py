# Databricks notebook source
from pyspark.sql.functions import monotonically_increasing_id, substring, current_date
from pyspark.sql.types import IntegerType, FloatType

# COMMAND ----------

# MAGIC %sql
# MAGIC SET TIME ZONE '-06:00';

# COMMAND ----------

origen ='dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/FilesUpload/DAMBS/DAMBS.txt'
path_delta = "/mnt/lakehylia-bronze/ops/regulatory/card-management/dambs"
path_procesados = 'dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/FilesUpload/DAMBS/DAMBS_Processed/'
ts=spark.sql(""" select current_timestamp() - INTERVAL 6 HOUR as ctime """).collect()[0]["ctime"]
dateFormat = "%Y%m%d"
dambsdia = path_procesados + 'DAMBS_' + ts.strftime(dateFormat) + '.txt'

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

df2 = df1.withColumn('AccountNumber',substring('value',4,19))\
         .withColumn('CustomerNumber', substring('value',46,19))\
         .withColumn('CardExpirationDate',substring('value',522,8))\
         .withColumn('NumberUnblockedCards',substring('value',603,5).cast(IntegerType()))\
         .withColumn('CurrentBalanceSign',substring('value',2545,1))\
         .withColumn('CurrentBalance',substring('value',2546,17).cast(FloatType()))

# COMMAND ----------

df2 = df2.drop('value', 'index').withColumn('date',current_date())

# COMMAND ----------

df2.write.format("delta").mode("append").save(path_delta)

# COMMAND ----------

# MAGIC %sh
# MAGIC find /dbfs/mnt/lakehylia-bronze/ops/regulatory/card-management -type f -name "*DAMBS*ZIP" -delete

# COMMAND ----------

dbutils.fs.mv(origen, dambsdia )
