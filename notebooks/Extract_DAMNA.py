# Databricks notebook source
from pyspark.sql.functions import monotonically_increasing_id, substring, date_format, current_date

# COMMAND ----------

# MAGIC %sql
# MAGIC SET TIME ZONE '-06:00';

# COMMAND ----------

origen ='dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/FilesUpload/DAMNA/DAMNA.txt'
path_delta = "/mnt/lakehylia-bronze/ops/regulatory/card-management/damna"
path_procesados = 'dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/FilesUpload/DAMNA/DAMNA_Processed/'
ts=spark.sql(""" select current_timestamp() - INTERVAL 6 HOUR as ctime """).collect()[0]["ctime"]
dateFormat = "%Y%m%d"
damnadia = path_procesados + 'DAMNA_' + ts.strftime(dateFormat) + '.txt'

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

df2 = df1.withColumn('CustomerNumber', substring('value',4,19))\
           .withColumn('Name',substring('value',638,40))\
           .withColumn('Municipality',substring('value',1065,30))\
           .withColumn('GenderCode',substring('value',958,1))\
           .withColumn('City',substring('value',918,30))\
           .withColumn('NameTypeIndicator',substring('value',635,1))

# COMMAND ----------

df2 = df2.drop('value', 'index').withColumn('date',current_date())

# COMMAND ----------

df2.write.format("delta").mode("append").save(path_delta)

# COMMAND ----------

# MAGIC %sh
# MAGIC find /dbfs/mnt/lakehylia-bronze/ops/regulatory/card-management -type f -name "*DAMNA*ZIP" -delete

# COMMAND ----------

dbutils.fs.mv(origen, damnadia )
