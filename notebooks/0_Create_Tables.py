# Databricks notebook source
# MAGIC %md
# MAGIC # Descripción
# MAGIC 
# MAGIC Se crean tablas `bronze.{extract}` para `{extract}` uno de `dambs`, `damna`, `atptx`.  
# MAGIC La ubicación correspondiente es: `/mnt/lakehylia-bronze/ops/regulatory/card-management/{extract}`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.dambs                
# MAGIC (                   
# MAGIC   AccountNumber STRING,
# MAGIC   CustomerNumber STRING,
# MAGIC   CardExpirationDate STRING,
# MAGIC   NumberUnblockedCards INT,
# MAGIC   CurrentBalanceSign STRING,
# MAGIC   CurrentBalance FLOAT,
# MAGIC   date DATE
# MAGIC )
# MAGIC USING DELTA 
# MAGIC PARTITIONED BY (Date)
# MAGIC LOCATION '/mnt/lakehylia-bronze/ops/regulatory/card-management/dambs';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.damna                 
# MAGIC (                   
# MAGIC   CustomerNumber STRING,
# MAGIC   Name STRING,
# MAGIC   Municipality STRING,
# MAGIC   GenderCode STRING,
# MAGIC   City STRING,
# MAGIC   NameTypeIndicator STRING,
# MAGIC   date DATE
# MAGIC )
# MAGIC USING DELTA 
# MAGIC PARTITIONED BY (Date)
# MAGIC LOCATION '/mnt/lakehylia-bronze/ops/regulatory/card-management/damna';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.atptx                 
# MAGIC (                   
# MAGIC   AccountNumber STRING,
# MAGIC   EffectiveDate STRING,
# MAGIC   TransactionType STRING,
# MAGIC   TransactionSign STRING,
# MAGIC   TransactionCode STRING,
# MAGIC   TransactionAmountSign STRING,
# MAGIC   TransactionAmount STRING,
# MAGIC   AcceptorCategoryCode STRING,
# MAGIC   TransactionChannel INT,
# MAGIC   date DATE
# MAGIC )
# MAGIC USING DELTA 
# MAGIC PARTITIONED BY (Date)
# MAGIC LOCATION '/mnt/lakehylia-bronze/ops/regulatory/card-management/atptx';
