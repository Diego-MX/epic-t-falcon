-- Databricks notebook source
-- MAGIC %python
-- MAGIC import datetime
-- MAGIC import pytz
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC currentdate = datetime.datetime.now(pytz.timezone('America/Mexico_City')).strftime("%Y%m")
-- MAGIC outputPath = "/dbfs/mnt/lakehylia-silver/ops/regulatory/card-management/transformation-layer/FT_R2422_"+currentdate+"01.csv"
-- MAGIC outputPath

-- COMMAND ----------

CREATE WIDGET TEXT pathR2422 DEFAULT "/mnt/lakehylia-silver/ops/regulatory/card-management/transformation-layer/r2422";
CREATE WIDGET TEXT pathCsv DEFAULT "/dbfs/mnt/lakehylia-silver/ops/regulatory/card-management/transformation-layer/FT_R2422.csv";

-- COMMAND ----------

SELECT * FROM bronze.damna;

-- COMMAND ----------

SELECT * FROM bronze.dambs;

-- COMMAND ----------

SELECT SUM(NumberUnblockedCards), (SUM(CurrentBalance)/SUM(NumberUnblockedCards)) FROM bronze.dambs

-- COMMAND ----------

SELECT
  *
FROM
  bronze.damna
  INNER JOIN bronze.dambs ON bronze.damna.CustomerNumber = bronze.dambs.CustomerNumber

-- COMMAND ----------

SELECT
  CONCAT(YEAR(bdan.Date),MONTH(bdan.Date)) AS MES_INFORMACION,
  COUNT(GenderCode) FILTER(WHERE GenderCode = 1) AS CONTRACT_ACTIVE_DEBIT_CARD_MALE,
  COUNT(GenderCode) FILTER(WHERE GenderCode = 2) AS CONTRACT_ACTIVE_DEBIT_CARD_FEMALE,
  COUNT(GenderCode) FILTER(WHERE GenderCode = 0) AS CONTRACT_ACTIVE_DEBIT_CARD_NOT_SPECIFIED
FROM
  bronze.damna AS bdan
  JOIN bronze.dambs AS bdam ON bdan.CustomerNumber = bdam.CustomerNumber
WHERE
  MONTH(bdan.Date) = MONTH(NOW() - INTERVAL 1 MONTH)
  GROUP BY MES_INFORMACION

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.r2422
USING DELTA
LOCATION "$pathR2422" AS (
SELECT
  CONCAT(YEAR(bdan.Date),MONTH(bdan.Date)) AS MES_INFORMACION,
  COUNT(GenderCode) FILTER(WHERE GenderCode = 1) AS CONTRACT_ACTIVE_DEBIT_CARD_MALE,
  COUNT(GenderCode) FILTER(WHERE GenderCode = 2) AS CONTRACT_ACTIVE_DEBIT_CARD_FEMALE,
  COUNT(GenderCode) FILTER(WHERE GenderCode = 0) AS CONTRACT_ACTIVE_DEBIT_CARD_NOT_SPECIFIED
FROM
  bronze.damna AS bdan
  INNER JOIN bronze.dambs AS bdam ON bdan.CustomerNumber = bdam.CustomerNumber
WHERE
  MONTH(bdan.Date) = MONTH(NOW() - INTERVAL 1 MONTH)
  GROUP BY MES_INFORMACION
)

-- COMMAND ----------

SELECT * FROM silver.r2422

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pathDelta = dbutils.widgets.get("pathR2422")
-- MAGIC df = spark.read.format("delta").load(pathDelta)
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pathFile = outputPath
-- MAGIC pd = df.toPandas()
-- MAGIC pd.to_csv(pathFile, header=True, index=False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/mnt/lakehylia-silver/ops/regulatory/card-management/transformation-layer/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()
