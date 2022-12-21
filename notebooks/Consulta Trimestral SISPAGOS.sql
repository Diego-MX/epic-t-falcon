-- Databricks notebook source
-- MAGIC %python
-- MAGIC %pip install python-dateutil

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime
-- MAGIC import math
-- MAGIC from dateutil.relativedelta import relativedelta  # requires python-dateutil
-- MAGIC 
-- MAGIC file_name = datetime(year=datetime.now().year, month=((math.floor(((datetime.now().month - 1) / 3) + 1) - 1) * 3) + 1, day=1).strftime("%Y%m%d")
-- MAGIC outputPath = "/dbfs/mnt/lakehylia-silver/ops/regulatory/card-management/transformation-layer/SISPAGOS_"+file_name+".csv"
-- MAGIC start_of_quarter = datetime(year=datetime.now().year, month=((math.floor(((datetime.now().month - 1) / 3) + 1) - 1) * 3) + 1, day=1)
-- MAGIC end_of_quarter = start_of_quarter + relativedelta(months=3, seconds=-1)
-- MAGIC print(outputPath)
-- MAGIC print(start_of_quarter)
-- MAGIC print(end_of_quarter)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import datetime
-- MAGIC import pytz
-- MAGIC from pyspark.sql.functions import *
-- MAGIC 
-- MAGIC currentdate = datetime.datetime.now(pytz.timezone('America/Mexico_City')).strftime("%Y%m%d")
-- MAGIC deltaPath = "/mnt/lakehylia-silver/ops/regulatory/card-management/transformation-layer/sispagos"
-- MAGIC spark.conf.set('deltaPath',str(deltaPath))
-- MAGIC directoryPath = "/mnt/lakehylia-silver/ops/regulatory/card-management/transformation-layer/"
-- MAGIC outputPath2 = "/dbfs/mnt/lakehylia-silver/ops/regulatory/card-management/transformation-layer/SISPAGOS_"+currentdate+".csv"
-- MAGIC outputPath2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("startQuarter",file_name)
-- MAGIC dbutils.widgets.text("pathSispagos","/mnt/lakehylia-silver/ops/regulatory/card-management/transformation-layer/sispagos")

-- COMMAND ----------

CREATE WIDGET TEXT pathSispagos DEFAULT "/mnt/lakehylia-silver/ops/regulatory/card-management/transformation-layer/sispagos";
CREATE WIDGET TEXT pathCsv DEFAULT "/dbfs/mnt/lakehylia-silver/ops/regulatory/card-management/transformation-layer/SISPAGOS.csv";

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.sispagos                
(                   
  Ano INT,
  Mes INT,
  Seccion STRING,
  Moneda STRING,
  Tipo_Cuenta INT,
  Tipo_Persona INT,
  Numero_de_Cuentas INT,
  Saldo_Promedio INT
)
USING DELTA 
LOCATION '$pathSispagos';

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC val df = spark.createDataFrame(Seq(
-- MAGIC   (2022, 12, "1.1", "MXN", 1010, 1816, 0, 0),
-- MAGIC   (2022, 12, "2.1", "MXN", 1723, 0, 0, 0),
-- MAGIC   (2022, 12, "2.1", "MXN", 1750, 0, 0, 0),
-- MAGIC   (2022, 12, "2.1.1", "MXN", 1724, 1772, 0, 0),
-- MAGIC   (2022, 12, "2.1.1", "MXN", 1724, 7012, 0, 0),
-- MAGIC   (2022, 12, "2.3.2", "MXN", 1724, 7012, 0, 0),
-- MAGIC   (2022, 12, "2.3.2", "MXN", 1724, 7015, 0, 0),
-- MAGIC   (2022, 12, "2.3.2", "MXN", 1724, 7018, 0, 0),
-- MAGIC   (2022, 12, "2.3.4", "MXN", 1839, 2752, 0, 0),
-- MAGIC   (2022, 12, "2.4.2", "MXN", 1724, 1772, 0, 0),
-- MAGIC   (2022, 12, "2.4.2", "MXN", 1724, 7019, 0, 0),
-- MAGIC   (2022, 12, "2.4.2", "MXN", 1724, 7020, 0, 0),
-- MAGIC   (2022, 12, "2.4.2", "MXN", 1724, 7025, 0, 0),
-- MAGIC   (2022, 12, "2.4.2", "MXN", 1724, 7026, 0, 0)
-- MAGIC )).toDF("Ano","Mes","Seccion","Moneda","Tipo_Cuenta","Tipo_Persona","Numero_de_Cuentas","Saldo_Promedio")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC df.write.format("delta").mode("overwrite").save(spark.conf.get("deltaPath"))

-- COMMAND ----------

SELECT * FROM bronze.dambs;

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vista1 AS
SELECT
  YEAR(Date) AS Ano,
  QUARTER(Date)*3 AS Mes,
  '2.1' AS Seccion,
  'MXN' AS Moneda,
  '1723' AS Tipo_Cuenta,
  '0' AS Tipo_Persona,
  SUM(NumberUnblockedCards) AS Numero_de_Cuentas,
  (SUM(CurrentBalance) / SUM(NumberUnblockedCards)) AS Saldo_Promedio
FROM
  bronze.dambs
WHERE
  date >= (NOW() - INTERVAL 3 MONTH)
GROUP BY
  YEAR(Date),
  QUARTER(Date)

-- COMMAND ----------

SELECT * FROM vista1

-- COMMAND ----------

MERGE INTO silver.sispagos USING vista1 ON silver.sispagos.Seccion = vista1.Seccion
AND silver.sispagos.Tipo_Cuenta = vista1.Tipo_Cuenta
AND silver.sispagos.Tipo_Persona = vista1.Tipo_Persona
WHEN MATCHED THEN
UPDATE
SET
  silver.sispagos.Ano = vista1.Ano,
  silver.sispagos.Mes = vista1.Mes,
  silver.sispagos.Numero_de_Cuentas = vista1.Numero_de_Cuentas,
  silver.sispagos.Saldo_Promedio = vista1.Saldo_Promedio

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vista2 AS
SELECT
  YEAR(Date) AS Ano,
  QUARTER(Date)*3 AS Mes,
  '1.1' AS Seccion,
  'MXN' AS Moneda,
  '1010' AS Tipo_Cuenta,
  '1816' AS Tipo_Persona,
  approx_count_distinct(AccountNumber) AS Numero_de_Cuentas,
  (SUM(CurrentBalance) / approx_count_distinct(AccountNumber)) AS Saldo_Promedio
FROM
  bronze.dambs
WHERE
  date >= (NOW() - INTERVAL 3 MONTH) AND to_timestamp(CardExpirationDate, 'yyyyMMdd') = to_date('20260731', 'yyyyMMdd')
GROUP BY
  YEAR(Date),
  QUARTER(Date)

-- COMMAND ----------

SELECT * FROM vista2

-- COMMAND ----------

MERGE INTO silver.sispagos USING vista2 ON silver.sispagos.Seccion = vista2.Seccion
AND silver.sispagos.Tipo_Cuenta = vista2.Tipo_Cuenta
AND silver.sispagos.Tipo_Persona = vista2.Tipo_Persona
WHEN MATCHED THEN
UPDATE
SET
  silver.sispagos.Ano = vista2.Ano,
  silver.sispagos.Mes = vista2.Mes,
  silver.sispagos.Numero_de_Cuentas = vista2.Numero_de_Cuentas,
  silver.sispagos.Saldo_Promedio = vista2.Saldo_Promedio

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vista3 AS
SELECT
  YEAR(Date) AS Ano,
  QUARTER(Date)*3 AS Mes,
  '2.1' AS Seccion,
  'MXN' AS Moneda,
  '1750' AS Tipo_Cuenta,
  '0' AS Tipo_Persona,
  '0' AS Numero_de_Cuentas,
  '0' AS Saldo_Promedio
FROM
  bronze.dambs
WHERE
  date >= (NOW() - INTERVAL 3 MONTH)
GROUP BY
  YEAR(Date),
  QUARTER(Date)

-- COMMAND ----------

select * from vista3

-- COMMAND ----------

MERGE INTO silver.sispagos USING vista3 ON silver.sispagos.Seccion = vista3.Seccion
AND silver.sispagos.Tipo_Cuenta = vista3.Tipo_Cuenta
AND silver.sispagos.Tipo_Persona = vista3.Tipo_Persona
WHEN MATCHED THEN
UPDATE
SET
  silver.sispagos.Ano = vista3.Ano,
  silver.sispagos.Mes = vista3.Mes,
  silver.sispagos.Numero_de_Cuentas = vista3.Numero_de_Cuentas,
  silver.sispagos.Saldo_Promedio = vista3.Saldo_Promedio

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vista4 AS
SELECT
  YEAR(Date) AS Ano,
  QUARTER(Date)*3 AS Mes,
  '2.1.1' AS Seccion,
  'MXN' AS Moneda,
  '1724' AS Tipo_Cuenta,
  '1772' AS Tipo_Persona,
  '0' AS Numero_de_Cuentas,
  '0' AS Saldo_Promedio
FROM
  bronze.dambs
WHERE
  date >= (NOW() - INTERVAL 3 MONTH)
GROUP BY
  YEAR(Date),
  QUARTER(Date)

-- COMMAND ----------

MERGE INTO silver.sispagos USING vista4 ON silver.sispagos.Seccion = vista4.Seccion
AND silver.sispagos.Tipo_Cuenta = vista4.Tipo_Cuenta
AND silver.sispagos.Tipo_Persona = vista4.Tipo_Persona
WHEN MATCHED THEN
UPDATE
SET
  silver.sispagos.Ano = vista4.Ano,
  silver.sispagos.Mes = vista4.Mes,
  silver.sispagos.Numero_de_Cuentas = vista4.Numero_de_Cuentas,
  silver.sispagos.Saldo_Promedio = vista4.Saldo_Promedio

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vista5 AS
SELECT
  YEAR(Date) AS Ano,
  QUARTER(Date)*3 AS Mes,
  '2.1.1' AS Seccion,
  'MXN' AS Moneda,
  '1724' AS Tipo_Cuenta,
  '7012' AS Tipo_Persona,
  '0' AS Numero_de_Cuentas,
  '0' AS Saldo_Promedio
FROM
  bronze.dambs
WHERE
  date >= (NOW() - INTERVAL 3 MONTH)
GROUP BY
  YEAR(Date),
  QUARTER(Date)

-- COMMAND ----------

MERGE INTO silver.sispagos USING vista5 ON silver.sispagos.Seccion = vista5.Seccion
AND silver.sispagos.Tipo_Cuenta = vista5.Tipo_Cuenta
AND silver.sispagos.Tipo_Persona = vista5.Tipo_Persona
WHEN MATCHED THEN
UPDATE
SET
  silver.sispagos.Ano = vista5.Ano,
  silver.sispagos.Mes = vista5.Mes,
  silver.sispagos.Numero_de_Cuentas = vista5.Numero_de_Cuentas,
  silver.sispagos.Saldo_Promedio = vista5.Saldo_Promedio

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vista6 AS
SELECT
  YEAR(Date) AS Ano,
  QUARTER(Date)*3 AS Mes,
  '2.3.2' AS Seccion,
  'MXN' AS Moneda,
  '1724' AS Tipo_Cuenta,
  '7012' AS Tipo_Persona,
  '0' AS Numero_de_Cuentas,
  '0' AS Saldo_Promedio
FROM
  bronze.dambs
WHERE
  date >= (NOW() - INTERVAL 3 MONTH)
GROUP BY
  YEAR(Date),
  QUARTER(Date)

-- COMMAND ----------

MERGE INTO silver.sispagos USING vista6 ON silver.sispagos.Seccion = vista6.Seccion
AND silver.sispagos.Tipo_Cuenta = vista6.Tipo_Cuenta
AND silver.sispagos.Tipo_Persona = vista6.Tipo_Persona
WHEN MATCHED THEN
UPDATE
SET
  silver.sispagos.Ano = vista6.Ano,
  silver.sispagos.Mes = vista6.Mes,
  silver.sispagos.Numero_de_Cuentas = vista6.Numero_de_Cuentas,
  silver.sispagos.Saldo_Promedio = vista6.Saldo_Promedio

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vista7 AS
SELECT
  YEAR(Date) AS Ano,
  QUARTER(Date)*3 AS Mes,
  '2.3.2' AS Seccion,
  'MXN' AS Moneda,
  '1724' AS Tipo_Cuenta,
  '7015' AS Tipo_Persona,
  '0' AS Numero_de_Cuentas,
  '0' AS Saldo_Promedio
FROM
  bronze.dambs
WHERE
  date >= (NOW() - INTERVAL 3 MONTH)
GROUP BY
  YEAR(Date),
  QUARTER(Date)

-- COMMAND ----------

MERGE INTO silver.sispagos USING vista7 ON silver.sispagos.Seccion = vista7.Seccion
AND silver.sispagos.Tipo_Cuenta = vista7.Tipo_Cuenta
AND silver.sispagos.Tipo_Persona = vista7.Tipo_Persona
WHEN MATCHED THEN
UPDATE
SET
  silver.sispagos.Ano = vista7.Ano,
  silver.sispagos.Mes = vista7.Mes,
  silver.sispagos.Numero_de_Cuentas = vista7.Numero_de_Cuentas,
  silver.sispagos.Saldo_Promedio = vista7.Saldo_Promedio

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vista8 AS
SELECT
  YEAR(Date) AS Ano,
  QUARTER(Date)*3 AS Mes,
  '2.3.2' AS Seccion,
  'MXN' AS Moneda,
  '1724' AS Tipo_Cuenta,
  '7018' AS Tipo_Persona,
  '0' AS Numero_de_Cuentas,
  '0' AS Saldo_Promedio
FROM
  bronze.dambs
WHERE
  date >= (NOW() - INTERVAL 3 MONTH)
GROUP BY
  YEAR(Date),
  QUARTER(Date)

-- COMMAND ----------

MERGE INTO silver.sispagos USING vista8 ON silver.sispagos.Seccion = vista8.Seccion
AND silver.sispagos.Tipo_Cuenta = vista8.Tipo_Cuenta
AND silver.sispagos.Tipo_Persona = vista8.Tipo_Persona
WHEN MATCHED THEN
UPDATE
SET
  silver.sispagos.Ano = vista8.Ano,
  silver.sispagos.Mes = vista8.Mes,
  silver.sispagos.Numero_de_Cuentas = vista8.Numero_de_Cuentas,
  silver.sispagos.Saldo_Promedio = vista8.Saldo_Promedio

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vista9 AS
SELECT
  YEAR(Date) AS Ano,
  QUARTER(Date)*3 AS Mes,
  '2.3.4' AS Seccion,
  'MXN' AS Moneda,
  '1839' AS Tipo_Cuenta,
  '2752' AS Tipo_Persona,
  '0' AS Numero_de_Cuentas,
  '0' AS Saldo_Promedio
FROM
  bronze.dambs
WHERE
  date >= (NOW() - INTERVAL 3 MONTH)
GROUP BY
  YEAR(Date),
  QUARTER(Date)

-- COMMAND ----------

MERGE INTO silver.sispagos USING vista9 ON silver.sispagos.Seccion = vista9.Seccion
AND silver.sispagos.Tipo_Cuenta = vista9.Tipo_Cuenta
AND silver.sispagos.Tipo_Persona = vista9.Tipo_Persona
WHEN MATCHED THEN
UPDATE
SET
  silver.sispagos.Ano = vista9.Ano,
  silver.sispagos.Mes = vista9.Mes,
  silver.sispagos.Numero_de_Cuentas = vista9.Numero_de_Cuentas,
  silver.sispagos.Saldo_Promedio = vista9.Saldo_Promedio

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vista10 AS
SELECT
  YEAR(Date) AS Ano,
  QUARTER(Date)*3 AS Mes,
  '2.4.2' AS Seccion,
  'MXN' AS Moneda,
  '1724' AS Tipo_Cuenta,
  '1772' AS Tipo_Persona,
  '0' AS Numero_de_Cuentas,
  '0' AS Saldo_Promedio
FROM
  bronze.dambs
WHERE
  date >= (NOW() - INTERVAL 3 MONTH)
GROUP BY
  YEAR(Date),
  QUARTER(Date)

-- COMMAND ----------

MERGE INTO silver.sispagos USING vista10 ON silver.sispagos.Seccion = vista10.Seccion
AND silver.sispagos.Tipo_Cuenta = vista10.Tipo_Cuenta
AND silver.sispagos.Tipo_Persona = vista10.Tipo_Persona
WHEN MATCHED THEN
UPDATE
SET
  silver.sispagos.Ano = vista10.Ano,
  silver.sispagos.Mes = vista10.Mes,
  silver.sispagos.Numero_de_Cuentas = vista10.Numero_de_Cuentas,
  silver.sispagos.Saldo_Promedio = vista10.Saldo_Promedio

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vista11 AS
SELECT
  YEAR(Date) AS Ano,
  QUARTER(Date)*3 AS Mes,
  '2.4.2' AS Seccion,
  'MXN' AS Moneda,
  '1724' AS Tipo_Cuenta,
  '7019' AS Tipo_Persona,
  '0' AS Numero_de_Cuentas,
  '0' AS Saldo_Promedio
FROM
  bronze.dambs
WHERE
  date >= (NOW() - INTERVAL 3 MONTH)
GROUP BY
  YEAR(Date),
  QUARTER(Date)

-- COMMAND ----------

MERGE INTO silver.sispagos USING vista11 ON silver.sispagos.Seccion = vista11.Seccion
AND silver.sispagos.Tipo_Cuenta = vista11.Tipo_Cuenta
AND silver.sispagos.Tipo_Persona = vista11.Tipo_Persona
WHEN MATCHED THEN
UPDATE
SET
  silver.sispagos.Ano = vista11.Ano,
  silver.sispagos.Mes = vista11.Mes,
  silver.sispagos.Numero_de_Cuentas = vista11.Numero_de_Cuentas,
  silver.sispagos.Saldo_Promedio = vista11.Saldo_Promedio

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vista12 AS
SELECT
  YEAR(Date) AS Ano,
  QUARTER(Date)*3 AS Mes,
  '2.4.2' AS Seccion,
  'MXN' AS Moneda,
  '1724' AS Tipo_Cuenta,
  '7020' AS Tipo_Persona,
  '0' AS Numero_de_Cuentas,
  '0' AS Saldo_Promedio
FROM
  bronze.dambs
WHERE
  date >= (NOW() - INTERVAL 3 MONTH)
GROUP BY
  YEAR(Date),
  QUARTER(Date)

-- COMMAND ----------

MERGE INTO silver.sispagos USING vista12 ON silver.sispagos.Seccion = vista12.Seccion
AND silver.sispagos.Tipo_Cuenta = vista12.Tipo_Cuenta
AND silver.sispagos.Tipo_Persona = vista12.Tipo_Persona
WHEN MATCHED THEN
UPDATE
SET
  silver.sispagos.Ano = vista12.Ano,
  silver.sispagos.Mes = vista12.Mes,
  silver.sispagos.Numero_de_Cuentas = vista12.Numero_de_Cuentas,
  silver.sispagos.Saldo_Promedio = vista12.Saldo_Promedio

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vista13 AS
SELECT
  YEAR(Date) AS Ano,
  QUARTER(Date)*3 AS Mes,
  '2.4.2' AS Seccion,
  'MXN' AS Moneda,
  '1724' AS Tipo_Cuenta,
  '7025' AS Tipo_Persona,
  '0' AS Numero_de_Cuentas,
  '0' AS Saldo_Promedio
FROM
  bronze.dambs
WHERE
  date >= (NOW() - INTERVAL 3 MONTH)
GROUP BY
  YEAR(Date),
  QUARTER(Date)

-- COMMAND ----------

MERGE INTO silver.sispagos USING vista13 ON silver.sispagos.Seccion = vista13.Seccion
AND silver.sispagos.Tipo_Cuenta = vista13.Tipo_Cuenta
AND silver.sispagos.Tipo_Persona = vista13.Tipo_Persona
WHEN MATCHED THEN
UPDATE
SET
  silver.sispagos.Ano = vista13.Ano,
  silver.sispagos.Mes = vista13.Mes,
  silver.sispagos.Numero_de_Cuentas = vista13.Numero_de_Cuentas,
  silver.sispagos.Saldo_Promedio = vista13.Saldo_Promedio

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vista14 AS
SELECT
  YEAR(Date) AS Ano,
  QUARTER(Date)*3 AS Mes,
  '2.4.2' AS Seccion,
  'MXN' AS Moneda,
  '1724' AS Tipo_Cuenta,
  '7026' AS Tipo_Persona,
  '0' AS Numero_de_Cuentas,
  '0' AS Saldo_Promedio
FROM
  bronze.dambs
WHERE
  date >= (NOW() - INTERVAL 3 MONTH)
GROUP BY
  YEAR(Date),
  QUARTER(Date)

-- COMMAND ----------

MERGE INTO silver.sispagos USING vista14 ON silver.sispagos.Seccion = vista14.Seccion
AND silver.sispagos.Tipo_Cuenta = vista14.Tipo_Cuenta
AND silver.sispagos.Tipo_Persona = vista14.Tipo_Persona
WHEN MATCHED THEN
UPDATE
SET
  silver.sispagos.Ano = vista14.Ano,
  silver.sispagos.Mes = vista14.Mes,
  silver.sispagos.Numero_de_Cuentas = vista14.Numero_de_Cuentas,
  silver.sispagos.Saldo_Promedio = vista14.Saldo_Promedio

-- COMMAND ----------

SELECT * FROM silver.sispagos

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pathDelta = dbutils.widgets.get("pathSispagos")
-- MAGIC df = spark.read.format("delta").load(pathDelta)
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pathCsv = outputPath
-- MAGIC pd = df.toPandas()
-- MAGIC pd.to_csv(pathCsv, header=True, index=False)
-- MAGIC dbutils.fs.ls(directoryPath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()
