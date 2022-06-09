-- Databricks notebook source
CREATE WIDGET TEXT pathSispagos DEFAULT "/mnt/lakehylia-silver/ops/regulatory/card-management/transformation-layer/sispagos";
CREATE WIDGET TEXT pathCsv DEFAULT = "/dbfs/mnt/lakehylia-silver/ops/regulatory/card-management/transformation-layetr/SISPAGOS.csv";

-- COMMAND ----------

SELECT * FROM bronze.atptx;

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vista1 AS
SELECT
  YEAR(Date) AS Ano,
  Month(Date) AS Mes,
  '2.1' AS Seccion,
  'MXN' AS Moneda,
  '1723' AS Tipo_Cuenta,
  '' AS Tipo_Persona,
  SUM(NumberUnblockedCards) AS Numero_de_Cuentas,
  (SUM(CurrentBalance) / SUM(NumberUnblockedCards)) AS Saldo_Promedio
FROM
  bronze.dambs
WHERE
  MONTH(Date) = MONTH(NOW() - INTERVAL 1 MONTH)
GROUP BY
  year(Date),
  month(Date)

-- COMMAND ----------

SELECT * FROM vista1

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vista1 AS
SELECT
  YEAR(Date) AS Ano,
  Month(Date) AS Mes,
  '2.1' AS Seccion,
  'MXN' AS Moneda,
  '1723' AS Tipo_Cuenta,
  '' AS Tipo_Persona,
  SUM(NumberUnblockedCards) AS Numero_de_Cuentas,
  (SUM(CurrentBalance) / SUM(NumberUnblockedCards)) AS Saldo_Promedio
FROM
  bronze.dambs
WHERE
  MONTH(Date) = MONTH(NOW() - INTERVAL 3 MONTH)
GROUP BY
  year(Date),
  month(Date)

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

-- MAGIC %python
-- MAGIC df = spark.read.csv('dbfs:/mnt/lakehylia-silver/ops/regulatory/card-management/transformation-layer/test_sispagos.csv', header=True, inferSchema=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("delta").mode("overwrite").save("/mnt/lakehylia-silver/ops/regulatory/card-management/transformation-layer/sispagos")

-- COMMAND ----------

MERGE INTO silver.sispagos USING vista1 ON silver.sispagos.Seccion = vista1.Seccion
AND silver.sispagos.Tipo_Cuenta = vista1.Tipo_Cuenta
WHEN MATCHED THEN
UPDATE
SET
  silver.sispagos.Numero_de_Cuentas = vista1.Numero_de_Cuentas,
  silver.sispagos.Saldo_Promedio = vista1.Saldo_Promedio

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vista2 AS
SELECT
  YEAR(Date) AS Ano,
  Month(Date) AS Mes,
  '1.1' AS Seccion,
  'MXN' AS Moneda,
  '1010' AS Tipo_Cuenta,
  '1816' AS Tipo_Persona,
  approx_count_distinct(AccountNumber) AS Numero_de_Cuentas,
  (SUM(CurrentBalance) / approx_count_distinct(AccountNumber)) AS Saldo_Promedio
FROM
  bronze.dambs
WHERE
  MONTH(Date) = MONTH(NOW() - INTERVAL 1 MONTH) AND to_timestamp(CardExpirationDate, 'yyyyMMdd') = to_date('20260731', 'yyyyMMdd')
GROUP BY
  year(Date),
  month(Date)

-- COMMAND ----------

SELECT * FROM vista2

-- COMMAND ----------

MERGE INTO silver.sispagos USING vista2 ON silver.sispagos.Seccion = vista2.Seccion
AND silver.sispagos.Tipo_Cuenta = vista2.Tipo_Cuenta
WHEN MATCHED THEN
UPDATE
SET
  silver.sispagos.Numero_de_Cuentas = vista2.Numero_de_Cuentas,
  silver.sispagos.Saldo_Promedio = vista2.Saldo_Promedio

-- COMMAND ----------

SELECT * FROM silver.sispagos

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pathDelta = dbutils.widgets.get("pathSispagos")
-- MAGIC df = spark.read.format("delta").load(pathDelta)
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pathCsv = dbutils.widgets.get("pathCsv")
-- MAGIC pd = df.toPandas()
-- MAGIC pd.to_csv(pathCsv, header=True, index=False)
