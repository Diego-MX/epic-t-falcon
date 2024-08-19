# Databricks notebook source
# MAGIC %md
# MAGIC # Introducción  
# MAGIC Al correr los notebooks de SisPagos R-2422, vimos que una de las columnas está mal calculada.  
# MAGIC En este _notebook_ recalculamos la columna y la rescribimos.  
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable as Δ
from pyspark.sql import functions as F, Column

from epic_py.delta import EpicDF

from config import (t_agent, t_resources, 
    DATALAKE_PATHS as paths)

delta_locs = t_resources.get_resource_url('abfss', 'storage', 
    container='silver', blob_path=paths['datasets'])


# COMMAND ----------

fiserv_tables = ['atpt', 'damna', 'dambs', 'dambs2', 'dambsc']
debug = False

if debug: 
    a_tbl = Δ.forPath(spark, f"{delta_locs}/dambsc/delta")

    a_col = F.to_date(F.regexp_extract(F.col('file_name'), r'20[\d\-]{8}', 0), 'y-M-d')
    its_str = "to_date(regexp_extract(file_name, r'20[\\d\\-]{8}', 0), 'y-M-d')"

    a_tbl.update(
        condition = "file_date != to_date(regexp_extract(file_name, r'20[\\d\\-]{8}', 0), 'y-M-d')", 
        set = {'file_date': "to_date(regexp_extract(file_name, r'20[\\d\\-]{8}', 0), 'y-M-d')"}
    )
