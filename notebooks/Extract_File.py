# Databricks notebook source
# MAGIC %md
# MAGIC # Introducción  
# MAGIC Identificamos tres capas de transformación:  `ATPTX`, `DAMBS`, `DAMNA`.  
# MAGIC Estos flujos capturan los archivos que son depositados (por Fiserv), y -naturalmente- los transforman en formato Δ para uso épico.  
# MAGIC Nos apoyamos del flujo en el _data factory_ que realiza lo siguiente:  
# MAGIC - Captura los archivos entrantes a la carpeta. 
# MAGIC - Identifica el tipo de capa al que corresponde el archivo. 
# MAGIC - Manda como variables el tipo de capa `FileName` y archivo correspondiente `ZipFileName`. 
# MAGIC - Ejecuta las celdas de este _notebook_ como flujo del código, es decir transformarlo en tabla Δ. 

# COMMAND ----------

# MAGIC %md 
# MAGIC # Revisión
# MAGIC Se revisó el código, y se hacen las siguientes observaciones.  
# MAGIC * Las *modificaciones* son cuestiones menores que se hicieron para facilitar el seguimiento del código.    
# MAGIC   Por lo general es cuestión de sintaxis, o recomendaciones generales de Python.  
# MAGIC * La refactorización se indica aparte, pues implica un cambio sustancial en el código,  
# MAGIC   solicito la junta para implementar los cambios en conjunto.  
# MAGIC 
# MAGIC ### Modificaciones
# MAGIC - En bloque `if ... else ...`, no se debe asumir que `else` implica `FileName` es el restante.  
# MAGIC   Se cambia por otro `elif` y `else` marca error.   
# MAGIC - Espacios `⎵` antes, después de `=`, y después de `,` en los argumentos.  
# MAGIC - Alinear los `=`, `:` cuando aplique. 
# MAGIC - Alinear las operaciones consecutivas de los _dataframes_: `select`,  `withColumn`, `drop`, etc.  
# MAGIC - Nunca usar "`from paquete import *`" sino darles variables a los módulos: (`sql.functions ~ F`, `sql.types ~ T`).  
# MAGIC - Uso de los allegados [f-strings de Python](https://realpython.com/python-f-strings/):  
# MAGIC   Por ejemplo:  `f"Tengo {edad} años"` en lugar de `"Tengo " + edad + " años"`
# MAGIC - ¿cuáles serían los argumentos para definir una función de procesamiento de alto nivel?   
# MAGIC   - `layer_zip_to_delta(archivo_in, archivo_out, tipo_archivo)` ...  
# MAGIC   - `layer_zip_to_delta(archivo_in)` e inferir `tipo_archivo`, `archivo_out` mediante reglas internas ...  
# MAGIC   
# MAGIC ### Refactorización
# MAGIC En `config.py` se pueden agrupar todos los `{DAMNA,DAMBS,ATPTX}_SETUP` como: 
# MAGIC ```
# MAGIC LAYER_SETUP = {
# MAGIC     'ATPTX' : (lo correspondiente a ATPTX_SETUP),
# MAGIC     'DAMBS' : (lo correspondiente a DAMBS_SETUP), 
# MAGIC     'DAMNA' : (lo correpsondiente a DAMNA_SETUP)
# MAGIC }
# MAGIC 
# MAGIC LAYER_COLS = {
# MAGIC     'ATPTX' : (lo correspondiente a ATPTX_width_column_defs), 
# MAGIC     'DAMBS' : (lo correspondiente a DAMBS_width_column_defs), 
# MAGIC     'DAMNA' : (lo correspondiente a DAMNA_width_column_defs), 
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC Y en la función utilizar la variable `FileName` como llave del diccionario para leer los argumentos correspondientes.  
# MAGIC La mayoría del código común queda afuera del `if ... else ... end`: 
# MAGIC ```
# MAGIC zip    = LAYER_SETUP[FileName]['paths']['zip']
# MAGIC origen = ... 
# MAGIC delta  = ...
# MAGIC procesados = ...
# MAGIC dateFormat = ...
# MAGIC 
# MAGIC # código común 
# MAGIC 
# MAGIC if   FileName == 'ATPTX': 
# MAGIC     # código diferenciado 
# MAGIC     df = (transformaciones de ATPTX)
# MAGIC elif FileName == 'DAMNA': 
# MAGIC     df = (transformaciones de DAMNA)
# MAGIC elif FileName == 'DAMBS': 
# MAGIC     df = (transformaciones de DAMBS)
# MAGIC else: 
# MAGIC     print("Error de variable FILENAME.")
# MAGIC 
# MAGIC # código común de escritura. 
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC # Ejecución
# MAGIC La siguiente sección contiene el código desarrollado por el equipo Readymind para extraer el contenido de los archivos y anexarlo (_append_) a las tablas en cuestión. 

# COMMAND ----------

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------

from pyspark.sql import functions as F, types as T
from config import LAYER_SETUP as setup
from config import LAYER_COLS as cols
spark.sql(""" SET TIME ZONE '-06:00' """)

# COMMAND ----------

dbutils.widgets.text('FileName', '')
dbutils.widgets.text('ZipFileName', '')
FileName    = dbutils.widgets.get('FileName')
ZipFileName = dbutils.widgets.get('ZipFileName')
zip = setup[FileName]['paths']['zip']
origen = setup[FileName]['paths']['origen']
delta  = setup[FileName]['paths']['delta']
procesados = setup[FileName]['paths']['procesados']
dateFormat = setup['DateFormat']
ts = spark.sql(""" select current_timestamp() - INTERVAL 6 HOUR  as ctime """).collect()[0]["ctime"]
filexdia = f"{procesados}{FileName}_{ts.strftime(dateFormat)}.txt"

# COMMAND ----------

df = spark.read.text(origen)
df = (df.coalesce(1)
        .withColumn("index", F.monotonically_increasing_id()))
footer_index = df.count() - 1
df = df.filter((df.index > 0) & (df.index < footer_index))
df = df.select("value", *[F.substring("value", *v).alias(k) for k, v in list(cols[FileName].items())])

# COMMAND ----------

if FileName == 'ATPTX':
  df = df.withColumn('TransactionChannel', F.col('TransactionChannel').cast(T.IntegerType()))
elif FileName == 'DAMBS':
  df = (df
      .withColumn('NumberUnblockedCards', F.col('NumberUnblockedCards').cast(T.IntegerType()))
      .withColumn('CurrentBalance', F.col('CurrentBalance').cast(T.FloatType())))
elif FileName == 'DAMNA':
    print("No hay tramsformaciones para DAMNA.")
else: 
    print("FileName debe ser uno de: ATPTX, DAMNA, DAMBS.")
df = (df
    .withColumn('date', F.current_date())
    .drop('value')
    .write.format("delta").mode("append").save(delta))
dbutils.fs.mv(origen, filexdia)
dbutils.fs.rm(zip + ZipFileName)       
