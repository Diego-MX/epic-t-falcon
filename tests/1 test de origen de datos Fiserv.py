# Databricks notebook source
# MAGIC %md 
# MAGIC # Acerca del notebook
# MAGIC 
# MAGIC Se realizan las pruebas indicadas en [Zephyr > Jira](https://bineo.atlassian.net/projects/PIR?selectedItem=com.atlassian.plugins.atlassian-connect-plugin:com.kanoah.test-manager__main-project-page#!/testCase/PIR-T2347).  
# MAGIC Y se adjuntan las evidencias correspondientes.
# MAGIC 
# MAGIC Las pruebas se realizan para cada una de las extracciones en dos partes; desde el origen y desde el destino.  
# MAGIC En el origen se revisa:  
# MAGIC - Cuándo fue el último envío de los datos de extracción. 
# MAGIC - Si el esquema enviado corresponde con el esquema establecido. 
# MAGIC - Otros estadísticos que nos den certeza de los datos recibidos;  
# MAGIC   no sólo se utilizan para la certeza, sino también con la intención de familiarizarnos con los mismos. 
# MAGIC   
# MAGIC En el destino de las extracciones se verifica lo siguiente:  
# MAGIC - Frecuencia y responsividad de la actualización de las extracciones.  
# MAGIC - Concordancia con los estadísticos extraidos en la fuente.  
# MAGIC - Se verificará los usos subsecuentes de los datos procesados, de acuerdo a su pertinencia.  

# COMMAND ----------

