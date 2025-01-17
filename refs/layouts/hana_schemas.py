from pyspark.sql import functions as F, types as T
from hdbcli import dbapi


# Esto se debe de leer de una tabla. Ejemplo:
# from spark_utils import table_2_struct
# 
# schema = table_2_struct(pd.from_csv(the_file))  
schema = T.StructType([
    T.StructField("NUMCLIENTE", T.StringType(), False), 
    T.StructField("NUMCREDITO", T.StringType(), False), 
    T.StructField("N_IMPORTECREDITO", T.StringType(), True), 
    T.StructField("PLAZO", T.StringType(), True), 
    T.StructField("PLAZOREMANENTE", T.StringType(), True), 
    T.StructField("TIPOCREDITO", T.StringType(), True), 
    T.StructField("N_STATUS", T.StringType(), True), 
    T.StructField("FRECUENCIA", T.StringType(), True), 
    T.StructField("PV", T.StringType(), True), 
    T.StructField("PV_FREQ", T.StringType(), True), 
    T.StructField("N_CAPVIG", T.StringType(), True), 
    T.StructField("N_CAPVEN", T.StringType(), True), 
    T.StructField("N_INTVIG", T.StringType(), True), 
    T.StructField("N_INTVEN", T.StringType(), True), 
    T.StructField("N_SALDOCONTABLE", T.StringType(), True), 
    T.StructField("N_CARTERAVENCIDO", T.StringType(), True), 
    T.StructField("N_TASACOBRADA", T.StringType(), True), 
    T.StructField("D_CORTE", T.DateType(), True), 
    T.StructField("D_ALTA", T.DateType(), True), 
    T.StructField("D_VENCIMIENTO", T.DateType(), True), 
    T.StructField("D_STATUS", T.StringType(), True), 
    T.StructField("MONTOEXIGIBLE_VIG", T.DecimalType(14, 2), True), 
    T.StructField("MONTOEXIGIBLECAP_VIG", T.DecimalType(14, 2), True), 
    T.StructField("MONTOEXIGIBLEINT_VIG", T.DecimalType(14, 2), True), 
    T.StructField("MONTOEXIGIBLECOM_VIG", T.DecimalType(14, 2), True), 
    T.StructField("MONTOEXIGIBLEFALTAPAG_VIG", T.DecimalType(14, 2), True), 
    T.StructField("MONTOEXIGIBLEIVAOTROS_VIG", T.DecimalType(14, 2), True), 
    T.StructField("MONTOEXIGIBLE_VEN", T.DecimalType(14, 2), True), 
    T.StructField("MONTOEXIGIBLECAP_VEN", T.DecimalType(14, 2), True), 
    T.StructField("MONTOEXIGIBLEINT_VEN", T.DecimalType(14, 2), True), 
    T.StructField("MONTOEXIGIBLECOM_VEN", T.DecimalType(14, 2), True), 
    T.StructField("MONTOEXIGIBLEFALTAPAG_VEN", T.DecimalType(14, 2), True), 
    T.StructField("MONTOEXIGIBLEIVAOTROS_VEN", T.DecimalType(14, 2), True), 
    T.StructField("PAGOREALIZADO", T.DecimalType(14, 2), True), 
    T.StructField("PAGOREALIZADOCAP", T.DecimalType(14, 2), True), 
    T.StructField("PAGOREALIZADOINT", T.DecimalType(14, 2), True), 
    T.StructField("PAGOREALIZADOCOM", T.DecimalType(14, 2), True), 
    T.StructField("PAGOREALIZADOFALTAPAG", T.DecimalType(14, 2), True), 
    T.StructField("PAGOREALIZADOIVAOTR", T.DecimalType(14, 2), True), 
    T.StructField("CODIGOCREDITOREESTRUCTURADO", T.StringType(), True), 
    T.StructField("DIASATRASO", T.StringType(), True), 
    T.StructField("ATR_SAP", T.StringType(), True), 
    T.StructField("MAXATR_SAP", T.StringType(), True), 
    T.StructField("NUMDISPOSICIONES", T.StringType(), True), 
    T.StructField("EDO_CTA_PAPEL", T.StringType(), True), 
    T.StructField("FECHADISPREEST", T.DateType(), True), 
    T.StructField("ANTIGUEDADACREDITADO", T.StringType(), True), 
    T.StructField("FECHAFINREEST", T.DateType(), True), 
    T.StructField("SALDO_REP_SIC", T.StringType(), True), 
    T.StructField("CAT", T.StringType(), True), 
    T.StructField("PRODUCTO", T.StringType(), True), 
    T.StructField("TASAINTERES", T.StringType(), True), 
    T.StructField("TIPOREESTRUCTURA", T.StringType(), True), 
    T.StructField("INDICADOR_SIC", T.StringType(), True)])


double_types = ["N_IMPORTECREDITO", 
    "PLAZO", 
    "PLAZOREMANENTE", 
    "N_CAPVIG", 
    "N_CAPVEN", 
    "N_INTVIG", 
    "N_INTVEN", 
    "N_SALDOCONTABLE", 
    "N_CARTERAVENCIDO", 
    "N_TASACOBRADA", 
    "SALDO_REP_SIC", 
    "TASAINTERES", 
    "ATR_SAP", 
    "MAXATR_SAP",                
    "CAT"]

long_types = ["TIPOCREDITO", 
    "N_STATUS", 
    "FRECUENCIA", 
    "PV", 
    "PV_FREQ", 
    "D_STATUS", 
    "DIASATRASO", 
    "NUMDISPOSICIONES", 
    "EDO_CTA_PAPEL", 
    "ANTIGUEDADACREDITADO", 
    "TIPOREESTRUCTURA", 
    "INDICADOR_SIC"]


conn = dbapi.connect(
    address="8974d28e-a050-49b9-9361-2c72ee99accc.hana.prod-us21.hanacloud.ondemand.com",
    port=443,
    user="DATALAKE",
    password="Epic_123",
    encrypt=True)