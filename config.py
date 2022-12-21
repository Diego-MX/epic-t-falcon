# En este archivo se indican las variables y parámetros que se utilizan en los notebooks correspondientes. 

# Revisión. 
# - Las direcciones en `/mnt/` se deben sustituir por la correspondiente sin el _mount_. 
#   (acceso con Service Principal desde configuración de Clúster, o del proyecto). 
# - Las direcciones de tablas procesadas deben incluir también el nombre alias de la tabla (no sólo la ubicación).
# - Las direcciones en `dbfs:/` se deben migrar al _datalake_ cuando sea pertinente.    
# - Las variables repetidas como `dateformat` o dirección común `abfss://bronze@lakehylia.blob.core.windows.net/ops/` 
#   se debe aprovechar de variables definidas. 
# 
# Como se indica en la Revisión en el _notebook_, utilizar llaves particulares (`ATPTX`, `DAMBS`, `DAMNA`) 
# para centralizar las variables similares.  


DATALAKE_SETUP = {
    'base_url' : 'abfss://bronze@lakehylia.blob.core.windows.net/ops', 
    'paths': {
        'spei' : 'transactions/spei', 
        'transformation-layer' : 'regulatory/card-management/transformation-layer'
    }
}

LAYER_SETUP = {
  'DateFormat' : '%Y%m%d',
  'DAMNA' : {
    'paths': { 
      'zip'    : 'dbfs:/FileStore/',
      'origen' : 'dbfs:/FileStore/DAMNA.txt',
      'delta'  : 'dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/damna',
      'procesados' : 'dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/FilesUpload/DAMNA/DAMNA_Processed/'
             }
            },
  'ATPTX' : {
    'paths': { 
      'zip'    : 'dbfs:/FileStore/',
      'origen' : 'dbfs:/FileStore/ATPTX.txt',
      'delta'  : 'dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/atptx',
      'alias'  : 'por rellenar Data Diego',
      'procesados' : 'dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/FilesUpload/ATPTX/ATPTX_Processed/'
              }},
  'DAMBS' : {
    'paths': { 
      'zip'    : 'dbfs:/FileStore/',
      'origen' : 'dbfs:/FileStore/DAMBS.txt',
      'delta'  : 'dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/dambs',
      'alias'  : 'por rellenar Data Diego',
      'procesados' : 'dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/FilesUpload/DAMBS/DAMBS_Processed/'
              }},
}

LAYER_COLS = {
    'ATPTX' : {
      'AccountNumber'   : (4, 19),
      'EffectiveDate'   : (32,8),
      'TransactionType' : (40,1),
      'TransactionSign' : (41,1),
      'TransactionCode' : (42,5),
      'TransactionAmountSign' : (47,1),
      'TransactionAmount'     : (48,17),
      'AcceptorCategoryCode'  : (157,5),
      'TransactionChannel'    : (766,2)
              },
    'DAMBS' : {
      'AccountNumber' : (4,19),
      'CustomerNumber' : (46,19),
      'CardExpirationDate' : (522,8),
      'NumberUnblockedCards' : (603,5),
      'CurrentBalanceSign' : (2545,1),
      'CurrentBalance' : (2546,17)
              },
    'DAMNA' : {
      'CustomerNumber' : (4,19),
      'Name' : (638,40),
      'Municipality' : (1065,30),
      'GenderCode' : (958,1),
      'City' : (918,30),
      'NameTypeIndicator' : (635,1)
              }
}





