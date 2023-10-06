# pylint: disable=missing-module-docstring
from collections import OrderedDict
from operator import methodcaller as ϱ

from pyspark.sql import functions as F
from toolz import pipe
from epic_py.tools import partial2

fpsl_specs = {
    'name': 'subledger',
    'alias': 'fpsl',
    'f-regex': r'CONCILIA(?P<key>\w+)(?P<date>\d{8})\.txt',
    'options': dict(sep= '|', header= True, nullValue= 'null',
        dateFormat= 'd.M.y', timestampFormat= 'd.M.y H:m:s',
        mode='PERMISIVE'),
    'schema' : OrderedDict({
        'C55POSTD' : 'date', 'C55YEAR'    : 'int',
        'C35TRXTYP': 'str' , 'C55CONTID'  : 'str',
        'C11PRDCTR': 'str' , 'K5SAMLOC'   : 'str',  
        'LOC_CURR' : 'str' , 'IGL_ACCOUNT': 'long'}),
    'mutate' : {
        'K5SAMLOC'    : pipe(F.col('K5SAMLOC'),
            partial2(F.regexp_replace, ...,  r"(\-?)([0-9\.])(\-?)", "$3$1$2"),
            ϱ('cast', 'double')),
        'txn_valid'   : F.col('C11PRDCTR').isNotNull()
                      & F.col('C11PRDCTR').startswith('EPC')
                      &~F.col('C35TRXTYP').startswith('S'),
<<<<<<< HEAD
        'num_cuenta'  : F.substring(F.col('C55CONTID'), 1, 11),
        'clave_txn'   : F.col('C35TRXTYP').cast('int'),
        'moneda'      : F.col('LOC_CURR'),
        'monto_txn'   : F.col('K5SAMLOC'),     # Suma y compara.
        'cuenta_fpsl' : F.col('IGL_ACCOUNT'),  # Referencia extra, asociada a NUM_CUENTA. 
        'tipo_prod'   : F.col('C11PRDCTR')}, 
    'match': {
        'where': [F.col('txn_valid')],
        'by'   : ['num_cuenta', 'clave_txn', 'moneda', 'tipo_prod'],
=======
        'num_cuenta'  : F.substring(F.col('C55CONTID'), 1, 11),  # JOIN
        'clave_txn'   : F.col('C35TRXTYP').cast('int'),
        'moneda'      : F.col('LOC_CURR'),
        'monto_txn'   : F.col('K5SAMLOC'),     # Suma y compara.
        'cuenta_fpsl' : F.col('IGL_ACCOUNT')}, # Referencia extra, asociada a NUM_CUENTA.
    'match': {
        'where': [F.col('txn_valid')],
        'by'   : ['num_cuenta', 'clave_txn', 'moneda'],
>>>>>>> 0d63371b8477e34121abc6fcf4f4329238a0ba36
        'agg'  : {
            'fpsl_num_txns': F.count('*'),
            'fpsl_monto'   : F.round(F.sum('monto_txn'), 2)}}
}
