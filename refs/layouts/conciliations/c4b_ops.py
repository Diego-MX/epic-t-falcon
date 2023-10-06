# pylint: disable=missing-module-docstring
from collections import OrderedDict

from pyspark.sql import functions as F

c4b_specs = {
    'name': 'cloud-banking',
    'alias': 'c4b',
    'f-regex': r'CONCILIA(?P<key>\w+)(?P<date>\d{8})\.txt',
    'options': dict(mode='PERMISIVE', 
        sep='|', header=True, nullValue='null',
        dateFormat='d.M.y', timestampFormat='d.M.y H:m:s'),
    'schema' : OrderedDict({
        'ACCOUNTID': 'str', 'TRANSACTIONTYPENAME': 'str', 'ACCOUNTHOLDERID': 'long',
        'POSTINGDATE':'date', 'AMOUNT': 'dbl', 'CURRENCY': 'str', 'VALUEDATE': 'date',
        'STATUSNAME': 'str', 'COUNTERPARTYACCOUNTHOLDER': 'str', 'COUNTERPARTYBANKACCOUNT': 'str',
        'CREATIONUSER': 'str', 'TRANSACTIONID': 'str', 'TYPECODE': 'int', 'TYPENAME': 'str',
        'PAYMENTTYPECODE': 'int', 'PAYMENTTYPENAME': 'str', 'TRANSACTIONTYPECODE':'int',
        'COMMUNICATIONCHANNELCODE': 'int', 'COMMUNICATIONCHANNELNAME': 'str',
        'ACCOUNTPROJECTION': 'str', 'ACCOUNTPRODUCTID': 'str', 'DEBITINDICATOR': 'str',
        'EXCHANGERATETYPECODE': 'str', 'EXCHANGERATETYPENAME': 'str', 'EXCHANGERATE': 'str',
        'AMOUNTAC': 'dbl', 'CURRENCYAC': 'str', 'PRENOTEID': 'str', 'STATUSCODE': 'int',
        'COUNTERPARTYBANKCOUNTRY': 'str', 'COUNTERPARTYBANKID': 'str',
        'COUNTERPARTYBANKACCOUNTID': 'str', 'PAYMENTTRANSACTIONORDERID': 'str',
        'PAYMENTTRANSACTIONORDERITEMID': 'str', 'REFADJUSTMENTTRANSACTIONID': 'str',
        'CANCELLATIONDOCUMENTINDICATOR': 'str', 'CANCELLEDENTRYREFERENCE': 'str',
        'CANCELLATIONENTRYREFERENCE': 'str', 'PAYMENTNOTES': 'str', 'CREATIONDATETIME': 'ts',
        'CHANGEDATETIME': 'ts', 'RELEASEDATETIME': 'ts', 'CHANGEUSER': 'str', 'RELEASEUSER': 'str',
        'COUNTER': 'int'}),
    'mutate': {
        'txn_valid'   : F.lit(True),  # Filtrar
        'num_cuenta'  : F.split(F.col('ACCOUNTID'), '-')[0], # JOIN
        'clave_txn'   : F.col('TRANSACTIONTYPECODE'),
        'moneda'      : F.col('CURRENCY'),
        'monto_txn'   : F.col('AMOUNT'),  # Suma y compara
        'tipo_txn'    : F.col('TYPENAME')},  # Referencia, asociada a CLAVE_TXN.
    'match': {
        'where': [F.col('txn_valid')],
        'by'   : ['num_cuenta', 'clave_txn', 'moneda'],
        'agg'  : {
            'c4b_num_txns': F.count('*'),
            'c4b_monto'   : F.round(F.sum('monto_txn'), 2)}}
}
