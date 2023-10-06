# pylint: disable=missing-module-docstring
from collections import OrderedDict
from pyspark.sql import functions as F


gfb_spei_specs = {  # RECOIF
    'name': 'spei-subledger', 
    'alias': 'gfb', 
    'f-regex': r'RECOIF(?P<key>\d+)(?P<date>\d{8})H(?P<time>\d{6})\.TXT', 
    'options': dict(mode='PERMISIVE', sep=';', header=False), 
    'schema' : OrderedDict({
        'extra': 'str', 'cep_issuer': 'int_2', 'account_id': 'long', 'account_digital': 'long', 
        'clabe': 'long', 'receiver_channel': 'str', 'receiver_service': 'str', 
        'txn_amount': 'dbl', 'txn_status': 'str', 'rejection_reason': 'str', 
        'sender_bank': 'str', 'date_added': 'str', 'receiver_name': 'str', 
        'receiver_account': 'long', 
        'receiver_clabe': 'long', 'receiver_rfc': 'str', 'concept': 'str', 'reference': 'str', 
        'tracking_key': 'str', 'uuid': 'str', 'status': 'str', 'posting_date': 'date'}), 
    'mutate': {
        'txn_valid'     : F.lit(True),             
        'ref_num'       : F.col('tracking_key'),   
        'account_num'   : F.col('account_digital'), 
        'txn_type_code' : F.when(F.col('receiver_service') == 'EMISION SPEI' , '500402')
                           .when(F.col('receiver_service') == 'SPEI RECIBIDO', '550403')
                           .otherwise(F.col('receiver_service')), 
        'txn_amount'    : F.col('txn_amount'), 
        'txn_status'    : F.col('status'),  
        'txn_date_gfb'  : F.to_date('posting_date', 'yyyy-MM-dd')}, 
    'match': {
        'where': [F.col('txn_valid')], 
        'by'   : ['ref_num', 'account_num', 'txn_type_code'], 
        'agg'  : {
            'gfb_status'  : F.first('txn_status'), 
            'gfb_num_txns': F.count('*'), 
            'gfb_monto'   : F.round(F.sum('txn_amount'), 2)}}}
    