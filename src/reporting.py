from collections import OrderedDict
from pyspark.sql import functions as F

spei_gfb = {  # RECOIF
    'name': 'spei-subledger', 
    'alias': 'gfb', 
    'options': dict(mode='PERMISIVE', sep=';', header=False), 
    'schema' : OrderedDict({
        'extra': 'str', 'cep_issuer': 'int_2', 'account_id': 'long', 'account_digital': 'long', 
        'clabe': 'long', 'receiver_channel': 'str', 'receiver_service': 'str', 
        'txn_amount': 'dbl', 'txn_status': 'str', 'rejection_reason': 'str', 
        'sender_bank': 'str', 'date_added': 'str', 'receiver_name': 'str', 
        'receiver_account': 'long', 'receiver_clabe': 'long', 
        'receiver_rfc': 'str', 'concept': 'str', 'reference': 'str', 
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
    
spei_c4b = {
    'name': 'spei-banking', 
    'alias': 'c4b', 
    'options': dict(mode='PERMISIVE', sep='|', header=False, 
            dateFormat='d-M-y', timestampFormat='d/M/y H:m:s'), 
    'schema': OrderedDict({
        'account_c4b': 'str', 'txn_type_code': 'str', 
        'txn_type': 'str', 'txn_postdate': 'ts', 
        'amount_local': 'dbl', # 'AMOUNT_CENTS': 'str',  
        # Vienen en formato europeo así que lo separamos y ajustamos. 
        'currency': 'str', 'txn_id': 'str', 
        'type_code': 'int', 'type_name': 'str', 
        'pymt_type': 'str', 'pymt_type_code': 'str', 'comm_channel_code': 'str', 
        'comm_channel': 'str', 'acct_projection': 'str', 'product_id': 'str', 
        'holder_id': 'long', 'debit_indicator': 'str', 
        'value_date': 'ts', 'creation_user': 'long', 'release_user': 'long', 
        'counter_holder': 'str', 'counter_bank_id': 'int', 'counter_account_id': 'long', 
        'counter_account': 'int', 'counter_bank_country': 'str', 'pymt_order_id': 'str', 
        'pymt_item_id': 'str', 'ref_adjust_txn_id': 'str', 
        'cancel_document': 'str', 'pymt_notes': 'str', 'end_to_end': 'str', 
        'ref_number': 'str', 'txn_status_code': 'int', 
        'txn_status': 'str', 'acct_holder_name': 'str', 
        'acct_holder_tax_id': 'str', 'cntr_party_tax_id': 'str', 
        'fee_amount': 'dbl', 'fee_currency': 'str', 
        'vat_amount': 'dbl', 'vat_currency': 'str'}), 
    'mutate': {
        'txn_postdate' : F.col('txn_postdate').cast('date'), 
        'value_date'   : F.col('value_date').cast('date'),
        'txn_valid'    : F.col('txn_status') != 'Posting Canceled',  # Filtrar
        'ref_num'      : F.col('end_to_end'),
        'account_num'  : F.substring(F.col('account_c4b'), 1, 11).cast('long'), 
        'txn_type_code': F.col('txn_type_code'),  
        'txn_amount'   : F.col('amount_local'), # Comparar. 
        'txn_date'     : F.col('txn_postdate'), 
        'txn_status'   : F.when(F.col('txn_status') == 'Posted', 'P')
                        .otherwise(F.col('txn_status'))}, 
    'match': {
        'where': [F.col('txn_valid')], 
        'by'   : ['ref_num', 'account_num', 'txn_type_code'], 
        'agg'  : {
            'c4b_num_txns': F.count('*'), 
            'c4b_monto'   : F.round(F.sum('txn_amount'), 2)}}
}

spei_2 = { # Este es Viejo, no estoy -DX- seguro de dónde.  
    'schema': {  
        'txn_code': 'str', 'txn_date': 'date', 'txn_time': 'str', 
        'txn_amount': 'dbl', 'ref_num': 'str', 'trck_code': 'str', 
        'sender_account': 'long', 'clabe_account': 'long', 'sender_name': 'str', 
        'sender_id': 'long', 'txn_description': 'str', 
        'receiver_bank_id': 'int', 'receiver_account': 'long', 
        'receiver_name': 'str', 'txn_geolocation': 'str', 'txn_status': 'int', 
        'resp_code': 'int'}, 
    'options': dict(
        mode='PERMISIVE', sep='|', header=False, dateFormat='d-M-y')
}