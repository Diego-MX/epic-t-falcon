import os

ENV    = os.environ.get('ENV_TYPE')
SERVER = os.environ.get('SERVER_TYPE')

RESOURCE_SETUP = {
    'qas' : {
        'scope'   : 'eh-core-banking', 
        'storage' : 'stlakehyliaqas', 
        'sp_keys' : {
            'tenant_id'        : 'aad-tenant-id', 
            'subscription_id'  : 'sp-core-events-suscription', 
            'client_id'        : 'sp-core-events-client', 
            'client_secret'    : 'sp-core-events-secret'
        } 
    }
}

DATALAKE_PATHS = {
    'abfss'  : "abfss://{stage}@{storage}.dfs.core.windows.net", 
    'blob'   : "https://{storage}.blob.core.windows.net/", 
    'spei'   : "ops/transactions/spei", 
    'raw'    : "ops/regulatory/card-management/transformation-layer", 
    'bronze' : "ops/regulatory/card-management/transformation-layer/unzippded-ready", 
    'silver' : "ops/card-management/datasets",
    # Es igual que RAW. 
    'transformation-layer' : "ops/regulatory/card-management/transformation-layer"
}

UAT_SPECS = {
    'DAMNA' : {
        'name' : 'clients', 
        'cols' : {
            'CustomerNumber'    : (   4,19),    # amna_acct
            'NameTypeIndicator' : ( 635, 1),    # amna_name_type_ind_1[1]
            'Name'              : ( 638,40),    # amna_name_line_1_1
            'GenderCode'        : ( 958, 1),    # amna_gender_code_1
            'City'              : ( 918,30),    # amna_city_1
            'Municipality'      : (1065,30) }}, # amna_count_1
    'DAMBS' : {
        'name' : 'accounts', 
        'cols' : {
            'AccountNumber'        : (   4, 19), # ambs_acct
            'CustomerNumber'       : (  46, 19), # ambs_cust_nbr
            'CardExpirationDate'   : ( 522,  8), # ambs_date_card_expr
            'NumberUnblockedCards' : ( 603,  5), # ambs_nbr_unblked_cards
            'CurrentBalanceSign'   : (2545,  1), #
            'CurrentBalance'       : (2546, 17)},# ambs_curr_bal
        'types': {
            'CustomerNumber'       : 'long',       # 
            'CardExpirationDate'   : 'date', 
            'CurrentBalanceSign'   : 'string', 
            'CurrentBalance'       : 'double', 
            'NumberUnblockedCards' : 'integer' }}, 
    'ATPTX' : {
        'name' : 'transactions', 
        'cols' : {
            'AccountNumber'         : (  4, 19),  # atpt_acct
            'EffectiveDate'         : ( 32,  8),  # atpt_mt_eff_date
            'TransactionType'       : ( 40,  1),  # atpt_mt_type
            'TransactionSign'       : ( 41,  1),  # 
            'TransactionCode'       : ( 42,  5),  # atpt_mt_txn_code
            'TransactionAmountSign' : ( 47,  1),  #
            'TransactionAmount'     : ( 48, 17),  # atpt_mt_amount
            'AcceptorCategoryCode'  : (157,  5),  # atpt_mt_category_code
            'TransactionChannel'    : (766,  2)}, # atpt_mt_channel
        'types': {
            'TransactionType'       : 'string'
        }} }



