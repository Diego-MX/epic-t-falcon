from os import environ, getenv
import re



from azure.identity import ClientSecretCredential
from azure.identity._credentials.default import DefaultAzureCredential
try: 
    from pyspark.dbutils import DBUtils
except ImportError: 
    DBUtils = None
try:
    from dotenv import load_dotenv
except ImportError: 
    load_dotenv = None
    

ENV    = environ.get('ENV_TYPE')
SERVER = environ.get('SERVER_TYPE')

RESOURCE_SETUP = {
    # No hay DEV porque no quisieron poner los archivos en DEV. 
    # Entonces usamos QAS como si fuera DEV. 🤷
    'qas' : {
        'scope'   : 'eh-core-banking', 
        'storage' : 'stlakehyliaqas',  # Is GEN2 ADLS. 
        'sp_keys' : {
            'tenant_id'        : (1, 'aad-tenant-id'), 
            'subscription_id'  : (1, 'sp-core-events-subscription'), 
            'client_id'        : (1, 'sp-core-events-client'), 
            'client_secret'    : (1, 'sp-core-events-secret')
        } 
    }, 
    'stg' : {
        'scope'   : 'ops-conciliations', 
        'storage' : 'stlakehyliastg', 
        'sp_keys' : {
            'tenant_id'        : (1, 'aad-tenant-id'), 
            'subscription_id'  : (1, 'sp-ops-conciliations-subscription'), 
            'client_id'        : (1, 'sp-ops-conciliations-client'), 
            'client_secret'    : (1, 'sp-ops-conciliations-secret')
        } 
    }, 
    'prd' : {
        'scope'   : 'ops-conciliations', 
        'storage' : 'stlakehyliaprd', 
        'sp_keys' : {
            'tenant_id'        : (1, 'aad-tenant-id'), 
            'subscription_id'  : (1, 'sp-ops-conciliations-subscription'), 
            'client_id'        : (1, 'sp-ops-conciliations-client'), 
            'client_secret'    : (1, 'sp-ops-conciliations-secret')
        } 
    }
}


DATALAKE_PATHS = {
    'spei'  : "ops/transactions/spei", 
    'blob'  : "https://{}.blob.core.windows.net/",   # STORAGE
    'abfss' : "abfss://{}@{}.dfs.core.windows.net",  # CONTAINER(bronze|silver|gold), STORAGE
    'from-cms' : "ops/regulatory/card-management/transformation-layer", 
    'prepared' : "ops/regulatory/card-management/transformation-layer/unzipped-ready", 
    'reports'  : "ops/regulatory/transformation-layer",
    'datasets' : "ops/card-management/datasets",
}

DELTA_TABLES = {
    'DAMNA' : ('damna',  'damna' , 'din_clients.slv_ops_cms_damna_stm'), 
    'ATPTX' : ('atpt',   'atpt'  , 'farore_transactions.slv_ops_cms_atptx_stm'), 
    'DAMBS1': ('dambs',  'dambs' , 'nayru_accounts.slv_ops_cms_dambs_stm'), 
    'DAMBS2': ('dambs2', 'dambs2', 'nayru_accounts.slv_ops_cms_dambs2_stm'), 
    'DAMBSC': ('dambsc', 'dambsc', 'nayru_accounts.slv_ops_cms_dambsc_stm')}


#  DAMBS
#  AccountNumber STRING,
#  CustomerNumber STRING,
#  CardExpirationDate STRING,
#  NumberUnblockedCards INT,
#  CurrentBalanceSign STRING,
#  CurrentBalance FLOAT,
#  date DATE ... partition by. 

# DAMNA
#  CustomerNumber STRING,F
#  Municipality STRING,
#  GenderCode STRING,
#  City STRING,
#  NameTypeIndicator STRING,
#  date DATE * partition by 

# ATPTX
# AccountNumber STRING,
# EffectiveDate STRING,
# TransactionType STRING,
# TransactionSign STRING,
# TransactionCode STRING,
# TransactionAmountSign STRING,
# TransactionAmount STRING,
#  AcceptorCategoryCode STRING,
#  TransactionChannel INT,
#  date DATE *partition by



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


class ConfigEnviron():
    '''
    This class sets up the initial authentication object.  It reads its 
    ENV_TYPE or cycle [dev,qas,prod] and SERVER(_TYPE) (local,dbks,wap). 
    And from then establishes its first secret-getter in order to later 
    establish its identity wether by a managed identity or service principal.  
    From then on, use PlatformResourcer to access other resources. 
    '''
    def __init__(self, env_type, server, spark=None):
        self.env = env_type
        self.spark = spark
        self.server = server
        self.config = RESOURCE_SETUP[env_type]
        self.set_secretters()

    def set_secretters(self): 
        if  self.server == 'local':
            if load_dotenv is None: 
                raise Exception("Failed to load library DOTENV.")
            load_dotenv('.env', override=True)        
            get_secret = lambda key: getenv(re.sub('-', '_', key.upper()))
            
        elif self.server == 'dbks': 
            if self.spark is None: 
                raise("Please provide a spark context on ConfigEnviron init.")
            dbutils = DBUtils(self.spark)
            get_secret = (lambda key: 
                dbutils.secrets.get(scope=self.config['scope'], key=re.sub('_', '-', key.lower())))
            
        def call_dict(a_dict): 
            map_val = (lambda a_val: 
                 get_secret(a_val[1]) if isinstance(a_val, tuple) else a_val)
            return {k: map_val(v) for (k, v) in a_dict.items()}

        self.get_secret = get_secret
        self.call_dict  = call_dict

        
    def set_credential(self):
        if not hasattr(self, 'call_dict'): 
            self.set_secretters()
            
        if self.server in ['local', 'dbks']: 
            principal_keys = self.call_dict(self.config['sp_keys'])
            the_creds = ClientSecretCredential(**principal_keys)
        elif self.server in ['wap']: 
            the_creds = DefaultAzureCredential()
        self.credential = the_creds
        
        
    def sparktransfer_credential(self): 
        if not hasattr(self, 'call_dict'): 
            self.set_secretters()
        # Assume account corresponding to BLOB_KEY is GEN2.  
        # and permissions are unlocked directly via CONFIG.SETUP_KEYS
        
        sp_dict  = self.config['sp_keys']
        blob_key = self.config['storage']
        gen_value = 'gen2'  # Storage is assumed GEN2
        
        tenant_id = self.get_secret(sp_dict['tenant_id'][1]) 
        oauth2_endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        if gen_value == 'gen2':
            pre_confs = {
                f"fs.azure.account.auth.type.{blob_key}.dfs.core.windows.net"           : 'OAuth',
                f"fs.azure.account.oauth.provider.type.{blob_key}.dfs.core.windows.net" : "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                f"fs.azure.account.oauth2.client.endpoint.{blob_key}.dfs.core.windows.net" : oauth2_endpoint,
                f"fs.azure.account.oauth2.client.id.{blob_key}.dfs.core.windows.net"    : sp_dict['client_id'],
                f"fs.azure.account.oauth2.client.secret.{blob_key}.dfs.core.windows.net": sp_dict['client_secret']}
        elif gen_value == 'gen1': 
            pre_confs = {
                f"fs.adl.oauth2.access.token.provider.type"    : 'ClientCredential', 
                f"fs.adl.account.{blob_key}.oauth2.client.id"  : sp_dict['client_id'],     # aplication-id
                f"fs.adl.account.{blob_key}.oauth2.credential" : sp_dict['client_secret'], # service-credential
                f"fs.adl.account.{blob_key}.oauth2.refresh.url": oauth2_endpoint}
        elif gen_value == 'v2': 
            pre_confs = {f"fs.azure.account.key.{blob_key}.blob.core.windows.net": sp_dict['sas_string']}
        
        for a_conf, its_val in self.call_dict(pre_confs).items():
            print(f"{a_conf} = {its_val}")
            self.spark.conf.set(a_conf, its_val)
          
        

    
