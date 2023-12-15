from operator import attrgetter as σ
from os import environ, getenv, remove
from pathlib import Path
import re

from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from toolz import compose_left, valmap

from dotenv import load_dotenv
from epic_py.platform import EpicIdentity
from epic_py.tools import partial2

load_dotenv('.env', override=True)

# pylint: disable=line-too-long


SETUP_2 = {
    'dev': {},
    'qas': {
        'service-principal': {
            'tenant_id'        : 'aad-tenant-id',
            'subscription_id'  : 'sp-ops-conciliations-subscription',   # sp-core-events-suscription
            'client_id'        : 'sp-ops-conciliations-client',        # sp-core-events-client
            'client_secret'    : 'sp-ops-conciliations-secret'},       # sp-core-events-secret
        'databricks-scope' : 'dbks-ops-conciliations'},       # dbks-ops-conciliations
    'stg': {
        'databricks-scope': 'dbks-ops-conciliations',
        'service-principal' : {
            'tenant_id'        : 'aad-tenant-id',
            'subscription_id'  : 'sp-ops-conciliations-subscription',
            'client_id'        : 'sp-ops-conciliations-client',
            'client_secret'    : 'sp-ops-conciliations-secret'}},
    'prd': {
        'databricks-scope': 'dbks-ops-conciliations',  # dbks-ops-conciliations
        'service-principal' : {
            'tenant_id'        : 'aad-tenant-id',  
            'subscription_id'  : 'sp-ops-conciliations-subscription',  # sp-collections-subscription
            'client_id'        : 'sp-ops-conciliations-client',        # sp-collections-client
            'client_secret'    : 'sp-ops-conciliations-secret'}},      # sp-collections-secret
    'drp': {}
}

RESOURCES_2 = {
    'qas': {
        'storage' : 'stlakehyliaqas',
        'keyvault' : 'kv-opsConcil-data-qas', # kv-cx-data-qas
        'metastore': ('sqlserver-lakehylia-data-qas', 'lakehylia_metastore_qas')},
    'stg': {        
        'storage' : 'stlakehyliastg',
        'keyvault' : 'kv-opsConcil-adm-stg',
        'metastore': ('sqlserver-lakehylia-adm-stg', 'lakehylia_metastore_stg')},
    'prd': {
        'storage' : 'stlakehyliaprd',
        'keyvault': 'kv-opsConcil-data-prd',
        'metastore': ('sqlserver-lakehylia-data-prd', 'lakehylia_metastore_prd')},
    'drp': {}
}

CORE_2 = {
    'qas-sap': {
        'base_url': "https://apiqas.apimanagement.us21.hana.ondemand.com/s4b",
        'auth_url': "https://apiqas.apimanagement.us21.hana.ondemand.com/oauth2/token",
        'client_id'    : 'core-api-key',
        'client_secret': 'core-api-secret',
        'sap_username' : 'core-api-user',
        'sap_password' : 'core-api-password'},
    'prd-sap': {
        'base_url': "https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b",
        'auth_url': "https://latp-apim.prod.apimanagement.us20.hana.ondemand.com/oauth2/token",
        'client_id': 'core-api-key',
        'client_secret': 'core-api-secret',
        'sap_username' : 'core-api-user',
        'sap_password' : 'core-api-password'},
}

BLOB_PATHS = {
    #'btp'          : "ops/fraude/bronze/btp",
    #'spei'         : "ops/transactions/spei",
    #'spei2'        : "ops/fraude/bronze/spei",
    'spei-gfb'     : "ops/core-banking/conciliations/recoif",
    'spei-c4b'     : "ops/core-banking/conciliations/spei",
    #'from-cms'     : "ops/regulatory/card-management/transformation-layer",
    #'prepared'     : "ops/regulatory/card-management/transformation-layer/unzipped-ready", # Extraer y descomprimir
    #'reports'      : "ops/regulatory/transformation-layer", # R2422, SISPAGOS,
    'reports2'     : "ops/regulatory/conciliations",         # Ya no me acuerdo qué chingados.  
    'cms-data'     : "ops/card-management/datasets",        # transformation-layer (raw -> CuSn)
    #'withdrawals'  : "ops/account-management/withdrawals",  # todos los retiros
    'commissions'  : "ops/account-management/commissions",  # retiros de cajeros
    'conciliations': "ops/core-banking/conciliations"        # conciliación operativa y de SPEI. 
}


RESOURCE_SETUP = {
    # No hay DEV porque no se pudo hacer un flujo para DEV.
    # Entonces usamos QAS como si fuera DEV. 🤷
    'qas' : {
        'scope'   : 'eh-core-banking',
        'sp_keys' : { # oauth-databricks-qas
            'tenant_id'        : (1, 'aad-tenant-id'),
            'subscription_id'  : (1, 'sp-core-events-suscription'),
            'client_id'        : (1, 'sp-core-events-client'),
            'client_secret'    : (1, 'sp-core-events-secret')},
        'storage' : 'stlakehyliaqas',
        'keyvault': 'kv-cx-data-qas',
    },
    'stg' : {
        'scope'   : 'ops-conciliations-stg',
        'sp_keys' : {
            'tenant_id'        : (1, 'aad-tenant-id'),
            'subscription_id'  : (1, 'sp-ops-conciliations-subscription'),
            'client_id'        : (1, 'sp-ops-conciliations-client'),
            'client_secret'    : (1, 'sp-ops-conciliations-secret')
        }
    },
    'prd' : {
        'scope'   : 'ops-conciliations-prd',
        'sp_keys' : {
            'tenant_id'        : (1, 'aad-tenant-id'),
            'subscription_id'  : (1, 'sp-ops-conciliations-subscription'),
            'client_id'        : (1, 'sp-ops-conciliations-client'),
            'client_secret'    : (1, 'sp-ops-conciliations-secret')
        }
    }
}


CORE_SETUP = {
    # MAIN y AUTH se usan diferentes en las APIs de SAP.
    # ... cuidado con (USERNAME, PASSWORD) de cada uno.
    'dev-sap': {
        'vault': 'kv-collections-data-dev',
        'main' : {
            'url' : "https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b",
            'access' : {
                'username': (1, 'core-api-key'),
                'password': (1, 'core-api-secret') } },
            'auth' : {
                'url' : "https://latp-apim.prod.apimanagement.us20.hana.ondemand.com/oauth2/token",
                'data': {
                    'grant_type' : 'password',
                    'username'   : (1, 'core-api-user'),
                    'password'   : (1, 'core-api-password') } } },
    'qas-sap': {
        'main': {
            'url': "https://apiqas.apimanagement.us21.hana.ondemand.com/s4b",
            'access': {
                'username': (1, 'core-api-key'),
                'password': (1, 'core-api-secret')} },
        'auth' : {
            'url'  : "https://apiqas.apimanagement.us21.hana.ondemand.com/oauth2/token",
            'data' : {
                'grant_type': 'password',
                'username': (1, 'core-api-user'),
                'password': (1, 'core-api-password')} } } }

# Env-independent Usage Variables.

DATALAKE_PATHS = {
    'blob'         : "https://{}.blob.core.windows.net",    # STORAGE
    'abfss'        : "abfss://{}@{}.dfs.core.windows.net",   # CONTAINER(bronze|silver|gold), STORAGE
    'btp'          : "ops/fraude/bronze/btp",                # ¿?
    'spei'         : "ops/transactions/spei",                # ¿?
    'spei2'        : "ops/fraude/bronze/spei",               # SPEI (original y conciliación)
    'spei-gfb'     : "ops/core-banking/conciliations/recoif",# SPEI conciliación II.
    'spei-c4b'     : "ops/core-banking/conciliations/spei",
    'from-cms'     : "ops/regulatory/card-management/transformation-layer",  #
    'prepared'     : "ops/regulatory/card-management/transformation-layer/unzipped-ready",  # Extraer y descomprimir
    'reports'      : "ops/regulatory/transformation-layer",  # R2422, SISPAGOS,
    'reports2'     : "ops/regulatory/conciliations",         # Ya no me acuerdo qué chingados.
    'datasets'     : "ops/card-management/datasets",         # transformation-layer (raw -> CuSn)
    #'withdrawals'  : "ops/account-management/withdrawals",   # todos los retiros
    'commissions'  : "ops/account-management/commissions",   # retiros de cajeros
    'conciliations': "ops/core-banking/conciliations"        # conciliación operativa y de SPEI.
}

DELTA_TABLES = {
    'damna' : 'din_clients.slv_ops_cms_damna_stm',
    'atpt'  : 'farore_transactions.slv_ops_cms_atptx_stm',
    'DAMBS1': 'nayru_accounts.slv_ops_cms_dambs_stm',
    'DAMBS2': 'nayru_accounts.slv_ops_cms_dambs2_stm',
    'DAMBSC': 'nayru_accounts.slv_ops_cms_dambsc_stm'}


LAYER_SETUP = {
    'DateFormat' : '%Y%m%d',
    'DAMNA' : {
        'paths': {
            'zip'    : 'dbfs:/FileStore/',
            'origen' : 'dbfs:/FileStore/DAMNA.txt',
            'delta'  : 'dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/damna',
            'procesados' : 'dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/FilesUpload/DAMNA/DAMNA_Processed/'}},
    'ATPTX' : {
        'paths': {
            'zip'    : 'dbfs:/FileStore/',
            'origen' : 'dbfs:/FileStore/ATPTX.txt',
            'delta'  : 'dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/atptx',
            'alias'  : 'por rellenar Data Diego',
            'procesados' : 'dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/FilesUpload/ATPTX/ATPTX_Processed/'}},
    'DAMBS' : {
        'paths': {
            'zip'    : 'dbfs:/FileStore/',
            'origen' : 'dbfs:/FileStore/DAMBS.txt',
            'delta'  : 'dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/dambs',
            'alias'  : 'por rellenar Data Diego',
            'procesados' : 'dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/FilesUpload/DAMBS/DAMBS_Processed/'}},
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


class ConfigEnviron():
    '''
    This class sets up the initial authentication object.  It reads its
    ENV_TYPE or cycle [dev,qas,prod] and SERVER(_TYPE) (local,dbks,wap).
    And from then establishes its first secret-getter in order to later
    establish its identity wether by a managed identity or service principal.
    From then on, use PlatformResourcer to access other resources.
    '''
    def __init__(self, env_type, server, spark=None):
        print(f"Env, Server: {env_type}, {server}")
        self.env = env_type
        self.spark = spark
        self.server = server
        self.config = RESOURCE_SETUP[env_type]
        self.set_secretters()

    def set_secretters(self):
        if  self.server == 'local':
            if load_dotenv is None:
                raise Exception("Failed to load library DOTENV.")       # pylint: disable=broad-exception-raised
            load_dotenv('.env', override=True)
            get_secret = lambda key: getenv(re.sub('-', '_', key.upper()))

        elif self.server == 'dbks':
            if self.spark is None:
                raise("Please provide a spark context on ConfigEnviron init.")      # pylint: disable=raising-bad-type
            from pyspark.dbutils import DBUtils     # pylint: disable=import-error,import-outside-toplevel,no-name-in-module
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
        self.credential = the_creds     # pylint: disable=attribute-defined-outside-init


    def get_blob_service(self, account=None):
        if not hasattr(self, 'blob_services'):
            self.blob_services = {}     # pylint: disable=attribute-defined-outside-init

        if account is None:
            account = self.config['storage']

        if not hasattr(self, 'credential'):
            self.set_credential()

        the_url = f"https://{account}.blob.core.windows.net/"
        b_service = BlobServiceClient(the_url, credential=self.credential)
        self.blob_services[account] = b_service
        return b_service


    def upload_storage_blob(self, a_file, blob, container, account=None, overwrite=False):
        b_service = self.get_blob_service(account)

        the_blob = b_service.get_blob_service(container, blob)      # pylint: disable=no-member
        with open(a_file, 'rb') as _b:
            the_blob.upload_blob(_b, overwrite=overwrite)



    def download_storage_blob(self, a_file, a_blob, container, account=None, force=False):
        if force and Path(a_file).is_file():
            remove(a_file)

        if Path(a_file).is_file():
            return

        b_service = self.get_blob_service(account)
        the_blob = b_service.get_blob_client(container, a_blob)
        with open(a_file, 'wb') as _b:
            b_data = the_blob.download_blob()
            _b.write(b_data.readall())


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
                f"fs.azure.account.oauth2.client.endpoint.{blob_key}.dfs.core.windows.net": oauth2_endpoint,
                f"fs.azure.account.oauth2.client.id.{blob_key}.dfs.core.windows.net"    : sp_dict['client_id'],
                f"fs.azure.account.oauth2.client.secret.{blob_key}.dfs.core.windows.net": sp_dict['client_secret']}
        elif gen_value == 'gen1':
            pre_confs = {
                f"fs.adl.oauth2.access.token.provider.type"    : 'ClientCredential',        # pylint: disable=fstring-without-interpolation
                f"fs.adl.account.{blob_key}.oauth2.client.id"  : sp_dict['client_id'],      
                f"fs.adl.account.{blob_key}.oauth2.credential" : sp_dict['client_secret'],  
                f"fs.adl.account.{blob_key}.oauth2.refresh.url": oauth2_endpoint}
        elif gen_value == 'v2':
            pre_confs = {f"fs.azure.account.key.{blob_key}.blob.core.windows.net": sp_dict['sas_string']}

        for a_conf, its_val in self.call_dict(pre_confs).items():
            print(f"{a_conf} = {its_val}")
            self.spark.conf.set(a_conf, its_val)


    def use_vault(self, get_vault=None):
        # When get_vault is None, the default keyvault is set.
        # Otherwise, just return a non-default vault object.
        if not hasattr(self, 'credential'):
            self.set_credential()
        vault_name = get_vault if get_vault else self.config['keyvault']
        params =  {
            'vault_url' : f"https://{vault_name}.vault.azure.net/",
            'credential': self.credential}
        the_vault = SecretClient(**params)
        if get_vault is None:
            self.vault = the_vault
        return the_vault


    def prepare_coresession(self, core_env):
        core_config = CORE_SETUP[core_env]
        a_vault = self.use_vault(core_config.get('vault', None))

        is_tuple   = partial2(isinstance, ..., tuple)
        get_secret = compose_left(a_vault.get_secret, σ('value'))
        secret_2   = lambda vv: get_secret(vv[1]) if is_tuple(vv) else vv
        call_dict  = partial2(valmap, secret_2, ...)

        sessioner = {
            'config'    : core_config,
            'call-dict' : call_dict,
            'get-secret': get_secret}
        return sessioner


ENV = environ.get('ENV_TYPE')
SERVER = environ.get('SERVER_TYPE')
CORE_ENV = environ.get('CORE_ENV')

t_agent = EpicIdentity.create(SERVER, config=SETUP_2[ENV])
t_resourcer = t_agent.get_resourcer(RESOURCES_2[ENV], check_all=False)
s_secret = t_resourcer.get_vault_secretter()
t_core = t_agent.prep_sap_connect(CORE_2[CORE_ENV], s_secret)
