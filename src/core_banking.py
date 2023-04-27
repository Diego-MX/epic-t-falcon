# Diego Villamil, EPIC
# CDMX, 4 de noviembre de 2021

from collections import ChainMap
from datetime import datetime as dt, timedelta as delta, date
from httpx import (Client, AsyncClient, 
    Auth as AuthX, post as postx, BasicAuth as BasicX)
from itertools import groupby
from json import dumps, decoder, loads
from math import ceil
from operator import itemgetter
import pandas as pd
from pandas import Series as pd_S, DataFrame as pd_DF
from typing import Union
from pytz import timezone
from uuid import uuid4

try: 
    from delta.tables import DeltaTable as Δ
except ImportError: 
    Δ = None
try: 
    from pyspark.sql import (functions as F, types as T, 
        DataFrame as spk_DF)
except ImportError:
    F = T = spk_df = None
try: 
    import xmltodict
except ImportError:
    xmltodict = None

from src.utilities import encode64, dict_minus, snake_2_camel
from src.core_models import Fee, FeeSet
from config import (ENV, RESOURCE_SETUP, 
    DATALAKE_PATHS as paths)

resources      = RESOURCE_SETUP[ENV]
slv_path       = paths['abfss'].format('silver', resources['storage'])
at_withdrawals = f"{slv_path}/{paths['withdrawals']}/delta"

API_LIMIT = 500
cdmx_tz = timezone('America/Mexico_City')

    
def date_2_pandas(sap_srs: pd_S, mode='/Date') -> pd_S:
    if mode == '/Date': 
        dt_regex  = r"/Date\(([0-9]*)\)/"
        epoch_srs = sap_srs.str.extract(dt_regex, expand=False)
        pd_date   = pd.to_datetime(epoch_srs, unit='ms')
    elif mode == 'yyyymmdd': 
        pd_date = pd.to_datetime(sap_srs, format='%Y%m%d')
    return pd_date


class SAPSession(Client): 
    # INIT from CONFIG.CONFIGENVIRON()
    def __init__(self, core_obj): 
        self.config     = core_obj['config']
        self.call_dict  = core_obj['call-dict']
        self.get_secret = core_obj['get-secret']
        
        super().__init__()
        self.base_url = core_obj['config']['main']['url']
        self.headers = self.std_headers
        self.set_auth()

    # Tech Specs
    std_headers = {
        'Accept-Encoding' : 'gzip, deflate',
        'Content-Type'    : 'application/json',
        'Accept' : 'application/json',
        'Format' : 'json'}
        
    api_calls = {
        'events-set' : {
            'persons'      : "v15/bp/EventSet", 
            'accounts'     : "v1/cac/EventSet", 
            'transactions' : "v1/bape/EventSet", 
            'prenotes'     : "v1/bapre/EventSet" },
        'person-set'       : "v15/bp/PersonSet",
        'contract-set'     : "v1/lacovr/ContractSet",
        'contract-qan'     : "v1/lacqan/ContractSet",
        'contract-current' : "v1/cac/ContractSet",
        'contract-loans'   : "v1/lac/ContractSet", 
        'fees-apply'       : "v1/feemass/FeeSet",
        'fees-verify'      : "v1/feemass/StatusFeeSet", 
        'api-defs'         : "v1/oapi/oAPIDefinitionSet" } 
    
    
    # Business Specs
    commission_labels = {
        'atm': (600405, 'ops-commissions-atm-001')}
    
    
    def process_commissions_atpt(self, spark,
            atpt_df: Union[spk_DF, pd_DF], 
            cmsn_key='atm', **kwargs): 
        
        by_k   = kwargs.get('how-many', API_LIMIT)
        update = kwargs.get('update', True)
        
        feemass_set = {f'feemass_{a_col}' for a_col in ['external_id', 'process_date']}
        fees_set    = {f'fee_{a_col}' for a_col in ['type_code', 'posting_date', 'value_date', 
                    'amount', 'currency', 'payment_note']}
        
        base_wdraws = spark.read.load(at_withdrawals)                       
        join_wdraws = ("b_core_acct = account_id" 
            + " AND atpt_mt_ref_nbr = txn_ref_number")
        w_status    = {'b_wdraw_commission_status': 'feemass_status'}
        
        fees_iterator = self.iter_feemass_commissions(atpt_df, cmsn_key, by_k)
        responses = []
        for feeset in fees_iterator: 
            fees_resp = self.call_feeset(feeset)
            responses.append(fees_resp)
            if fees_resp.status_code == 200 and update: 
                fees_df  = feeset.as_dataframe(w_status=1)
                fees_spk = spark.createDataFrame(fees_df).toDF()
                (base_wdraws
                    .merge(fees_spk, join_wdraws)
                    .whenMatchedUpdate(
                        set={**w_status, **fees_set, **feemass_set})
                    .execute())
        return responses
    
    
    def verify_commissions_atpt(self, **f_args):
        getters = {'url': self.api_calls['fees-verify']}             
        if f_args: 
            getters['params'] = {
                '$filter': expr_dict_2_filter(f_args, camel_keys=False)}
        the_resp = self.get(**getters)
        return self.hook_d_results(the_resp, {'__metadata'})
        
    
    def iter_feemass_commissions(self, 
            atpt_df: Union[pd_DF, spk_DF], 
            cmsn_key='atm', by_k=API_LIMIT):
        
        if isinstance(atpt_df, pd_DF):
            row_itr = atpt_df.iterrows()
            len_df  = len(atpt_df)
        elif isinstance(atpt_df, spk_DF): 
            row_itr = enumerate(atpt_df.rdd.toLocalIterator())
            len_df  = atpt_df.count()
        iter_key = lambda ii_row: ii_row[0]//by_k
        n_grps   = ceil(len_df/by_k)
        
        ## Execution 
        cmsn_id, cmsn_name = self.commission_labels[cmsn_key]
        fees_fixed = {
            'type_code'    : cmsn_id, 
            'currency'     : 'MXN', 
            'payment_note' : f"{cmsn_key}, {cmsn_name}"}
               
        for kk, sub_itr in groupby(row_itr, iter_key): 
            print(f'Calling group {kk+1} of {n_grps}.')
            now_dt = dt.now(tz=cdmx_tz)
            a_uuid = str(uuid4())
            
            fees_ls = [ Fee(**fees_fixed, 
                    txn_ref_number = rr['atpt_mt_ref_nbr'], 
                    account_id     = rr['b_core_acct'], 
                    posting_date   = now_dt,
                    value_date     = now_dt
                    ) for _, rr in sub_itr]
            feeset_obj = FeeSet(
                process_date = now_dt, 
                external_id  = a_uuid, 
                fee_detail   = fees_ls)
            yield feeset_obj
        
        
    def call_feeset(self, feemass: FeeSet, merge=False): 
        fee_data = feemass.json(by_alias=True,
            exclude={'fee_detail': {'__all__': {'txn_ref_number'}}})
        posters = {
            'url' : f"{self.api_calls['fees-apply']}", 
            'data': fee_data}
        the_resp = self.post(**posters)
        
        return the_resp
    
    
    def call_person_set(self, params_x={}, **kwargs): 
        # Remove Keys from response list. 
        output   = kwargs.get('output',  'DataFrame')
        how_many = kwargs.get('how_many', API_LIMIT)
        rm_keys  = ['__metadata', 'Roles', 'TaxNumbers', 'Relation', 'Partner', 'Correspondence']
        
        params = {'$top': how_many, '$skip': 0}
        params.update(params_x)
        p_getters = {
            'url'   : self.api_calls['person-set'], 
            'params': params}  # pass as reference, ;)
        
        post_persons   = []
        post_responses = []
        while True:
            prsns_resp = self.get(p_getters)
            post_responses.append(prsns_resp)
            if output == 'Response': 
                return 
            
            prsns_ls = self.hook_d_results(prsns_resp)
            post_persons.extend(prsns_ls)
            
            params['$skip'] += len(prsns_ls)
            if (len(prsns_ls) < how_many) : 
                break
        
        # Procesamiento de Output.  
        if output == 'Response': 
            return prsns_resp
        
        elif output == 'List': 
            return post_persons
        
        elif output == 'DataFrame': 
            persons_mod = [dict_minus(a_person, rm_keys) for a_person in post_persons]
            persons_df = (pd.DataFrame(persons_mod)
                .assign(ID = lambda df: df.ID.str.pad(10, 'left', '0')))
            return persons_df
        
        else: 
            raise Exception(f'Output {output} is not valid.')

            
    def set_auth(self): 
        auth_args = {
            'url' : self.config['auth']['url'], 
            'data': self.call_dict(self.config['auth']['data']), 
            'auth': BasicX(**self.call_dict(self.config['main']['access'])),
            'headers': {'Content-Type': "application/x-www-form-urlencoded"}
        }
        self.auth = SAPAuth('any_initial_token', auth_args)
        
    
    def hook_d_results(self, response, rm_keys=None): 
        hook_allowed_types = ['application/json', 'application/atom+xml']
        
        the_type = response.headers['Content-Type']
        if   'application/json' in the_type: 
            the_json = response.json()
            the_results = the_json['d']['results']
        elif 'application/atom+xml' in the_type: 
            the_results = _xml_results(response.text)
        else: 
            raise Exception(f"Couldn't extract results from response with content type '{the_type}'.")
        
        if rm_keys:
            for e_result in the_results:
                for kk in rm_keys: 
                    e_result.pop(kk)
                
        return the_results



def expr_dict_2_filter(a_dict, camel_keys=False): 
    """SAP admits specific filter constructs."""
    def item_filter(a_key, a_val): 
        a_fltr = (f"({a_key} eq '{a_val}')" if isinstance(a_val, str)
            else " or ".join(f"({a_key} eq '{v_i}')" for v_i in a_val))
        return f"({a_fltr})"
    
    mod_key = snake_2_camel if camel_keys else (lambda key: key)
    d_fltr = " and ".join(item_filter(mod_key(kk), vv) 
            for kk, vv in a_dict.items())
    return d_fltr


def datetime_2_filter(sap_dt, range_how=None) -> str: 
    """Filtros que sirven para la API de eventos."""
    dt_string = (lambda a_dt: 
            "datetime'{}'".format(a_dt.strftime('%Y-%m-%dT%H:%M:%S')))
    if range_how == 'functions': 
        dt_clause = lambda cmp_dt : "{}(EventDateTime,{})".format(*cmp_dt)
    else: 
        dt_clause = lambda cmp_dt : "EventDateTime {} {}".format(*cmp_dt)
        
    if isinstance(sap_dt, (dt, date)):
        sap_filter = dt_clause(['ge', dt_string(sap_dt)])
        return sap_filter
    
    two_cond = (isinstance(sap_dt, (list, tuple)) 
            and (len(sap_dt) == 2) 
            and (sap_dt[0] < sap_dt[1]))
    if not two_cond: 
        raise Exception("SAP_DT may be DATETIME or LIST[DT1, DT2] with DT1 < DT2")
    
    into_clauses = zip(['ge', 'lt'], map(dt_string, sap_dt))
    # ... = (['ge', dtime_string(sap_dt[0])], 
    #        ['lt', dtime_string(sap_dt[1])])
    map_clauses = map(dt_clause, into_clauses)
    if range_how in [None, 'and']: 
        sap_filter = ' and '.join(map_clauses)
    elif range_how == 'expand_list':  
        sap_filter = list(map_clauses)
    elif range_how == 'between':
        sap_filter = ("EventDateTime between datetime'{}' and datetime'{}'"
            .format(*map_clauses))
    elif range_how == 'functions': 
        sap_filter = 'and({},{})'.format(*map_clauses)
    return sap_filter
        

def str_error(an_error): 
    try: 
        a_json = an_error.json()
        return dumps(a_json, indent=2)
    except Exception: 
        return str(an_error)


def _xml_results(xml_text): 
    get_entry_ds = (lambda entry_dict: 
        {re.sub(r'^d\:', '', k_p): v_p 
        for k_p, v_p in entry_dict.items() if k_p.startswith('d:')})

    entry_ls = xmltodict.parse(xml_text)['feed']['entry']
    entry_props = [entry['content']['m:properties'] for entry in entry_ls]
    entry_rslts = [get_entry_ds(prop) for prop in entry_props]
    return entry_rslts


class SAPAuth(AuthX): 
    def __init__(self, token, post_args):
        self.token = token
        self.post_args = post_args
    
    def auth_flow(self, request):
        response = yield request
        if response.status_code == 401:
            j_token = postx(**self.post_args).json()
            self.token = j_token['access_token']
            request.headers['Authorization'] = f"Bearer {self.token}"
            yield request
            
   

        
    
 