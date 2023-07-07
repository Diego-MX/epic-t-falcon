# Diego Villamil, EPIC
# CDMX, 4 de noviembre de 2021

from datetime import datetime as dt, date
from httpx import (Client, Auth as AuthX, post as postx, BasicAuth as BasicX)
from itertools import groupby
from json import dumps
from math import ceil
import pandas as pd
from pandas import DataFrame as pd_DF
from pyspark.sql import (DataFrame as spk_DF)
import re
from typing import Union
from pytz import timezone
from uuid import uuid4
import xmltodict

from epic_py.tools import dict_plus

from src.core_models import Fee, FeeSet

API_LIMIT = 500


    
def date_2_pandas(sap_srs: pd.Series, mode='/Date') -> pd.Series:
    if mode == '/Date': 
        dt_regex  = r"/Date\(([0-9]*)\)/"
        epoch_srs = sap_srs.str.extract(dt_regex, expand=False)
        pd_date   = pd.to_datetime(epoch_srs, unit='ms')
    elif mode == 'yyyymmdd': 
        pd_date = pd.to_datetime(sap_srs, format='%Y%m%d')
    return pd_date


def datetime_2_filter(sap_dt, range_how=None) -> str: 
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
        


class SAPSession(Client): 
    # INIT from CONFIG.CONFIGENVIRON()
    def __init__(self, core_obj): 
        self.config     = core_obj['config']
        self.call_dict  = core_obj['call-dict']
        self.get_secret = core_obj['get-secret']
        
        super().__init__()
        self.base_url = core_obj['config']['main']['url']
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
        'api-defs'         : "v1/oapi/oAPIDefinitionSet" } 
    
    # Business Specs
    commission_labels = {
        'atm': (600405, 'ops-commissions-atm-001')}
        
        
    def call_txns_commissions(self, 
            accounts_df: Union[pd_DF, spk_DF], 
            cmsn_key='atm', **kwargs): 
        
        ## Setup. 
        by_k = kwargs.get('how-many', API_LIMIT)
        
        if isinstance(accounts_df, pd_DF):
            row_itr = accounts_df.iterrows()
            len_df  = len(accounts_df)
            
        elif isinstance(accounts_df, spk_DF): 
            row_itr = enumerate(accounts_df.rdd.toLocalIterator())
            len_df  = accounts_df.count()
        
        iter_key = lambda ii_row: ii_row[0]//by_k
        n_grps   = ceil(len_df/by_k)
        
        ## Execution 
        cmsn_id, cmsn_name = self.commission_labels[cmsn_key]
        
        fees_fix = {
            'TypeCode'   : cmsn_id, 
            'Currency'   : 'MXN', 
            'PaymentNote': f"{cmsn_key}, {cmsn_name}"}
        
        responses = []
        for kk, sub_itr in groupby(row_itr, iter_key): 
            print(f'Calling group {kk} of {n_grps}.')
            an_id = str(uuid4())
            now_str = dt.now(tz=timezone('America/Mexico_City')).strftime('%Y%m%d')
            fees_set = [Fee(**fees_fix, **{
                'AccountID'  : rr['b_sap_savings'], 
                'PostingDate': now_str, 
                'ValueDate'  : now_str}) for _, rr in sub_itr]
            feeset_obj = FeeSet(**{
                'ProcessDate': now_str, 
                'ExternalID' : an_id, 
                'FeeDetail'  : fees_set})
            posters = {
                    'url' : f"{self.api_calls['fees-apply']}", 
                    'data': feeset_obj.json()}
            the_resp = self.post(**posters)
            responses.append(the_resp) 
        return responses
    
    
    def pos_txn_commission(self, txn_resp): 
        # if txn_resp.status_code == 201: 
        #    feeseters = ['ProcessDate', 'ExternalID']
        #    post_args = txn_resp.json()['d']
        #    the_fees  = loads(txn_resp.request.content)['FeeDetail']
        pass
        
        
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
            persons_mod = [ dict_plus(a_person).difference(rm_keys) 
                for a_person in post_persons]
            persons_df = (pd_DF(persons_mod)
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
        self.auth = SAPAuth('any_wrong_token', auth_args)
        
    
    def hook_d_results(self, response): 
        # hook_allowed_types = ['application/json', 'application/atom+xml']
        
        the_type = response.headers['Content-Type']
        if   'application/json' in the_type: 
            the_json = response.json()
            the_results = the_json['d']['results']
        elif 'application/atom+xml' in the_type: 
            the_results = _xml_results(response.text)
        else: 
            raise Exception(f"Couldn't extract results from response with content type '{the_type}'.")
            
        return the_results
        

    def update_token(self, token): 
        auth_args = {
            'url' : self.config['auth']['url'], 
            'data': self.call_dict(self.config['auth']['data']), 
            'auth': BasicX(**self.call_dict(self.config['main']['access'])),
            'headers': {'Content-Type': "application/x-www-form-urlencoded"}
        }
        j_token = postx(**auth_args).json()
        self.auth = Bearer2(j_token['access_token'])
        
    
    

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
            
    
class Bearer2(AuthX): 
    # Funciona para las dos conexiones. 
    def __init__(self, token_str): 
        self.token = token_str 

    def auth_flow(self, a_request): 
        a_request.headers['Authorization'] = f"Bearer {self.token}"
        yield a_request

    def __call__(self, a_request):
        a_request.headers['Authorization'] = f"Bearer {self.token}"
        return a_request

        
    
 
