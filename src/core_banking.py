# Diego Villamil, EPIC
# CDMX, 4 de noviembre de 2021

from datetime import datetime as dt, timedelta as delta, date
from httpx import AsyncClient, Auth as AuthX
from itertools import groupby
from json import dumps, decoder
from math import ceil
import pandas as pd
from pandas.core import series, frame 
import requests
from requests import auth, exceptions, Session
from typing import Union
from pytz import timezone

try: 
    from delta.tables import DeltaTable
except ImportError: 
    DeltaTable = None
try: 
    from pyspark.sql import types as T, dataframe as spk_df
except ImportError:
    spk_df, T = None
try: 
    import xmltodict
except ImportError:
    xmltodict = None
    
from src.core_models import Fee, FeeSet
from src.utilities import encode64, dict_minus

API_LIMIT = 500



def str_error(an_error): 
    try: 
        a_json = an_error.json()
        return dumps(a_json, indent=2)
    except Exception: 
        return str(an_error)

    
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
        

class BearerAuth2(auth.AuthBase, AuthX): 
    # Funciona para las dos conexiones. 
    def __init__(self, token_str): 
        self.token = token_str 

    def auth_flow(self, a_request): 
        a_request.headers['Authorization'] = f"Bearer {self.token}"
        yield a_request

    def __call__(self, a_request):
        a_request.headers['Authorization'] = f"Bearer {self.token}"
        return a_request
    

class SAPSession(Session): 
# INIT from CONFIG.CONFIGENVIRON()
    def __init__(self, core_obj, set_token=True): 
        super().__init__()
        self.config     = core_obj['config']
        self.call_dict  = core_obj['call-dict']
        self.get_secret = core_obj['get-secret']
        self.base_url   = core_obj['config']['main']['url']
        self.headers.update(self.std_headers)
        self.set_token()
        self.set_status_hook()
        
    # Tech Specs
    std_headers = {
            'Accept-Encoding' : 'gzip, deflate',
            'Content-Type'    : 'application/json',
            'Accept'   : 'application/json',
            'Format'   : 'json'}
        
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
                accounts_df: Union[frame.DataFrame, spk_df.DataFrame], 
                cmsn_key='atm', **kwargs): 
        
        cmsn_id, cmsn_name = self.commission_labels[cmsn_key]
        
        by_k = kwargs.get('how-many', API_LIMIT)
        date_str = dt.now(tz=timezone('America/Mexico_City')).strftime('%Y-%m-%d')
        
        if isinstance(accounts_df, frame.DataFrame):
            row_itr = accounts_df.iterrows()
            len_df  = len(accounts_df)
            
        elif isinstance(accounts_df, spk_df.DataFrame): 
            row_itr = enumerate(accounts_df.rdd.toLocalIterator())
            len_df  = accounts_df.count()
        
        iter_key = lambda ii_row: ii_row[0]//by_k
        n_grps   = ceil(len_df/by_k)
        
        responses = []
        # Usamos la Response completa, pero seguramente querremos algo más estructurado. 
        for kk, sub_itr in groupby(row_itr, iter_key): 
            print(f'Calling group {kk} of {n_grps}.')
            fees_set   = [Fee(**{
                'AccountID'  : row[1]['atpt_acct'], 
                'TypeCode'   : cmsn_id, 
                'PostingDate': date_str}) for row in sub_itr]
            feeset_obj = FeeSet(**{
                'ProcessDate': date_str, 
                'ExternalID' : cmsn_name, 
                'FeeDetail'  : fees_set})
            posters = {
                    'url' : f"{self.base_url}/{self.api_calls['fees-apply']}", 
                    'data': feeset_obj.json()}
            try: 
                the_resp = self.post(**posters)
            except Exception as ex: 
                the_resp = {'Error': feeset_obj}
            responses.append(the_resp)            
        return responses
    
    
    def call_person_set(self, params={}, **kwargs): 
        # Remove Keys from response list. 
        output   = kwargs.get('output',  'DataFrame')
        how_many = kwargs.get('how_many', API_LIMIT)
        rm_keys  = ['__metadata', 'Roles', 'TaxNumbers', 'Relation', 'Partner', 'Correspondence']
        
        params_0 = {'$top': how_many, '$skip': 0}
        params_0.update(params)

        post_persons   = []
        post_responses = []
        while True:
            prsns_resp = self.get(f"{self.base_url}/{self.api_calls['person-set']}", params=params_0)
            post_responses.append(prsns_resp)
            if output == 'Response': 
                return 
            
            prsns_ls = self.hook_d_results(prsns_resp)
            post_persons.extend(prsns_ls)
            
            params_0['$skip'] += len(prsns_ls)
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
        
    
    def set_token(self, verbose=0): 
        posters = {
            'url' : self.config['auth']['url'], 
            'data': self.call_dict(self.config['auth']['data']), 
            'auth': auth.HTTPBasicAuth(**self.call_dict(self.config['main']['access'])), 
            'headers': {'Content-Type': "application/x-www-form-urlencoded"}
        }
        
        # This POST is independent of the session itself; 
        # hence called from REQUESTS, instead of SELF. 
        the_resp = requests.post(**posters)
        
        if verbose > 0: 
            print(f"Status Code: {the_resp.status_code}")
            
        if the_resp.status_code == 200: 
            self.token = the_resp.json()
            self.auth  = BearerAuth2(self.token['access_token'])
        else: 
            if verbose > 0:
                print(f"Post request headers: {the_resp.request.headers}")
            raise Exception(f"Error Authenticating; STATUS_CODE: {the_resp.status_code}")
            
        
    def set_status_hook(self, auth_tries=3): 
        def status_hook(response, *args, **kwargs): 
            response_1 = response
            for ii in range(auth_tries): 
                if response_1.status_code == 200: 
                    break
                elif response_1.status_code == 401: 
                    self.set_token()
                    response_1 = self.send(response.request)
                    continue
                else: 
                    response_1.raise_for_status()
            return response_1
        
        self.hooks['response'].append(status_hook)
    
    
    def hook_d_results(self, response): 
        hook_allowed_types = ['application/json', 'application/atom+xml']
        
        the_type = response.headers['Content-Type']
        if 'application/json' in the_type: 
            the_json = response.json()
            the_results = the_json['d']['results']
        elif 'application/atom+xml' in the_type: 
            the_results = self._xml_results(response.text)
        else: 
            raise Exception(f"Couldn't extract results from response with content type '{the_type}'.")
            
        return the_results
        
   
    def _xml_results(self, xml_text): 
        get_entry_ds = (lambda entry_dict: 
            {re.sub(r'^d\:', '', k_p): v_p 
            for k_p, v_p in entry_dict.items() if k_p.startswith('d:')})
        
        entry_ls = xmltodict.parse(xml_text)['feed']['entry']
        entry_props = [entry['content']['m:properties'] for entry in entry_ls]
        entry_rslts = [get_entry_ds(prop) for prop in entry_props]
        return entry_rslts


    


    
    
    
    
    