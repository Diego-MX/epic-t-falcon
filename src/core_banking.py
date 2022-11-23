# Diego Villamil, EPIC
# CDMX, 4 de noviembre de 2021

import base64
from datetime import datetime as dt, timedelta as delta, date
from httpx import AsyncClient, Auth as AuthX
from itertools import product
from json import dumps
import pandas as pd
from pandas.core import series, frame 
import requests
from requests import auth, exceptions, Session


try: 
    from delta.tables import DeltaTable
except ImportError: 
    DeltaTable = None
try: 
    from pyspark.sql import types as T
except ImportError:
    types = None

from src.core_models import Fee, FeeSetRequest
   

encode64 = (lambda a_str: 
    base64.b64encode(a_str.encode('ascii')).decode('ascii'))


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
        std_headers = {
            'Content-Type' : "application/json",
            'Accept-Encoding' : "gzip, deflate",
            'format': 'json'}
        
        super().__init__()
        self.config     = core_obj['config']
        self.call_dict  = core_obj['call-dict']
        self.get_secret = core_obj['get-secret']
        self.headers.update(std_headers)
        self.set_token()
        self.set_status_hook()

    
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
    
    
    def txns_commissions(self, accounts_df, cmsn_type='atm', **kwargs): 
        the_types = {
            'atm': (600405, 'ops-commissions-atm-001')}

        if not isinstance(accounts_df, frame.DataFrame):  
            raise Exception('ACCOUNTS_DF must be a DataFrame (Pandas).')
            
        # SAP calls them fees; we, commissions.
        commissions_ls = [Fee(AccountID=row['atpt_acct'], TypeCode=kwargs['TypeCode'])
                for _, row in accounts_df.iterrows()]
        
        date_str = date.today().strftime('%Y-%m-%d')
        commission_requesters = {
            'ProcessDate': date_str, 
            'ExternalID' : the_types[cmsn_type][1], 
            'FeeDetail'  : commissions_ls}
        
        feeset_obj = FeeSetRequest(**commission_requesters)
        posters = {
            'url' : f"{self.config['main']['url']}/{self.api_calls['fees-apply']}", 
            'data': feeset_obj.json()}
        
        try: 
            f_response = self.post(**posters)
            f_response.raise_for_status()
        except exceptions.HTTPError as err:
            self.set_token()
            f_response = self.post(**posters)
        
        return f_response
    
    
    def set_token(self, verbose=0): 
        posters = {
            'url' : self.config['auth']['url'], 
            'data': self.call_dict(self.config['auth']['data']), 
            'auth': auth.HTTPBasicAuth(**self.call_dict(self.config['main']['access'])), 
            'headers': {'Content-Type': "application/x-www-form-urlencoded"}
        
        # This post is independent of the session itself.  
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
            response_1 = response.copy()
            for _ in range(auth_tries): 
                if   response_1.status_code == 200: 
                    break
                elif response_1.status_code == 401: 
                    self.set_token()
                    response_1 = self.send(response.request)
                    continue
                else: 
                    response_1.raise_for_status()
            return response_1
        
        self.hooks['response'].append(status_hook)
                
    
    
    


    
    
    
    
    