import base64
from datetime import datetime as dt
from operator import attrgetter
import pandas as pd
from pandas.core import series
from pandas.core.frame import DataFrame as pd_DF
from pyspark.sql.dataframe import DataFrame as spk_DF
from os import path
import re
from typing import Union

try: 
    from pyspark.dbutils import DBUtils
except ImportError: 
    DBUtils = None


encode64 = (lambda a_str: 
    base64.b64encode(a_str.encode('ascii')).decode('ascii'))


def dict_minus(a_dict, key_ls, copy=True): 
    b_dict = a_dict.copy() if copy else a_dict
    for key in key_ls: 
        b_dict.pop(key, None)
    return b_dict


def pd_print(a_df: pd_DF, width=180): 
    options = ['display.max_rows',    None, 
               'display.max_columns', None, 
               'display.width',       width]
    with pd.option_context(*options):
        print(a_df)
        

def dirfiles_df(a_dir, spark=None) -> pd_DF:
    if spark is None: 
        raise Exception('Must provide Spark Session.')
        
    dbutils = DBUtils(spark)
    file_attrs = ['modificationTime', 'name', 'path', 'size']
    get_file_attrs = attrgetter(*file_attrs)
    
    files_ls = [get_file_attrs(filish) for filish in dbutils.fs.ls(a_dir)]
    files_df = (pd.DataFrame(files_ls, columns=file_attrs)
        .assign(**{'modificationTime': lambda df: 
                pd.to_datetime(df['modificationTime'], unit='ms')}))
    return files_df


def file_exists(a_path: str): 
    if a_path.startswith('/dbfs'):
         return path.exists(a_path)
    else:
        try:
            dbutils.fs.ls(a_path)
            return True
        except Exception as e:
            if 'java.io.FileNotFoundException' in str(e):
                return False
        else:
            raise Exception("Can't find file {a_path}.")



def pd_print(a_df: pd_DF, width=180): 
    options = ['display.max_rows', None, 
               'display.max_columns', None, 
               'display.width', width]
    with pd.option_context(*options):
        print(a_df)

        
def len_cols(cols_df: pd_DF) -> int: 
    # Ya ni me acuerdo para que sirve. 
    req_cols = set(['aux_fill', 'Length'])
    
    if not req_cols.issubset(cols_df.columns): 
        raise "COLS_DF is assumed to have attributes 'AUX_FILL', 'LENGTH'."
    
    last_fill = cols_df['aux_fill'].iloc[-1]
    up_to = -1 if last_fill else len(cols_df)
    the_len = cols_df['Length'][:up_to].sum()
    return int(the_len)
    
    
