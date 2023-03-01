import base64
from datetime import datetime as dt
from operator import attrgetter
import pandas as pd
from pandas import Series as pd_S, DataFrame as pd_DF
from pathlib import Path
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


def snake_2_camel(snake, first_too=True):
    wrd_0, *wrds = snake.split('_')
    if first_too: 
        wrd_0 = wrd_0.capitalize()
    caps = (wrd_0, *(ww.capitalize() for ww in wrds))
    return ''.join(caps)
    
    
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


def path_type(a_path: str, spark): 
    try: 
        dbutils = DBUtils(spark)
        ls_1 = dbutils.fs.ls(a_path)
    except Exception as expt: 
        return (0, 'Error')
    
    if (len(ls_1) == 1) and (ls_1[0].path == a_path): 
        return (len(ls_1), 'File')
    else: 
        return (len(ls_1), 'Folder')
 
    
        

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
            raise Exception(f"Can't find file {a_path}.")




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
    
    
def dbks_path(a_path: Path): 
    a_str = str(a_path)
    if re.match(r'^(abfss|dbfs|file):', a_str): 
        b_str = re.sub(':/', '://', a_str)
    else: 
        b_str = a_str 
    return b_str
