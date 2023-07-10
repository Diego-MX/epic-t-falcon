import base64
from datetime import datetime as dt
from delta.tables import DeltaTable as Δ
from functools import reduce, partial
from operator import attrgetter
from pathlib import Path
import pandas as pd
from pandas import Series as pd_S, DataFrame as pd_DF
from pyspark.dbutils import DBUtils
from pyspark.sql import (functions as F, types as T, 
    DataFrame as spk_DF, Column, Row)
from os import path
import re
from toolz.functoolz import identity, compose
from toolz.dicttoolz import valmap
from typing import Union



encode64 = (lambda a_str: 
    base64.b64encode(a_str.encode('ascii')).decode('ascii'))


def as_callable(yy): 
    λ_y = yy if callable(yy) else (lambda xx: yy)
    return λ_y
    

def snake_2_camel(snake, first_too=True, pre_sub={}, post_sub={}):
    f_sub = lambda wrd, k_v: re.sub(*k_v, wrd)
    
    snake_1 = reduce(f_sub, pre_sub.items(), snake)
    wrd_0, *wrds = snake_1.split('_')
    if first_too: 
        wrd_0 = wrd_0.capitalize()
    caps_1 = (wrd_0, *(ww.capitalize() for ww in wrds))
    caps_2 = ''.join(caps_1)
    caps = reduce(f_sub, post_sub.items(), caps_2)
    return caps
    
    
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
        return 'Not Exists'
    
    if (len(ls_1) == 1) and (ls_1[0].path == a_path): 
        return 'File'
    else: 
        return 'Folder'
 
        
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


def upsert_delta(spark, new_tbl, base_path, 
        how, on_cols:Union[list, dict]): 
    if how == 'simple': 
        if not isinstance(on_cols, list): 
            raise Exception("ON_COLS must be of type LIST.")
        on_str = " AND ".join(f"(t1.{col} = t0.{col})" for col in on_cols)
        
        (Δ.forPath(spark, base_path).alias('t0')
            .merge(new_tbl.alias('t1'), on_str)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
        
    elif how == 'delete': 
        if not isinstance(on_cols, dict): 
            raise Exception("ON_COLS must be of type DICT.")
        ids_cc, cmp_cc = map(on_cols.get, ['ids', 'cmp'])

        s_delete = (spark.read
            .load(base_path)
            .join(new_tbl, how='leftsemi', on=ids_cc)
            .join(new_tbl, how='leftanti', on=ids_cc + cmp_cc)
            .withColumn('to_delete', F.lit(True)))

        t_merge = (new_tbl
            .withColumn('to_delete', F.lit(False))
            .unionByName(s_delete))

        mrg_condtn = ' AND '.join(f"(t1.{cc} = t0.{cc})" for cc in ids_cc+cmp_cc) 
        new_values = {cc: F.col(f"t1.{cc}") for cc in new_tbl.columns}
        
        (Δ.forPath(spark, base_path).alias('t0')
            .merge(t_merge.alias('t1'), mrg_condtn)
            .whenMatchedDelete('to_delete')
            .whenMatchedUpdate(set=new_values)
            .whenNotMatchedInsert(values=new_values)
            .execute())
    return 


def get_date(a_col: Union[str, Column]) -> Column: 
    date_reg = r'20[\d\-]{8}'
    date_col = F.to_date(F.regexp_extract(a_col, date_reg, 0), 'yyyy-mm-dd')
    return date_col

    
    