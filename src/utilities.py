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

    

def write_datalake(a_df: Union[spk_DF, pd_DF, series.Series], 
        a_path, container=None, overwrite=False, spark=None): 
    
    if isinstance(a_df, spk_DF):
        dbutils = DBUtils(spark)
        if file_exists(a_path): 
            dbutils.fs.rm(a_path)
        
        pre_path = re.sub(r'\.csv$', '', a_path)
        a_df.coalesce(1).write.format('csv').save(pre_path)
        
        path_000 = [ff.path for ff in dbutils.fs.ls(pre_path) 
                if re.match(r'.*000\.csv$', ff.name)][0]
        dbutils.fs.cp(path_000, a_path)
        dbutils.fs.rm(pre_path, recurse=True)

    elif isinstance(a_df, (pd_DF, series.Series)):
        if container is None: 
            raise "Valid Container is required"
            
        the_blob = container.get_blob_client(a_path)
        to_index = {pd_DF: False, series.Series: True}
        
        str_df = a_df.to_csv(index=to_index[type(a_df)], encoding='utf-8')
        the_blob.upload_blob(str_df, encoding='utf-8', overwrite=overwrite)
    
