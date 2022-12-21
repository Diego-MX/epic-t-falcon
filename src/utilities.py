import base64
from datetime import datetime as dt
from operator import attrgetter
import pandas as pd
from pandas.core import frame as pd_DF
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


def dirfiles_df(a_dir, spark=None) -> pd_DF:
    if spark: 
        dbutils = DBUtils(spark)
        attrs = ['modificationTime', 'name', 'path', 'size']
        
        getattrs = attrgetter(*attrs)
        files_ls = [getattrs(filish) for filish in dbutils.fs.ls(a_dir)]
        files_df = (pd.DataFrame(files_ls, columns=attrs)
            .assign(**{'modificationTime': lambda df: 
                    pd.to_datetime(df['modificationTime'], unit='ms')}))
    else: 
        raise Exception('Must provide Spark Session.')
    
    return files_df
        
    
