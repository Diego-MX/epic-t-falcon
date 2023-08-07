import numpy as np
from pandas import DataFrame as pd_DF, Series as pd_S
from pyspark.sql import functions as F, types as T, DataFrame as spk_DF
import re
from typing import Union

from epic_py.delta import EpicDF, file_exists


from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


def colsdf_prepare(a_df: pd_DF) -> pd_DF: 
    int_cols = ['From', 'Length', 'fmt_len']
    b_df = (a_df
        .astype({an_int: int for an_int in int_cols})
        .assign(
            sgn_name = lambda df: df['name'].shift(1),
            len_8   = lambda df: (df['Length'] < 8).isin([True]), 
            c_type = lambda df: np.where(df['aux_fill'].isin([True]), 0, 
                              np.where(df['aux_sign'].isin([True]), 1, 
                            np.where(df['aux_sign'].shift(1).isin([True]), 2, 
                          np.where(df['aux_date'].isin([True]), 3, 
                        np.where((df['fmt_type'] == 'X').isin([True]), 4, 
                      np.where(((df['fmt_type'] == '9') & df['len_8']).isin([True]), 5,
                    np.where(((df['fmt_type'] == '9') & (~df['len_8'])).isin([True]), 6,
                  -1))))))))
        .set_index('name'))
    return b_df
        
    
def colsdf_2_select(b_df: pd_DF, base_col='value'): 
    # F_NAUGHT as callable placeholder in list. 
    naught  = lambda name: T.NullType()
    sgn_dbl = lambda name: (F.col(name).cast(T.DoubleType()) 
        * F.concat(F.col(f'{name}_sgn'), F.lit('1')).cast(T.DoubleType())).alias(name)
    f_date  = lambda name: F.to_date(F.col(name), 'yyyyMMdd').alias(name)
    f_str   = lambda name: F.col(name).alias(name)
    f_int   = lambda name: F.col(name).cast(T.IntegerType()).alias(name)
    f_long  = lambda name: F.col(name).cast(T.LongType()).alias(name)
    
    # Corresponding to C_TYPE = ... 2, 3, 4, 5, 6
    type_funcs = [naught, naught, sgn_dbl, f_date, f_str, f_int, f_long]
    
    def row_to_select(a_row): 
        col_1 = F.trim(F.substring(F.col(base_col), a_row['From'], a_row['Length']))
        w_enc = a_row['Enc_Fix']
        col_2 = F.decode(F.encode(col_1, 'iso-8859-1'), w_enc) if w_enc else col_1
        return col_2
        
    len_slct = [row_to_select(a_row).alias(name)
        for name, a_row in b_df.iterrows() if a_row['c_type'] > 0]
    
    type_slct = [ type_funcs[a_row['c_type']](name)
        for name, a_row in b_df.iterrows() if a_row['c_type'] in [2, 3, 4, 5]]
 
    to_select = {
        '1-substring': len_slct, 
        '2-typecols' : type_slct}
    return to_select

    

def colsdf_2_schema(b_df: pd_DF) -> T.StructType: 
    # NAUGHT_TYPE placeholder callable to keep other indices in place.  
    
    naught_type = lambda x: None
    set_types = [naught_type, naught_type, 
        T.DoubleType, T.DateType, T.StringType, T.IntegerType]
    
    the_fields = [T.StructField(name, set_types[a_row['c_type']](), True)
        for name, a_row in b_df.iterrows() if a_row['c_type'] in [2, 3, 4, 5]]
    
    the_schema = T.StructType(the_fields)
    return the_schema




def join_with_suffix(a_df:EpicDF, b_df:EpicDF, on_cols, how, suffix): 
    non_on = (set(a_df.columns)
        .intersection(b_df.columns)
        .difference(on_cols))
    a_rnms = {a_col+suffix[0]: a_col for a_col in non_on}
    b_rnms = {b_col+suffix[1]: b_col for b_col in non_on}
    
    a_rnmd = a_df.with_column_plus(a_rnms)
    b_rnmd = b_df.with_column_plus(b_rnms)
    
    joiner = a_rnmd.join(b_rnmd, on=on_cols, how=how)
    return joiner
    


def write_datalake(a_df: Union[spk_DF, pd_DF, pd_S], 
        a_path, container=None, overwrite=False, spark=None): 
    
    if isinstance(a_df, spk_DF):
        dbutils = DBUtils(spark)
        if file_exists(a_path): 
            dbutils.fs.rm(a_path)
        
        pre_path = re.sub(r'\.csv$', '', a_path)
        (a_df.coalesce(1).write
            .format('csv')
            .save(pre_path))
        
        path_000 = [ff.path for ff in dbutils.fs.ls(pre_path) 
                if re.match(r'.*000\.csv$', ff.name)][0]
        dbutils.fs.cp(path_000, a_path)
        dbutils.fs.rm(pre_path, recurse=True)

    elif isinstance(a_df, (pd_DF, pd_S)):
        if container is None: 
            raise "Valid Container is required"
            
        the_blob = container.get_blob_client(a_path)
        to_index = {pd_DF: False, pd_S:True}
        
        str_df = a_df.to_csv(index=to_index[type(a_df)], encoding='utf-8')
        the_blob.upload_blob(str_df, encoding='utf-8', overwrite=overwrite)
    

