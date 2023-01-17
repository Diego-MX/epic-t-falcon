from functools import reduce
import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame as pd_DF
from pyspark.sql import functions as F, types as T
from pyspark.sql.column import Column as spk_Col
from pyspark.sql.dataframe import DataFrame as spk_DF
from typing import List, Dict


def colsdf_prepare(a_df: pd_DF) -> pd_DF: 
    int_cols = ['From', 'Length', 'fmt_len']
    b_df = (a_df
        .astype({an_int: int for an_int in int_cols})
        .assign(
            sgn_name = lambda df: df['name'].shift(1), 
            c_type = lambda df: np.where(df['aux_fill'].isin([True]), 0, 
                              np.where(df['aux_sign'].isin([True]), 1, 
                            np.where(df['aux_sign'].shift(1).isin([True]), 2, 
                          np.where(df['aux_date'].isin([True]), 3, 
                        np.where((df['fmt_type'] == 'X').isin([True]), 4, 
                      np.where((df['fmt_type'] == '9').isin([True]), 5, 
                    -1)))))))
        .set_index('name'))
    return b_df
        
    
def colsdf_2_select(b_df: pd_DF, base_col='value'): 
    # F_NAUGHT as callable placeholder in list. 
    f_naught  = lambda name: None
    f_sgn_dbl = lambda name: (F.col(name).cast(T.DoubleType()) 
            *F.concat(F.col(f'{name}_sgn'), F.lit('1')).cast(T.DoubleType())
            ).alias(name)
    f_date    = lambda name: F.to_date(F.col(name), 'yyyyMMdd').alias(name)
    f_str     = lambda name: F.trim(F.col(name)).alias(name)
    f_int     = lambda name: F.col(name).cast(T.IntegerType()).alias(name)
    
    # Corresponding to C_TYPE = ... 2, 3, 4, 5. 
    type_funcs = [f_naught, f_naught, f_sgn_dbl, f_date, f_str, f_int]
    
    len_slct = [
        F.substring(F.col(base_col), a_row['From'], a_row['Length']).alias(name)
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



def len_cols(cols_df: pd_DF) -> int: 
    last_fill = cols_df['aux_fill'].iloc[-1]
    up_to = -1 if last_fill else len(cols_df)
    the_len = cols_df['Length'][:up_to].sum()
    return int(the_len)



def with_columns(a_df: spk_DF, cols_dict: dict) -> spk_DF: 
    f_with = lambda x_df, col_item: x_df.withColumn(col_item[0], col_item[1])
    
    non_cols = {name for name, col in cols_dict.items() 
            if not isinstance(col, spk_Col)}
    if non_cols: 
        raise Exception(f"Non Columns: {non_cols}")
        
    b_df = reduce(f_with, cols_dict.items(), a_df)
    return b_df


def join_with_suffix(a_df, b_df, on_cols, how, suffix): 
    non_on = set(a_df.columns).intersection(b_df.columns).difference(on_cols)
    a_rnmr = {a_col+suffix[0]: a_col for a_col in non_on}
    b_rnmr = {b_col+suffix[1]: b_col for b_col in non_on}
    
    a_rnmd = with_columns(a_df, a_rnms)
    b_rnmd = with_columns(b_df, b_rnms)
    
    joiner = a_rnmd.join(b_rnmd, on=on_cols, how=how)
    return joiner
    
    
    


    
