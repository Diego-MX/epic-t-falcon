import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
from pyspark.sql import functions as F, types as T



def colsdf_2_pyspark(pre_df: DataFrame, base_col='value'): 
    # ['From', 'Length', 'Field Name', 'Format', 'Description',
    #    'Technical Mapping', 'name', 'fmt_type', 'fmt_len', 'aux_date',
    #    'aux_sign', 'chk_len', 'chk_sign', 'chk_name'] 

    int_cols = ['From', 'Length', 'fmt_len']

    b_df = (pre_df
        .astype({an_int: int for an_int in int_cols})
        .assign(
            sgn_name = lambda df: df['name'].shift(1), 
            c_type = lambda df: np.where(df['aux_fill'], 0, 
                            np.where(df['aux_sign'], 1, 
                          np.where(df['aux_sign'].shift(1).isin([True]), 2, 
                        np.where(df['aux_date'], 3, 
                      np.where(df['fmt_type'] == 'X', 4, 
                    np.where(df['fmt_type'] == '9', 5, 
                  -1)))))))
        .set_index('name'))
    
    len_slct = [
        F.substring(F.col(base_col), a_row['From'], a_row['Length']).alias(name)
        for name, a_row in b_df.iterrows() if a_row['c_type'] > 0]
    
    cols_sgns  = [(F.col(name)*F.concat(F.col(f'{name}_sgn'), F.lit('1'))
                    .cast(T.IntegerType())).alias(name)
        for name, a_row in b_df.iterrows() if a_row['c_type'] == 2]

    cols_dates = [ F.to_date(F.col(name), 'yyyyMMdd').alias(name)
        for name, a_row in b_df.iterrows() if a_row['c_type'] == 3]

    cols_str   = [ F.trim(F.col(name)).alias(name) 
        for name, a_row in b_df.iterrows() if a_row['c_type'] == 4]

    cols_ints  = [ F.col(name).cast(T.IntegerType()).alias(name) 
        for name, a_row in b_df.iterrows() if a_row['c_type'] == 5]

    cols_sort  = [ F.col(name) 
        for name, a_row in b_df.iterrows() if a_row['c_type'] in [2, 3, 4, 5]]
    
    to_select = {
        '1-substring': len_slct, 
        '2-typecols' : cols_str + cols_ints + cols_dates + cols_sgns, 
        '3-sorted'   : cols_sort}
    return to_select



def len_cols(cols_df: DataFrame) -> int: 
    last_fill = cols_df['aux_fill'].iloc[-1]
    up_to = -1 if last_fill else len(cols_df)
    the_len = cols_df['Length'][:up_to].sum()
    return int(the_len)
    
