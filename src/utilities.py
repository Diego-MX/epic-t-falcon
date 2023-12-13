from delta.tables import DeltaTable as Δ
from pandas import DataFrame as pd_DF
from pyspark.sql import functions as F, Column
from typing import Union
        
def len_cols(cols_df: pd_DF) -> int: 
    # Ya ni me acuerdo para que sirve. 
    req_cols = set(['aux_fill', 'Length'])
    
    if not req_cols.issubset(cols_df.columns): 
        raise "COLS_DF is assumed to have attributes 'AUX_FILL', 'LENGTH'."
    
    last_fill = cols_df['aux_fill'].iloc[-1]
    up_to = -1 if last_fill else len(cols_df)
    the_len = cols_df['Length'][:up_to].sum()
    return int(the_len)

def get_date(a_col: Union[str, Column]) -> Column: 
    date_reg = r'20[\d\-]{8}'
    date_col = F.to_date(F.regexp_extract(a_col, date_reg, 0), 'y-M-d')
    # Los formateadores de Spark, son diferentes que Python. 
    return date_col
