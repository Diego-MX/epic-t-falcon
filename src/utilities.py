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


   
