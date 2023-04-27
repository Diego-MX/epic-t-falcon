import base64
from datetime import datetime as dt
from delta.tables import DeltaTable as Δ
from operator import attrgetter
from pathlib import Path
import pandas as pd
from pandas import Series as pd_S, DataFrame as pd_DF
from pyspark.dbutils import DBUtils
from pyspark.sql import (functions as F, types as T, 
    DataFrame as spk_DF, Column, Row)
from os import path
import re
from typing import Union


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


def upsert_delta(spark, new_tbl, base_path, how, 
                 on_cols:Union[list, dict]): 
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
    

## Modificado por otros desarrolladores.  

def get_staged_updates(dt, df):
    """
    """
    recent_registries = df.where(F.col("end_date").isNull())
    insert_registries = df.where(F.col("end_date").isNotNull())
    
    new_to_insert = (recent_registries.alias("updates")
        .join(dt.toDF().alias("satellite"), on="hash_id")
        .where("satellite.end_date is NULL AND updates.hash_diff <> satellite.hash_diff"))
    
    staged_updates = (new_to_insert
        .selectExpr("NULL as merge_key", "updates.*") 
        .unionByName(recent_registries
        .selectExpr("hash_id AS merge_key", "*")) 
        .unionByName(insert_registries
        .selectExpr("NULL as merge_key", "*"))) 
    return staged_updates
    
    
def merge_satellite_into_delta(df, dt):
    """
    """
    staged_updates = get_staged_updates(dt, df)
    
    (dt.alias("clients")
        .merge(staged_updates.alias("staged_updates"),
            condition="clients.hash_id = staged_updates.merge_key")
         .whenMatchedUpdate(
            condition="clients.end_date is NULL AND clients.hash_diff <> staged_updates.hash_diff", 
             # el end_date debería estar vacio solo para UN registro con su hash_id unico
            set={"end_date": "current_date()"})
         .whenNotMatchedInsertAll().execute())
    
    
def batch_recent_retrieval(df):
    """
    """
    windowSpec = (W.partitionBy("hash_id")
        .orderBy(F.desc("event_datetime")))
    retrievals = (df
        .withColumn("row_number", F.row_number().over(windowSpec))
        .withColumn("end_date", F.when(F.col("row_number") > 1, F.current_timestamp()))
        .drop("row_number"))
    return retrievals
    
    
def deduplicate_id_diff_rows(df):
    """
    """
    window_id_diff = (W.partitionBy("hash_id", "hash_diff")
        .orderBy(F.desc("event_datetime")))
    deduplicated = (df
        .withColumn("row_number", F.row_number().over(window_id_diff))
        .filter("row_number = 1")
        .drop("row_number"))
    return deduplicated


def retrieve_delta_table(spark, dt_path):
    """
    """
    retrieved = Δ.forPath(spark, f'dbfs:/user/hive/warehouse/{dt_path}')
    return retrieved


def add_hashdiff(df):
    """
    creates a new column which is the computation of the hash code of all the dataframe columns with exception of
    column names included in the exclude_cols variable
    """
    def create_hashdiff(row, 
            exclude_cols=["hash_id", "load_date", "end_date", 'event_datetime', 'created_datetime']):
        row_dict = row.asDict()                                                  
        concat_str = ''
        allowed_cols = sorted(set(row_dict.keys()) - set(exclude_cols))
        for v in allowed_cols:
            concat_str = concat_str + '||' + str(row_dict[v])         # concatenate values
        concat_str = concat_str[2:] 
        row_dict["hash_raw"] = concat_str                 # preserve concatenated value for testing (this can be removed later)
        row_dict["hash_diff"] = hashlib.sha256(concat_str.encode("utf-8")).hexdigest()
        return Row(**row_dict)

    _schema = df.alias("df2").schema # get a cloned schema from dataframe
    (_schema
        .add("hash_raw", T.StringType(), False)
        .add("hash_diff", T.StringType(), False)) # modified inplace
    the_hashdiff = df.rdd.map(create_hashdiff).toDF(_schema)
    return the_hashdiff


def concat_column_values(df, 
    exclude_cols=["hash_id", "load_date", "end_date", 'event_datetime', 'created_datetime']):
    prefix = "casted"
    original_columns = df.columns
    set_original_columns = set(original_columns)
    set_exclude_cols = set(exclude_cols)
    allowed_cols = sorted(set_original_columns - set_exclude_cols)
    prefix_allowed_cols = [f"{prefix}_{c}" for c in allowed_cols]
    print(f"Using columns: {allowed_cols}")
    
    intermediate_df = (df.select(df.columns 
        + [F.col(c).cast("string").alias(f"{prefix}_{c}") for c in allowed_cols])
        .fillna("", subset=prefix_allowed_cols))
    
    subset_df = (intermediate_df
        .withColumn("hash_raw", F.concat_ws("||", *prefix_allowed_cols))
        .withColumn("hash_diff", F.sha2(F.concat_ws("||", *prefix_allowed_cols), 256))
        .drop(*prefix_allowed_cols))
    return subset_df


def compute_hashdiff(df, separator='|', offset=8, num_bits=256, take_hash_id=False):
    """
    """
    og_cols = df.columns[offset:]
    
    if not take_hash_id:
        out = df.withColumn("hash_diff", F.sha2(F.concat_ws(separator, *og_cols), num_bits))
    else:
        og_cols = ['hash_id'] + og_cols
        out = df.withColumn("hash_diff", F.sha2(F.concat_ws(separator, *og_cols), num_bits))
    #print(og_cols)
    return out
    