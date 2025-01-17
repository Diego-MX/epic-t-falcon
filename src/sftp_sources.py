from datetime import date
from itertools import product

from delta import DeltaTable as Δ
import pandas as pd
from pandas import DataFrame as pd_DF
from pyspark.sql import (functions as F, DataFrame as spk_DF)

from epic_py.delta import EpicDF, path_type
from src import tools, utilities as utils


def update_sourcers(blobber, blob_dir, trgt_dir): 
    sources = []
    layouts = ['detail', 'header', 'trailer']
    for src, lyt in product(sources, layouts): 
        a_blob = f"{blob_dir}/{src}_{lyt}_latest.feather" 
        f_trgt = f"{trgt_dir}/{src}_{lyt}_latest.feather" 
        blobber.download_storage_blob(f_trgt, a_blob, 'bronze', None, True)
    print("Updating sources successful.")


meta_cols = [('_metadata.file_name', 'file_path'), 
             ('_metadata.file_modification_time', 'file_modified')]

def prepare_sourcer(b_key, layout_dir): 
    meta_cols = [('_metadata.file_name', 'file_path'), 
             ('_metadata.file_modification_time', 'file_modified')]
    
    temper_file  = f"{layout_dir}/{b_key}_{{}}_latest.feather" 
    dtls_file = f"{layout_dir}/{b_key}_detail_latest.feather"
    dtls_df = (pd.read_feather(dtls_file)
        .assign(name = lambda df: 
                df['name'].str.replace(')', '', regex=False)))
    hdrs_df = pd.read_feather(temper_file.format('header'))
    trlr_df = pd.read_feather(temper_file.format('trailer'))    
    up_to_len = max(utils.len_cols(hdrs_df), utils.len_cols(trlr_df))  

    prep_dtls     = tools.colsdf_prepare(dtls_df)
    the_selectors = tools.colsdf_2_select(prep_dtls, 'value')  
    # 1-substring, 2-typecols, 3-sorted
    the_selectors['long_rows'] = (F.length(F.rtrim(F.col('value'))) > up_to_len)
    return the_selectors
    
    
def read_delta_basic(spark, src_path, tgt_path=None) -> spk_DF: 
    meta_cols = [('_metadata.file_name', 'file_name'), 
        ('_metadata.file_modification_time', 'file_modified')]
    
    src_fs = path_type(src_path)
    
    if ((src_fs == 'Folder') 
            and (tgt_path is not None) 
            and Δ.isDeltaTable(spark, tgt_path)): 
        since_mod = (spark.read.format('delta')
            .load(tgt_path)
            .select(F.max('file_modified'))
            .collect()[0][0])
        some_options = {
            'recursiveFileLookup': 'true', 
            'header': 'false',
            'modifiedAfter': since_mod}
    elif src_fs == 'Folder':
        some_options = {
            'recursiveFileLookup': 'true', 
            'header': 'false',
            'modifiedAfter': date(2020, 1, 1)}
    else: 
        some_options = {
            'recursiveFileLookup': 'false', 
            'header': 'true',
            'modifiedAfter': date(2020, 1, 1)}
    mod_after = some_options.pop('modifiedAfter')
    basic = (EpicDF(spark.read.format('text')
        .options(**some_options)
        .load(src_path, modifiedAfter=mod_after))
        .select('value', *[F.col(a_col[0]).alias(a_col[1]) 
                for a_col in meta_cols])
        .withColumn('value', F.decode('value', 'iso-8859-1')))
    return basic


def delta_with_sourcer(pre_Δ: spk_DF, sourcer, only_substring=False) -> spk_DF: 
    meta_keys = ['file_name', 'file_date', 'file_modified']
    delta_1 = (pre_Δ
        .withColumn('file_date', utils.get_date('file_name'))
        .filter(sourcer['long_rows'])
        .select(*sourcer['1-substring'], *meta_keys))
    if not only_substring: 
        delta_2 = (delta_1
            .select(*sourcer['2-typecols'], *meta_keys))
    else: 
        delta_2 = delta_1
    return delta_2
    

        
    
