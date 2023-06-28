from datetime import date
from itertools import product
import pandas as pd
from pandas import DataFrame as pd_DF
from pyspark.sql import (functions as F, DataFrame as spk_DF)

from src import tools, utilities as utils
### PROCESS_FILES
### READ_SOURCE_TABLE


# Nos apoyamos en estos formatos de archivos predefinidos. 
# Estas Regex funcionan para dataframes de pandas, mas no de Spark. 
file_formats = {
    'subledger'     : r'CONCILIA(?P<key>\w+)(?P<date>\d{8})\.txt', 
    'Cloud-Banking' : r'CONCILIA(?P<key>\w+)(?P<date>\d{8})\.txt', 
    'cloud-banking' : r'CONCILIA(?P<key>\w+)(?P<date>\d{8})\.txt', 
    'spei-ledger'   : r'RECOIF(?P<key>\d+)(?P<date>\d{8})H(?P<time>\d{6})\.TXT',  
    'spei-banking'  : r'SPEI_FILE_(?P<key1>\w+)_(?P<date>\d{8})_?(?P<key2>\w*)\.txt', 
    'spei2-banking' : r'SPEI_FILE_(?P<key1>\w+)_(?P<date>\d{8})_?(?P<key2>\w*)\.txt',
}

date_formats = {
    'spei-ledger' : '%d%m%Y'}

    
def process_files(file_df: pd_DF, a_src) -> pd_DF: 
    
    date_fmtr = lambda df: pd.to_datetime(df['date'], 
                format=date_formats.get(a_src, '%Y%m%d'))
    
    file_keys = (file_df['name'].str.extract(file_formats[a_src])
        .assign(date=date_fmtr, source=a_src))
    mod_df = pd.concat([file_df, file_keys], axis=1)
    return mod_df


# Esta función utiliza las variables definidas en `1. Esquemas de archivos`: 
# tsv_options, schemas, renamers, read_cols, mod_cols, base_cols. 
# Aguas con esas definiciones.  

def get_match_path(dir_df, file_keys): 
    # Estos Matchers se usan para la función GET_MATCH_PATH, y luego READ_SOURCE_TABLE. 
    matchers_keys = {
        'subledger'     : ["key == 'FZE02'", "key == 'FZE03'"],  # FPSL
        'cloud-banking' : ["key == 'CC4B2'", "key == 'CCB15'"],  # C4B
        'spei-ledger'   : [], 
        'spei-banking'  : []}
    
    def condition_path(lgls): 
        # (estatus, respuesta-ish)
        if   lgls.sum() == 1: 
            return (1, dir_df.loc[lgls]['path'].values[0])
        elif lgls.sum() == 0: 
            return (0, "There aren't matches")
        elif lgls.sum() > 1: 
            return (2, "There are many matches")
    
    matches_0 = (dir_df['date'].dt.date == date_key)
    the_matchers = ['True'] + matchers_keys[src_key]
    
    fails = {0: 0, 2: 0}
    for ii, other_match in enumerate(the_matchers): 
        matches_1 = matches_0 & dir_df.eval(other_match)
        has_path  = condition_path(matches_1)
        if has_path[0] == 1: 
            return has_path[1]
        else: 
            fails[has_path[0]] += 1
            print(f"Trying matches {ii+1} of {len(the_matchers)}:  failing status {has_path[0]}.")
            continue
    else: 
        print(f"Can't find file for date {date_key}.")
        return None

    
def update_sourcers(blobber, blob_dir, trgt_dir): 
    sources = ['damna', 'atpt', 'dambs', 'dambs2', 'dambsc']
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
    # Siempre es 56 = max(47, 56)

    prep_dtls     = tools.colsdf_prepare(dtls_df)
    the_selectors = tools.colsdf_2_select(prep_dtls, 'value')  
    # 1-substring, 2-typecols, 3-sorted

    the_selectors['long_rows'] = (F.length(F.rtrim(F.col('value'))) > up_to_len)
    return the_selectors
    
    
def read_delta_basic(spark, src_path, tgt_path=None, **kwargs) -> spk_DF: 
    meta_cols = [('_metadata.file_name', 'file_name'), 
             ('_metadata.file_modification_time', 'file_modified')]
    
    src_fs = utils.path_type(src_path, spark)[1]
    
    if ((src_fs == 'Folder') 
            and (tgt_path is not None) 
            and Δ.isDeltaTable(spark, tgt_path)): 
        optn_lookup = 'true'
        optn_hdr    = 'false'
        optn_mod = (spark.read.format('delta')
            .load(tgt_path)
            .select(F.max('file_modified'))
            .collect()[0][0])
        
    elif src_fs == 'Folder':
        optn_lookup = 'true'
        optn_mod    = date(2020, 1, 1)
        optn_hdr    = 'false'
    else: 
        optn_lookup = 'false'
        optn_hdr    = 'true'
        optn_mod    = date(2020, 1, 1)
    
    pre_delta = (spark.read.format('text')
        .option('recursiveFileLookup', optn_lookup)
        .option('header', optn_hdr)
        .load(src_path, modifiedAfter=optn_mod)
        .select('value', *[F.col(a_col[0]).alias(a_col[1]) 
                for a_col in meta_cols])
        .withColumn('value', F.decode('value', 'iso-8859-1')))
    
    return pre_delta


def delta_with_sourcer(pre_Δ: spk_DF, sourcer, only_substring=False) -> spk_DF: 
    meta_keys = ['file_name', 'file_date', 'file_modified']
    # Sourcer has LONG_ROWS, 1-SUBSTRING, 2-TYPECOLS
    
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
    

        
    
