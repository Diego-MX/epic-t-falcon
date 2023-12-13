
from operator import attrgetter as σ

import pandas as pd
from pandas import DataFrame as pd_DF
from toolz import compose_left

from epic_py.tools import packed, partial2


file_formats = {
    'subledger'     : r'CONCILIA(?P<key>\w+)(?P<date>\d{8})\.txt', 
    'Cloud-Banking' : r'CONCILIA(?P<key>\w+)(?P<date>\d{8})\.txt', 
    'cloud-banking' : r'CONCILIA(?P<key>\w+)(?P<date>\d{8})\.txt', 
    'spei-ledger'   : r'RECOIF(?P<key>\d+)(?P<date>\d{8})H(?P<time>\d{6})\.TXT',  
    'spei-banking'  : r'SPEI_FILE_(?P<key1>\w+)_(?P<date>\d{8})_?(?P<key2>\w*)\.txt', 
    'spei2-banking' : r'SPEI_FILE_(?P<key1>\w+)_(?P<date>\d{8})_?(?P<key2>\w*)\.txt'}

date_formats = {
    'spei-ledger' : '%d%m%Y'}


def process_files(file_df: pd_DF, a_src) -> pd_DF: 
    date_fmt = date_formats.get(a_src, '%Y%m%d')
    date_fmtr = compose_left(σ('date'), # df: df.date > to_datetime(...)
        partial2(pd.to_datetime, ..., format=date_fmt), 
        σ('dt.date'))
    file_keys = (file_df['name'].str.extract(file_formats[a_src])
        .assign(date=date_fmtr, source=a_src))
    return pd.concat([file_df, file_keys], axis=1)


def files_matcher(files_df, match_dict): 
    nn, pp = len(files_df), len(match_dict)
    matcher = pd.Series(0, range(nn))
    for ii, kv in enumerate(match_dict.items()): 
        k_srs, v_val = files_df[kv[0]], kv[1]
        print(f"Compare types: {k_srs.dtype}, {type(v_val)}")
        matcher += (pp-ii) * (k_srs == v_val).astype(int)
    w_match = (files_df
        .assign(matcher=matcher)
        .sort_values(['matcher', *match_dict], ascending=False)
        .reset_index()
        .loc[:, ['matcher', *match_dict, 'modificationTime', 'name', 'size', 'path']])
    
    are_max = (matcher == max(matcher))
    one_match = (sum(are_max) == 1)
    full_match = (w_match.loc[0, 'matcher'] == sum(range(pp+1)))
    if full_match and one_match: 
        path = w_match.loc[0, 'path']
        status = 1
    elif one_match: 
        path = w_match.loc[0, 'path']
        status = 2
    else: 
        path = None
        status = 3
    return (w_match, path, status)


# Esta función utiliza las variables definidas en `1. Esquemas de archivos`: 
# tsv_options, schemas, renamers, read_cols, mod_cols, base_cols. 
# Aguas con esas definiciones.  

def get_match_path(dir_df, file_keys): 
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
    
    matches_0 = (dir_df['date'].dt.date == file_keys['date'])
    the_matchers = ['True'] + matchers_keys[file_keys['key']]
    
    fails = {0: 0, 2: 0}
    for ii, other_match in enumerate(the_matchers):     # pylint: disable=invalid-name 
        matches_1 = matches_0 & dir_df.eval(other_match)
        has_path  = condition_path(matches_1)
        if has_path[0] == 1: 
            return has_path[1]
        else: 
            fails[has_path[0]] += 1
            print(f"Trying matches {ii+1} of {len(the_matchers)}:  failing status {has_path[0]}.")
            continue     
    print(f"Can't find file for date {file_keys['date']}.")
    return None


def get_source_path(dir_df, file_keys): 
    λ_equal = packed("({} == '{}')".format)         # pylint: disable=consider-using-f-string
    q_str = ' & '.join(map(λ_equal, file_keys.items()))
    
    path_df = dir_df.query(q_str)
    if path_df.shape[0] != 1: 
        print(f"File keys match is not unique... (len: {path_df.shape[0]})")
        return None
    return path_df['path'].iloc[0]
    