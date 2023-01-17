import pandas as pd
from pandas.core import frame as pd_DF

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




def get_match_path(src_key, dir_df, date_key): 
    # Estos Matchers se usan para la función GET_MATCH_PATH, y luego READ_SOURCE_TABLE. 
    matchers_keys = {
        'subledger'     : ["key == 'FZE02'"],  # FPSL
        'cloud-banking' : ["key == 'CC4B2'"],  # C4B
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
            
    
