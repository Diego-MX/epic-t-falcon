import numpy as np
import pandas as pd
from epic_py.utilities import read_excel_table



def fiserv_data(a_df: pd.DataFrame): 
    # Field Name, Format, Technical Mapping
    mod_cols = {
        'Field Name': lambda df: df['Field Name'].str.strip(), 
        'Format'    : lambda df: df['Format'].astype(str), 
        'fmt_type'  : lambda df: df['Format'].str.extract(r'^([X9])')[0], 
        'fmt_len'   : lambda df: df['Format'].str.extract(r'.*\((\d+)\)')[0].fillna(1), 
        'aux_fill'  : lambda df: df['Field Name'].str.match(r'.*filler', case=False), 
        'aux_date'  : lambda df: df['Technical Mapping'].str.find('YYYYMMDD') > -1, 
        'aux_sign'  : lambda df: df['Technical Mapping'].str.find('+ or -'  ) > -1, 
        'name_1'    : lambda df: df['Technical Mapping']
            .str.strip().str.split(' ').str[0].str.lower()
            .str.replace(r'\((\d+)\)', r'_\1', regex=True)
            .str.replace('-', '_'), 
        'name'      : lambda df: np.where(~df['aux_sign'], df['name_1'], 
            df['name_1'].shift(-1) + '_sgn'),
        'chk_sign'  : lambda df: np.where(~df['aux_sign'], True, 
            df['Field Name'].str.replace(' - sign', '') == df['Field Name'].shift(-1)), 
        'chk_len'   : lambda df: (df['From'] + df['Length'] == df['From'].shift(-1)) 
            | (np.arange(len(df)) == len(df)-1), 
        'chk_name'  : lambda df: ~df['name'].duplicated() 
            | df['aux_sign'] | df['aux_fill']}

    b_df = (a_df
        .rename(lambda a_str: a_str.strip(), axis=1)
        .assign(**mod_cols)
        .astype({'fmt_len': int}))
    return b_df 
    


if __name__ == '__main__': 

    from config import (ConfigEnviron, 
        ENV, SERVER, DATALAKE_PATHS as paths)
    from datetime import datetime as dt
    from pathlib import Path
    app_environ = ConfigEnviron(ENV, SERVER)
    
    specs_brz = Path(paths['from-cms'])/"layouts"

    specs_file = "refs/layouts/Data Extracts.xlsx.lnk"

    sheets_tables = [
        ('DATPTX01', 'atpt'),  # , ('DAMNAC01', 'damnac')      
        ('DAMBS101', 'dambs'), 
        ('DAMBS201', 'dambs2'), 
        ('DAMBSC01', 'dambsc'),
        ('DAMNA001', 'damna')] 

    table_types = ['detail', 'header', 'trailer']

    the_cols = ['name', 'From', 'Length', 'Format', 'Field Name', 'Technical Mapping', 
            'fmt_type', 'fmt_len', 'aux_date', 'aux_sign', 'aux_fill']

    now_str = dt.now().strftime("%Y-%m-%d_%H:%M")
    
    for a_sht, a_tbl in sheets_tables: 
        for suffix in table_types: 

            file_name = f"refs/layouts/cms/{a_tbl}_{suffix}.feather"
            b_name1 =  f"{specs_brz}/{a_tbl}_{suffix}_latest.feather"
            b_name2 = f"{specs_brz}/{a_tbl}_{suffix}_{now_str}.feather"

            pre_table = read_excel_table(specs_file, a_sht, f"{a_tbl}_{suffix}")
            mid_table = fiserv_data(pre_table)
            chk_sums  = len(mid_table) - mid_table.loc[:, mid_table.columns.str.startswith('chk')].sum()

            print(str(chk_sums))
            
            table_columns = mid_table.loc[:, the_cols]

            table_columns.to_feather(file_name) 
            app_environ.upload_storage_blob(file_name, b_name1, 'bronze', overwrite=True)
            app_environ.upload_storage_blob(file_name, b_name2, 'bronze', overwrite=True)