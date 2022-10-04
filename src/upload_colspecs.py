import numpy as np
import pandas as pd
from re import IGNORECASE
from epic_py.utilities import read_excel_table


def print_df(a_df, width=180): 
    options = ['display.max_rows', None, 
               'display.max_columns', None, 
               'display.width', width]
    with pd.option_context(*options):
        print(a_df)



specs_file = 'refs/catalogs/Data Extracts.xlsx.lnk'

sheets_tables = [('DATPTX', 'atpt'),  # , ('DAMNAC01', 'damnac') 
    ('DAMBS201', 'dambs'), ('DAMBSC01', 'dambsc'),
    ('DAMNA001', 'damna')] 
table_types = ['detail', 'header', 'trailer']



fmt_rgx = r'([X9])\((\d+)\)'
mod_cols = {
    'Field Name': lambda df: df['Field Name'].str.strip(), 
    'Format'    : lambda df: df['Format'].astype(str), 
    'fmt_type'  : lambda df: df['Format'].str.extract(fmt_rgx)[0], 
    'fmt_len'   : lambda df: df['Format'].str.extract(fmt_rgx)[1], 
    'aux_date'  : lambda df: df['Technical Mapping'].str.find('YYYYMMDD') > -1, 
    'aux_sign'  : lambda df: df['Technical Mapping'].str.find('+ or -'  ) > -1, 
    'name_1'    : lambda df: df['Technical Mapping']
            .str.strip().str.split(' ').str[0].str.lower().replace('-', '_'), 
    'name'      : lambda df: np.where(~df['aux_sign'], df['name_1'], 
            df['name_1'].shift(1) + '_sgn'),
    'chk_len'   : lambda df:(df['From'] + df['Length'] == df['From'].shift(-1)) 
            | (np.arange(len(df)) == len(df) - 1), 
    'chk_sign'  :(lambda df: np.where(df['aux_sign'], 
            df['Field Name'].str.replace(' - sign', '', regex=False) == df['Field Name'].str.strip().shift(-1), True)), 
    'chk_name'  :(lambda df:~df['name'].duplicated() 
            | df['aux_sign']
            | df['Field Name'].str.match(r'.*filler', case=False))
}

a_sht, a_tbl, suffix = 'DAMNA001', 'damna', 'detail'
the_cols = ['name', 'From', 'Length', 'Field Name', 'Technical Mapping', 
    'fmt_type', 'fmt_len', 'aux_date', 'aux_sign']
for a_sht, a_tbl in sheets_tables: 
    for suffix in table_types: 
        table_columns = (read_excel_table(specs_file, a_sht, f"{a_tbl}_{suffix}")
            .rename(lambda a_str: a_str.strip(), axis=1)
            .assign(**mod_cols)
            .loc[:, the_cols])
        table_columns.to_feather (f"refs/catalogs/{a_tbl}_{suffix}.feather") 
    