import numpy as np
import pandas as pd
from epic_py.utilities import read_excel_table


def print_df(a_df, width=180): 
    options = ['display.max_rows', None, 
               'display.max_columns', None, 
               'display.width', width]
    with pd.option_context(*options):
        print(a_df)


def fiserv_data(a_df): 
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
        'chk_sign'  : lambda df: np.where(df['aux_sign'], 
            df['Field Name'].str.startswith(df['Field Name'].shift(1)), True), 
        'chk_len'   : lambda df: (df['From'] + df['Length'] == df['From'].shift(-1)) 
            | (np.arange(len(df)) == len(df)-1), 
        'chk_name'  : lambda df: ~df['name'].duplicated() 
            | df['aux_sign'] | df['aux_fill']}

    b_df = (a_df
        .rename(lambda a_str: a_str.strip(), axis=1)
        .assign(**mod_cols)
        .astype({'fmt_len': int}))
    return b_df 
    


specs_file = "refs/catalogs/Data Extracts.xlsx.lnk"

sheets_tables = [('DATPTX01', 'atpt'),  # , ('DAMNAC01', 'damnac') 
    ('DAMBS101', 'dambs'), ('DAMBS201', 'dambs2'), ('DAMBSC01', 'dambsc'),
    ('DAMNA001', 'damna')] 
table_types = ['detail', 'header', 'trailer']



the_cols = ['name', 'From', 'Length', 'Format', 'Field Name', 'Technical Mapping', 
        'fmt_type', 'fmt_len', 'aux_date', 'aux_sign', 'aux_fill']

a_sht, a_tbl, suffix = 'DATPTX01', 'atpt', 'detail'

for a_sht, a_tbl in sheets_tables: 
    for suffix in table_types: 
        print(f"### Saving table: {a_sht}_{suffix}:")

        pre_table = read_excel_table(specs_file, a_sht, f"{a_tbl}_{suffix}")
        mid_table = fiserv_data(pre_table)
        chk_sums  = len(mid_table) - mid_table.loc[:, mid_table.columns.str.startswith('chk')].sum()
        print(str(chk_sums))
        table_columns = mid_table.loc[:, the_cols]
        table_columns.to_feather(f"refs/catalogs/{a_tbl}_{suffix}.feather") 
    