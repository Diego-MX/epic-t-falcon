
from epic_py.utilities import read_excel_table 

atpt_spec_file = 'refs/catalogs/MonetaryTxnExtract.xlsx.lnk'
## has sheets/tables 'trailer', 'detail', 'header'  

atpt_cols = (read_excel_table(atpt_spec_file, 'DetailRecord', 'detail_record')
    .rename(lambda a_str: a_str.strip(), axis=1)
    .assign(**{'Field Name': lambda df: df['Field Name'].str.strip() }))

atpt_cols.to_feather("refs/catalogs/atpt_details.feather") 


header_atpt = (read_excel_table(atpt_spec_file, 'HeaderRecord', 'header_record')
    .rename(lambda a_str: a_str.strip(), axis=1)
    .assign(**{'Field Name': lambda df: df['Field Name'].str.strip()}))

header_atpt.to_feather("refs/catalogs/atpt_header.feather")


trailer_atpt = (read_excel_table(atpt_spec_file, 'TrailerRecord', 'trailer_record')
    .rename(lambda a_str: a_str.strip(), axis=1)
    .assign(**{'Field Name': lambda df: df['Field Name'].str.strip()}))

trailer_atpt.to_feather("refs/catalogs/atpt_trailer.feather")