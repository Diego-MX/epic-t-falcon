
from epic_py.utilities import read_excel_table 

atpt_spec_file = 'refs/catalogs/MonetaryTxnExtract.xlsx.lnk'
## has sheets/tables 'trailer', 'detail', 'header'  

atpt_cols = read_excel_table(atpt_spec_file, 'DetailRecord', 'detail_record')
atpt_cols.to_feather("refs/catalogs/atpt_cols.feather") 

header_atpt = read_excel_table(atpt_spec_file, 'HeaderRecord', 'header_record')
header_atpt.to_feather("refs/catalogs/atpt_header.feather")