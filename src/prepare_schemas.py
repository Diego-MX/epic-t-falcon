
from epic_py.utilities import read_excel_table 

atpt_spec_file = 'refs/catalogs/MonetaryTxnExtract.xlsx.lnk'
## has sheets/tables 'trailer', 'detail', 'header'  

atpt_cols = read_excel_table(atpt_spec_file, 'DetailRecord', 'detail_record')
 
