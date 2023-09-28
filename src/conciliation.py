from collections import OrderedDict
from warnings import warn

import pandas as pd
from pandas import DataFrame as pd_DF
from pyspark.sql import functions as F, types as T
from epic_py.delta import EpicDF, when_plus

schema_types = {
    'int' : T.IntegerType, 'long' : T.LongType,   'ts'   : T.TimestampType, 
    'str' : T.StringType,  'dbl'  : T.DoubleType, 'date' : T.DateType, 
    'bool': T.BooleanType, 'float': T.FloatType,  'null' : T.NullType}    

class Sourcer(): 
    def __init__(self, path, **kwargs): 
        self.path = path
        self.name = kwargs.get('name' )
        self.alias = kwargs.get('alias')
        self.options = kwargs.get('options',{})
        self.mutate = kwargs.get('mutate', {})
        self.schema = kwargs.get('schema', {})
        self.match = kwargs.get('match',  {})
        
    def start_data(self, spark): 
        a_schema = self._prep_schema()
        df_0 = (spark.read
            .format('csv')
            .options(**self.options)
            .schema(a_schema)
            .load(self.path))
        return EpicDF(df_0)

    def setup_data(self, a_df):
        typer = {
            'str'  : lambda cc: F.trim(cc), 
            'int_2': lambda cc: F.trim(cc).cast('int')}
        prepare_cols = {kk: typer[vv](F.col(kk))
            for kk, vv in self.schema.items() if vv in typer}
        df_1 = (a_df
            .with_column_plus(prepare_cols)
            .with_column_plus(self.mutate))
        return df_1

    def join_diffs(self, d_df:EpicDF, a_df:EpicDF, **kwargs): 
        how = kwargs.get('how', 'left')
        on  = kwargs.get('on', self.match['by'])
        j_df = d_df.join(a_df, how, on)
        return j_df

    def _prep_schema(self, pre_schema={}): 
        pre_schema = pre_schema or self.schema
        typers = {kk: schema_types.get(vv, T.StringType) 
                for kk, vv in pre_schema.items()}
        fields = [T.StructField(kk, vv(), True)
                for kk, vv in typers.items()]
        return T.StructType(fields)

    
class Conciliator:
    def __init__(self, src_1, src_2, check): 
        nm_1 = getattr(src_1, 'name', 1)
        nm_2 = getattr(src_2, 'name', 2)
        m_1, m_2 = src_1.match, src_2.match

        self._names = (nm_1, nm_2)
        self.matches = {nm_1 : m_1, nm_2: m_2}
        self.data = {}
        self.check = check
        self.by = m_1['by']
        assert m_1['by'] == m_2['by'], "BY columns are not equal."
        
        
    def base_match(self, *dfs): 
        names = self._names
        for nm, df in zip(names, dfs): 
            self.data[nm] = df

        m_0, m_1 = self.matches[names[0]], self.matches[names[1]]
        j_0 = (dfs[0]
            .filter_plus(*m_0.get('where', []))
            .groupBy(m_0.get('by', []))
            .agg_plus(m_0.get('agg', {})))
        j_1 = (dfs[1]
            .filter_plus(*m_1.get('where', []))
            .groupBy(m_1.get('by', []))
            .agg_plus(m_1.get('agg', {})))
        the_join = (j_0
            .join(j_1, on=self.by, how='full')
            .withColumn('check_key', when_plus(self.check, reverse=True)))
        self.base_data = the_join
        return the_join
    
    def filter_checks(self, base_df: EpicDF=None, f_keys=[], join_alias=None): 
        base_df = base_df or self.base_data
        key_filter = self._keys_to_column(f_keys)
        j_df = self.data.get(join_alias)

        f_df = base_df.filter(key_filter)
        if join_alias: 
            f_df = f_df.join(j_df, on=self.by, how='left')
        return f_df 

    def _keys_to_column(self, str_keys): 
        if isinstance(str_keys, list): 
            f_col = F.col('check_key').isin(str_keys)
        elif isinstance(str_keys, str) and str_keys.startswith('~'):
            f_col = F.col('check_key') != str_keys[1:]            
        return f_col
    

# Nos apoyamos en estos formatos de archivos predefinidos. 
# Estas Regex funcionan para dataframes de pandas, mas no de Spark. 
file_formats = {
    'subledger'     : r'CONCILIA(?P<key>\w+)(?P<date>\d{8})\.txt', 
    'Cloud-Banking' : r'CONCILIA(?P<key>\w+)(?P<date>\d{8})\.txt', 
    'cloud-banking' : r'CONCILIA(?P<key>\w+)(?P<date>\d{8})\.txt', 
    'spei-ledger'   : r'RECOIF(?P<key>\d+)(?P<date>\d{8})H(?P<time>\d{6})\.TXT',  
    'spei-banking'  : r'SPEI_FILE_(?P<key1>\w+)_(?P<date>\d{8})_?(?P<key2>\w*)\.txt', 
    'spei2-banking' : r'SPEI_FILE_(?P<key1>\w+)_(?P<date>\d{8})_?(?P<key2>\w*)\.txt'}

date_formats = {
    'spei-ledger' : '%d%m%Y'}

def process_files(file_df: pd_DF, a_src, w_match=None) -> pd_DF: 
    date_fmtr = lambda df: pd.to_datetime(df['date'], 
                format=date_formats.get(a_src, '%Y%m%d'))
    file_keys = (file_df['name'].str.extract(file_formats[a_src])
        .assign(date=date_fmtr, source=a_src))
    return pd.concat([file_df, file_keys], axis=1)


def files_matcher(files_df, match_dict): 
    nn, pp = len(files_df), len(match_dict)
    matcher = pd.Series(0, range(nn))
    for ii, kv in enumerate(match_dict.items()): 
        matcher += (pp-ii) * (files_df[kv[0]] == kv[1]).astype(int)
    w_match = (files_df
        .assign(matcher=matcher)
        .sort_values(['matcher', *match_dict], ascending=False)
        .reset_index())
    
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
        print(f"Can't find file for date {file_keys['date']}.")
        return None


def get_source_path(dir_df, file_keys): 
    λ_equal = lambda k_v: "({} == '{}')".format(*k_v)
    q_str = ' & '.join(map(λ_equal, file_keys.items()))
    
    path_df = dir_df.query(q_str)
    if path_df.shape[0] != 1: 
        print(f"File keys match is not unique... (len: {path_df.shape[0]})")
        return None
    
    return path_df['path'].iloc[0]
    
