from collections import OrderedDict
from epic_py.delta import EpicDF, when_plus
from pyspark.sql import functions as F, types as T
from toolz import dicttoolz as d_toolz


schema_types = {
    'int' : T.IntegerType, 'long' : T.LongType,   'ts'   : T.TimestampType, 
    'str' : T.StringType,  'dbl'  : T.DoubleType, 'date' : T.DateType, 
    'bool': T.BooleanType, 'float': T.FloatType,  'null' : T.NullType}
    

class Sourcer(): 
    def __init__(self, path, **kwargs): 
        self.path = path
        self.name  = kwargs.get('name', {})
        self.alias = kwargs.get('alias', {})
        self.options = kwargs.get('options', {})
        self.mutate  = kwargs.get('mutate', {})
        self.schema  = kwargs.get('schema', {})
        self.match   = kwargs.get('match', {})
        
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

    def join_diffs(self, d_df, a_df, **kwargs): 
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

        assert m_1['by'] == m_2['by'], "BY columns are not equal."
        
        
    def base_match(self, *dfs): 
        names = self._names
        for nm, df in zip(names, dfs): 
            self.data[nm] = df

        m_1, m_2 = self.matches[names[0]], self.matches[names[1]]
        j_1 = (dfs[0]
            .filter_plus(m_1.get('where', []))
            .groupBy(m_1.get('by', []))
            .agg_plus(m_1.get('agg', {})))
        j_2 = (dfs[1]
            .filter_plus(m_1.get('where', []))
            .groupBy(m_1.get('by', []))
            .agg_plus(m_1.get('agg', {})))
        the_join = (j_1
            .join(j2, on=self.by, how='full')
            .withColumn('check_key', when_plus(self.check)))
        self.base_data = the_join
        return the_join
    
    def filter_checks(self, base_df:EpicDF=None, f_keys=[], j_alias=None): 
        base_df = base_df or self.base_data
        key_filter = self._keys_to_column(f_keys)
        j_df = self.get(j_alias)

        f_df = base_df.filter(key_filter)
        if j_alias: 
            f_df = f_df.join(j_df, 'left', self.by)
        return f_df 

    def _keys_to_column(self, str_keys): 
        if isinstance(str_keys, list): 
            f_col = F.col('check_key').isin(str_keys)
        elif isinstance(str_keys, str) and str_keys.startswith('~'):
            f_col = F.col('check_key') != str_keys[1:]            
        return f_col
    

def get_source_path(dir_df, file_keys): 
    q_str = ' & '.join(f"({k} == '{v}')" 
        for k, v in file_keys.items())
    
    path_df = dir_df.query(q_str)
    if path_df.shape[0] != 1: 
        
        print(f"File keys match is not unique.")
        return None
    
    return path_df['path'].iloc[0]
