"""Main models are SOURCER, CONCILIATOR"""
from operator import methodcaller as ϱ
from pyspark.sql import functions as F, types as T
from toolz import compose_left

from epic_py.delta import EpicDF, when_plus

# pylint: disable=dangerous-default-value
# pylint: disable=invalid-name
# pylint: disable=missing-class-docstring

schema_types = {
    'int' : T.IntegerType, 'long' : T.LongType,   'ts'   : T.TimestampType, 
    'str' : T.StringType,  'dbl'  : T.DoubleType, 'date' : T.DateType, 
    'bool': T.BooleanType, 'float': T.FloatType,  'null' : T.NullType}    

class Sourcer(): 
    def __init__(self, path, **kwargs): 
        self.path = path
        self.name = kwargs.get('name')
        self.alias = kwargs.get('alias')
        self.match = kwargs.get('match', {})
        self.mutate = kwargs.get('mutate', {})
        self.schema = kwargs.get('schema', {})
        self.options = kwargs.get('options', {})
        
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
            'str'  : F.trim, 
            'int_2': compose_left(F.trim, ϱ('cast', 'int'))}
        prepare_cols = {kk: typer[vv](F.col(kk))
            for kk, vv in self.schema.items() if vv in typer}
        df_1 = (a_df
            .with_column_plus(prepare_cols)
            .with_column_plus(self.mutate))
        return df_1

    def join_diffs(self, d_df:EpicDF, a_df:EpicDF, **kwargs): 
        how = kwargs.get('how', 'left')
        on  = kwargs.get('on', self.match['by'])    # pylint: disable=invalid-name
        j_df = d_df.join(a_df, how, on)
        return j_df

    def _prep_schema(self, pre_schema={}): 
        pre_schema = pre_schema or self.schema
        typers = {kk: schema_types.get(vv, T.StringType) 
                for kk, vv in pre_schema.items()}
        fields = [T.StructField(kk, vv(), True)
                for kk, vv in typers.items()]
        return T.StructType(fields)

    
class Conciliator:      # pylint: disable=missing-class-docstring
    def __init__(self, src_1, src_2, check): 
        nm_1 = getattr(src_1, 'name', 1)
        nm_2 = getattr(src_2, 'name', 2)
        m_1, m_2 = src_1.match, src_2.match

        self._names = (nm_1, nm_2)
        self.matches = {nm_1 : m_1, nm_2: m_2}
        self.data = {}
        self.check = check
        self.by = m_1['by']     # pylint: disable=invalid-name
        self.base_data = None
        assert m_1['by'] == m_2['by'], "BY columns are not equal."
        
        
    def base_match(self, *dfs): 
        names = self._names
        for nm, df in zip(names, dfs):      # pylint: disable=invalid-name 
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
    