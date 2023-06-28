from datetime import datetime
from pandas import DataFrame as pd_DF
from pydantic import BaseModel, validator
from typing import List, Optional

from src.utilities import snake_2_camel, MatchCase, partial2


camelize_alias = partial2(snake_2_camel, ..., 
    first_too=True, post_sub={'Id': 'ID'})

dt_parser = MatchCase(via=type, case_dict={
    str : partial2(datetime.strptime, ..., "%Y%m%d"), 
    None: lambda to_dt: to_dt})


class Fee(BaseModel): 
    class Config:
        alias_generator = camelize_alias
        allow_population_by_field_name = True
        json_encoders = {
            datetime: lambda dt: dt.strftime('%Y%m%d')}

    account_id     : str
    type_code      : str 
    posting_date   : datetime 
    value_date     : datetime
    amount         : Optional[float] = None 
    currency       : str
    payment_note   : str
    # Estos fueron agregados después. 
    transaction_id : str
    pos_fee        : Optional[str] 
    status_process : Optional[int]
    status_descr   : Optional[str]
    log_msg        : Optional[str]
    external_id    : Optional[str]
    process_date   : Optional[datetime] 
    

    # Todos los fields con DATETIME les aplicamos este validator. 
    # No hemos encontrado cómo hacerlo generalizado. 
    @validator('posting_date', pre=True)
    def parse_posting_date(cls, v): 
        return dt_parser(v)

    @validator('value_date', pre=True)
    def parse_value_date(cls, v): 
        return dt_parser(v)

    @validator('process_date', pre=True)
    def parse_process_date(cls, v): 
        return dt_parser(v)


class FeeSet(BaseModel): 
    class Config:
        alias_generator = camelize_alias
        json_encoders = {
            datetime: lambda dt: dt.strftime('%Y%m%d')}
        allow_population_by_field_name = True

    external_id  : str
    process_date : datetime
    status       : Optional[str]
    fee_detail   : List[Fee]
    
    def __len__(self):  
        return len(self.FeeDetail)

    def as_dataframe(self) -> pd_DF: 
        feemass = {kk: vv for kk, vv in self 
                if kk != 'fee_detail' and vv is not None}
        details = [{kk: vv for kk, vv in fee if vv is not None} 
                for fee in self.fee_detail]

        details_df = (pd_DF(details)
            .assign(**feemass))
            
        return details_df
    
    @validator('process_date', pre=True)
    def parse_process_date(cls, v): 
        return dt_parser(v)

    
    