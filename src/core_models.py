from datetime import datetime 
from delta.tables import DeltaTable as Δ
import pandas as pd
from pydantic import BaseModel, Field
from pydantic.dataclasses import dataclass
from typing import List, Optional

from src.utilities import snake_2_camel



class Fee(BaseModel): 
    account_id     : str = Field(alias='AccountID')
    type_code      : str 
    posting_date   : datetime
    value_date     : datetime
    amount         : Optional[float] = None 
    currency       : str
    payment_note   : str
    txn_ref_number : str   # Este no es requisito de la API.   
    
    class Config:
        alias_generator = snake_2_camel
        json_encoders = {
            datetime: lambda dt: dt.strftime('%Y%m%d')}
        allow_population_by_field_name = True
    

class FeeSet(BaseModel): 
    external_id  : str=Field(alias='ExternalID')
    process_date : datetime
    status       : Optional[str]
    fee_detail   : List[Fee]
    
    def as_dataframe(self, w_status=None) -> pd.DataFrame: 
        if w_status is None: 
            w_status = self.status
        
        details = [{ f"fee_{kk}": vv for kk, vv in fee} 
                        for fee in self.fee_detail]
        feemass = {"feemass_external_id": self.external_id, 
                   "feemass_process_date": self.process_date, 
                   "feemass_status"     : w_status}
        details_df = (pd.DataFrame(details)
            .assign(**feemass))
        return details_df
    
    def __len__(self): 
        return len(self.FeeDetail)
    
    class Config:
        alias_generator = snake_2_camel
        json_encoders = {
            datetime: lambda dt: dt.strftime('%Y%m%d')}
        allow_population_by_field_name = True
    
    
    
        
        
