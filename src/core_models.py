from datetime import date
from math import ceil
from pydantic import BaseModel
from typing import List, Optional

class Fee(BaseModel): 
    AccountID   : str
    TypeCode    : str
    PostingDate : str
    ValueDate   : str
    Amount      : Optional[float]
    Currency    : str
    PaymentNote : str

class FeeSet(BaseModel): 
    ExternalID  : str
    ProcessDate : str
    Status      : Optional[str]
    FeeDetail   : List[Fee]
    
    def __len__(self): 
        return len(self.FeeDetail)
    
    
