from pydantic import BaseModel
from typing import List, Optional

class Fee(BaseModel): 
    AccountID   : str
    TypeCode    : Optional[str]
    PostingDate : Optional[str]
    ValueDate   : Optional[str]
    Amount      : Optional[float]
    Currency    : Optional[str]
    PaymentNote : Optional[str]

class FeeSetRequest(BaseModel): 
    ExternalID  : str
    ProcessDate : str
    Status      : Optional[str]
    FeeDetail   : List[Fee]
    
