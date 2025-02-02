from dataclasses import dataclass
from datetime import datetime

@dataclass
class Change:
    log_id: int
    pk_val: int
    table_id: int
    mutation: str
    occured_at: datetime
    processed: bool
