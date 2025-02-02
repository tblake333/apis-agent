from dataclasses import dataclass
from datetime import datetime

@dataclass
class ConnectionInfo:
    db_path: str
    db_user: str
    db_password: str
