"""
Application configuration for the database probe.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    path: str
    user: str = "sysdba"
    password: str = "masterkey"
    charset: str = "UTF8"


@dataclass
class WorkerConfig:
    """Worker thread configuration."""
    max_workers: int = 10
    intake_position: int = 1


@dataclass
class AppConfig:
    """Main application configuration."""
    database: DatabaseConfig
    workers: WorkerConfig = WorkerConfig()
    
    @classmethod
    def from_microsip_path(cls, db_path: str) -> 'AppConfig':
        """Create configuration using the microsip database path."""
        return cls(
            database=DatabaseConfig(path=db_path)
        )
