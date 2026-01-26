"""
Application configuration for the database probe.

Supports configuration via:
- Environment variables (recommended for production)
- Programmatic configuration
- Default values for development
"""

import os
from dataclasses import dataclass, field
from typing import Optional


def _get_env(key: str, default: Optional[str] = None) -> Optional[str]:
    """Get environment variable value."""
    return os.environ.get(key, default)


def _get_env_bool(key: str, default: bool = False) -> bool:
    """Get environment variable as boolean."""
    value = os.environ.get(key, "").lower()
    if value in ("true", "1", "yes", "on"):
        return True
    if value in ("false", "0", "no", "off"):
        return False
    return default


def _get_env_int(key: str, default: int) -> int:
    """Get environment variable as integer."""
    value = os.environ.get(key)
    if value is not None:
        try:
            return int(value)
        except ValueError:
            pass
    return default


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    path: str
    user: str = "sysdba"
    password: str = "masterkey"
    charset: str = "UTF8"

    @classmethod
    def from_env(cls, db_path: Optional[str] = None) -> 'DatabaseConfig':
        """
        Create database config from environment variables.

        Environment variables:
            PROBE_DB_PATH: Path to the Firebird database file
            PROBE_DB_USER: Database user (default: sysdba)
            PROBE_DB_PASSWORD: Database password (default: masterkey)
            PROBE_DB_CHARSET: Database charset (default: UTF8)
        """
        return cls(
            path=db_path or _get_env("PROBE_DB_PATH", ""),
            user=_get_env("PROBE_DB_USER", "sysdba"),
            password=_get_env("PROBE_DB_PASSWORD", "masterkey"),
            charset=_get_env("PROBE_DB_CHARSET", "UTF8")
        )


@dataclass
class WorkerConfig:
    """Worker thread configuration."""
    max_workers: int = 10
    intake_position: int = 1

    @classmethod
    def from_env(cls) -> 'WorkerConfig':
        """
        Create worker config from environment variables.

        Environment variables:
            PROBE_MAX_WORKERS: Maximum number of worker threads (default: 10)
            PROBE_INTAKE_POSITION: Starting position for change intake (default: 1)
        """
        return cls(
            max_workers=_get_env_int("PROBE_MAX_WORKERS", 10),
            intake_position=_get_env_int("PROBE_INTAKE_POSITION", 1)
        )


@dataclass
class CloudSyncConfig:
    """Cloud synchronization configuration."""
    endpoint: str = "http://localhost:8080/api/changes"
    api_key: Optional[str] = None
    buffer_path: str = "probe_buffer.db"
    enable_background_retry: bool = True
    enabled: bool = True

    @classmethod
    def from_env(cls) -> 'CloudSyncConfig':
        """
        Create cloud sync config from environment variables.

        Environment variables:
            PROBE_CLOUD_ENDPOINT: Cloud API endpoint URL
            PROBE_CLOUD_API_KEY: API key for authentication
            PROBE_BUFFER_PATH: Path to local SQLite buffer file
            PROBE_ENABLE_BACKGROUND_RETRY: Enable background retry (default: true)
            PROBE_CLOUD_ENABLED: Enable cloud sync (default: true)
        """
        return cls(
            endpoint=_get_env("PROBE_CLOUD_ENDPOINT", "http://localhost:8080/api/changes"),
            api_key=_get_env("PROBE_CLOUD_API_KEY"),
            buffer_path=_get_env("PROBE_BUFFER_PATH", "probe_buffer.db"),
            enable_background_retry=_get_env_bool("PROBE_ENABLE_BACKGROUND_RETRY", True),
            enabled=_get_env_bool("PROBE_CLOUD_ENABLED", True)
        )


@dataclass
class AppConfig:
    """Main application configuration."""
    database: DatabaseConfig
    workers: WorkerConfig = field(default_factory=WorkerConfig)
    cloud_sync: CloudSyncConfig = field(default_factory=CloudSyncConfig)

    @classmethod
    def from_microsip_path(cls, db_path: str) -> 'AppConfig':
        """
        Create configuration using the microsip database path.

        This method creates a basic configuration with sensible defaults.
        For production use, prefer from_env() to use environment variables.
        """
        return cls(
            database=DatabaseConfig(path=db_path),
            workers=WorkerConfig(),
            cloud_sync=CloudSyncConfig()
        )

    @classmethod
    def from_env(cls, db_path: Optional[str] = None) -> 'AppConfig':
        """
        Create configuration from environment variables.

        This is the recommended method for production deployments.

        Args:
            db_path: Optional database path override. If not provided,
                     uses PROBE_DB_PATH environment variable.
        """
        return cls(
            database=DatabaseConfig.from_env(db_path),
            workers=WorkerConfig.from_env(),
            cloud_sync=CloudSyncConfig.from_env()
        )
