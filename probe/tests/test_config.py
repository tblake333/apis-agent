"""
Tests for configuration classes.
"""
import pytest
from config.app_config import AppConfig, DatabaseConfig, WorkerConfig


class TestDatabaseConfig:
    """Test cases for DatabaseConfig class."""
    
    def test_default_values(self):
        """Test that default values are set correctly."""
        config = DatabaseConfig(path="/test/path.fdb")
        
        assert config.path == "/test/path.fdb"
        assert config.user == "sysdba"
        assert config.password == "masterkey"
        assert config.charset == "UTF8"
    
    def test_custom_values(self):
        """Test that custom values can be set."""
        config = DatabaseConfig(
            path="/custom/path.fdb",
            user="custom_user",
            password="custom_password",
            charset="LATIN1"
        )
        
        assert config.path == "/custom/path.fdb"
        assert config.user == "custom_user"
        assert config.password == "custom_password"
        assert config.charset == "LATIN1"


class TestWorkerConfig:
    """Test cases for WorkerConfig class."""
    
    def test_default_values(self):
        """Test that default values are set correctly."""
        config = WorkerConfig()
        
        assert config.max_workers == 10
        assert config.intake_position == 1
    
    def test_custom_values(self):
        """Test that custom values can be set."""
        config = WorkerConfig(max_workers=5, intake_position=10)
        
        assert config.max_workers == 5
        assert config.intake_position == 10


class TestAppConfig:
    """Test cases for AppConfig class."""
    
    def test_initialization(self, test_database_config, test_worker_config):
        """Test AppConfig initialization."""
        config = AppConfig(
            database=test_database_config,
            workers=test_worker_config
        )
        
        assert config.database == test_database_config
        assert config.workers == test_worker_config
    
    def test_default_worker_config(self, test_database_config):
        """Test that default worker config is used when not provided."""
        config = AppConfig(database=test_database_config)
        
        assert config.workers.max_workers == 10
        assert config.workers.intake_position == 1
    
    def test_from_microsip_path(self):
        """Test creating config from microsip path."""
        db_path = "/test/microsip.fdb"
        config = AppConfig.from_microsip_path(db_path)
        
        assert config.database.path == db_path
        assert config.database.user == "sysdba"
        assert config.database.password == "masterkey"
        assert config.database.charset == "UTF8"
        assert config.workers.max_workers == 10
