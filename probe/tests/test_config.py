"""
Tests for configuration classes.
"""
import os
import pytest
from unittest.mock import patch
from config.app_config import AppConfig, DatabaseConfig, WorkerConfig, CloudSyncConfig


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

    def test_from_env(self):
        """Test creating config from environment variables."""
        with patch.dict(os.environ, {
            'PROBE_DB_PATH': '/env/path.fdb',
            'PROBE_DB_USER': 'env_user',
            'PROBE_DB_PASSWORD': 'env_password',
            'PROBE_DB_CHARSET': 'WIN1252'
        }):
            config = DatabaseConfig.from_env()

            assert config.path == '/env/path.fdb'
            assert config.user == 'env_user'
            assert config.password == 'env_password'
            assert config.charset == 'WIN1252'

    def test_from_env_with_override(self):
        """Test creating config from env with path override."""
        with patch.dict(os.environ, {'PROBE_DB_PATH': '/env/path.fdb'}):
            config = DatabaseConfig.from_env(db_path='/override/path.fdb')

            assert config.path == '/override/path.fdb'


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

    def test_from_env(self):
        """Test creating config from environment variables."""
        with patch.dict(os.environ, {
            'PROBE_MAX_WORKERS': '20',
            'PROBE_INTAKE_POSITION': '5'
        }):
            config = WorkerConfig.from_env()

            assert config.max_workers == 20
            assert config.intake_position == 5

    def test_from_env_defaults(self):
        """Test that defaults are used when env vars not set."""
        with patch.dict(os.environ, {}, clear=True):
            config = WorkerConfig.from_env()

            assert config.max_workers == 10
            assert config.intake_position == 1


class TestCloudSyncConfig:
    """Test cases for CloudSyncConfig class."""

    def test_default_values(self):
        """Test that default values are set correctly."""
        config = CloudSyncConfig()

        assert config.endpoint == "http://localhost:8080/api/changes"
        assert config.api_key is None
        assert config.buffer_path == "probe_buffer.db"
        assert config.enable_background_retry is True
        assert config.enabled is True

    def test_custom_values(self):
        """Test that custom values can be set."""
        config = CloudSyncConfig(
            endpoint="https://api.example.com/changes",
            api_key="secret-key",
            buffer_path="/custom/buffer.db",
            enable_background_retry=False,
            enabled=False
        )

        assert config.endpoint == "https://api.example.com/changes"
        assert config.api_key == "secret-key"
        assert config.buffer_path == "/custom/buffer.db"
        assert config.enable_background_retry is False
        assert config.enabled is False

    def test_from_env(self):
        """Test creating config from environment variables."""
        with patch.dict(os.environ, {
            'PROBE_CLOUD_ENDPOINT': 'https://api.test.com/changes',
            'PROBE_CLOUD_API_KEY': 'test-key',
            'PROBE_BUFFER_PATH': '/test/buffer.db',
            'PROBE_ENABLE_BACKGROUND_RETRY': 'false',
            'PROBE_CLOUD_ENABLED': 'true'
        }):
            config = CloudSyncConfig.from_env()

            assert config.endpoint == 'https://api.test.com/changes'
            assert config.api_key == 'test-key'
            assert config.buffer_path == '/test/buffer.db'
            assert config.enable_background_retry is False
            assert config.enabled is True

    def test_from_env_boolean_parsing(self):
        """Test that boolean env vars are parsed correctly."""
        test_cases = [
            ('true', True),
            ('True', True),
            ('TRUE', True),
            ('1', True),
            ('yes', True),
            ('on', True),
            ('false', False),
            ('False', False),
            ('FALSE', False),
            ('0', False),
            ('no', False),
            ('off', False),
        ]

        for value, expected in test_cases:
            with patch.dict(os.environ, {'PROBE_CLOUD_ENABLED': value}):
                config = CloudSyncConfig.from_env()
                assert config.enabled is expected, f"Failed for value '{value}'"


class TestAppConfig:
    """Test cases for AppConfig class."""

    def test_initialization(self, test_database_config, test_worker_config, test_cloud_sync_config):
        """Test AppConfig initialization."""
        config = AppConfig(
            database=test_database_config,
            workers=test_worker_config,
            cloud_sync=test_cloud_sync_config
        )

        assert config.database == test_database_config
        assert config.workers == test_worker_config
        assert config.cloud_sync == test_cloud_sync_config

    def test_default_worker_config(self, test_database_config):
        """Test that default worker config is used when not provided."""
        config = AppConfig(database=test_database_config)

        assert config.workers.max_workers == 10
        assert config.workers.intake_position == 1

    def test_default_cloud_sync_config(self, test_database_config):
        """Test that default cloud sync config is used when not provided."""
        config = AppConfig(database=test_database_config)

        assert config.cloud_sync.endpoint == "http://localhost:8080/api/changes"
        assert config.cloud_sync.enabled is True

    def test_from_microsip_path(self):
        """Test creating config from microsip path."""
        db_path = "/test/microsip.fdb"
        config = AppConfig.from_microsip_path(db_path)

        assert config.database.path == db_path
        assert config.database.user == "sysdba"
        assert config.database.password == "masterkey"
        assert config.database.charset == "UTF8"
        assert config.workers.max_workers == 10
        assert config.cloud_sync.enabled is True

    def test_from_env(self):
        """Test creating config from environment variables."""
        with patch.dict(os.environ, {
            'PROBE_DB_PATH': '/env/microsip.fdb',
            'PROBE_MAX_WORKERS': '5',
            'PROBE_CLOUD_ENDPOINT': 'https://api.example.com/changes'
        }):
            config = AppConfig.from_env()

            assert config.database.path == '/env/microsip.fdb'
            assert config.workers.max_workers == 5
            assert config.cloud_sync.endpoint == 'https://api.example.com/changes'

    def test_from_env_with_path_override(self):
        """Test creating config from env with path override."""
        with patch.dict(os.environ, {'PROBE_DB_PATH': '/env/path.fdb'}):
            config = AppConfig.from_env(db_path='/override/path.fdb')

            assert config.database.path == '/override/path.fdb'
