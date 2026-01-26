"""
Pytest configuration and shared fixtures for the probe application tests.
"""
import tempfile
import pytest
import fdb
from pathlib import Path
from unittest.mock import Mock, MagicMock

from config.app_config import AppConfig, DatabaseConfig, WorkerConfig, CloudSyncConfig
from models.connection_info import ConnectionInfo


@pytest.fixture
def temp_fdb_file():
    """Create a temporary FDB file for testing."""
    import os
    import uuid
    
    # Create a unique temporary file path
    temp_dir = tempfile.gettempdir()
    temp_filename = f"test_{uuid.uuid4().hex}.fdb"
    temp_path = os.path.join(temp_dir, temp_filename)
    
    try:
        # Create the database
        conn = fdb.create_database(
            dsn=temp_path,
            user='SYSDBA',
            password='masterkey',
            page_size=4096
        )
        conn.close()
        
        yield temp_path
        
    finally:
        # Cleanup
        try:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
        except (OSError, FileNotFoundError):
            pass


@pytest.fixture
def fdb_connection(temp_fdb_file):
    """Create a connection to a temporary FDB database."""
    conn = fdb.connect(
        dsn=temp_fdb_file,
        user='SYSDBA',
        password='masterkey'
    )
    yield conn
    conn.close()


@pytest.fixture
def test_database_config(temp_fdb_file):
    """Create a test database configuration."""
    return DatabaseConfig(
        path=temp_fdb_file,
        user='SYSDBA',
        password='masterkey',
        charset='UTF8'
    )


@pytest.fixture
def test_worker_config():
    """Create a test worker configuration."""
    return WorkerConfig(
        max_workers=2,  # Small number for testing
        intake_position=1
    )


@pytest.fixture
def test_cloud_sync_config():
    """Create a test cloud sync configuration."""
    return CloudSyncConfig(
        endpoint="http://localhost:8080/api/changes",
        api_key="test-api-key",
        buffer_path=":memory:",  # Use in-memory SQLite for tests
        enable_background_retry=False,  # Disable for tests
        enabled=True
    )


@pytest.fixture
def test_app_config(test_database_config, test_worker_config, test_cloud_sync_config):
    """Create a test application configuration."""
    return AppConfig(
        database=test_database_config,
        workers=test_worker_config,
        cloud_sync=test_cloud_sync_config
    )


@pytest.fixture
def test_connection_info(test_database_config):
    """Create a test connection info."""
    return ConnectionInfo(
        db_path=test_database_config.path,
        db_user=test_database_config.user,
        db_password=test_database_config.password
    )


@pytest.fixture
def mock_queue():
    """Create a mock queue for testing."""
    return Mock()


@pytest.fixture
def mock_changes_intake():
    """Create a mock changes intake for testing."""
    mock = Mock()
    mock.start = Mock()
    mock.stop = Mock()
    mock.join = Mock()
    return mock


@pytest.fixture
def mock_changes_processor():
    """Create a mock changes processor for testing."""
    mock = Mock()
    mock.begin_read = Mock()
    return mock


@pytest.fixture
def mock_database_manager():
    """Create a mock database manager for testing."""
    mock = Mock()
    mock.setup = Mock(return_value=({1: 'test_table'}, {'test_table': 'id'}))
    mock.ensure_clean_slate = Mock()
    mock.reset_state = Mock()
    return mock


@pytest.fixture
def mock_executor():
    """Create a mock thread pool executor for testing."""
    mock = Mock()
    mock.submit = Mock()
    mock.shutdown = Mock()
    return mock


@pytest.fixture
def mock_sync_client():
    """Create a mock cloud sync client for testing."""
    mock = Mock()
    mock.send_insert = Mock(return_value=True)
    mock.send_update = Mock(return_value=True)
    mock.send_delete = Mock(return_value=True)
    mock.send = Mock(return_value=True)
    mock.get_buffer_stats = Mock(return_value={'pending_count': 0, 'endpoint': 'http://test', 'connected': True})
    mock.flush_buffer = Mock(return_value=0)
    mock.close = Mock()
    return mock
