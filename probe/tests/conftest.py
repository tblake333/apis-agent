"""
Pytest configuration and shared fixtures for the probe application tests.
"""
import tempfile
import pytest
import fdb
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

from config.app_config import AppConfig, DatabaseConfig, WorkerConfig, CloudSyncConfig
from models.connection_info import ConnectionInfo


def is_firebird_available():
    """Check if Firebird library is available and compatible."""
    try:
        # Try to load the library to check architecture compatibility
        fdb.load_api()
        return True
    except (OSError, Exception):
        return False


@pytest.fixture
def temp_fdb_file():
    """Create a temporary FDB file for testing."""
    if not is_firebird_available():
        pytest.skip("Firebird not available or incompatible architecture")

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


@pytest.fixture
def mock_urlopen():
    """Mock urllib.request.urlopen for HTTP testing."""
    with patch('urllib.request.urlopen') as mock:
        yield mock


@pytest.fixture
def temp_sqlite_buffer():
    """Temporary file-based SQLite buffer."""
    import os
    import uuid
    temp_dir = tempfile.gettempdir()
    temp_filename = f"test_buffer_{uuid.uuid4().hex}.db"
    temp_path = os.path.join(temp_dir, temp_filename)

    yield temp_path

    # Cleanup
    try:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
    except (OSError, FileNotFoundError):
        pass


@pytest.fixture
def populated_fdb_with_tables(fdb_connection):
    """FDB with test tables and sample data for schema discovery."""
    # Create test tables with various types
    fdb_connection.execute_immediate("""
        CREATE TABLE USERS (
            USER_ID INTEGER NOT NULL PRIMARY KEY,
            USERNAME VARCHAR(50) NOT NULL,
            EMAIL VARCHAR(100),
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            IS_ACTIVE SMALLINT DEFAULT 1
        )
    """)
    fdb_connection.commit()

    fdb_connection.execute_immediate("""
        CREATE TABLE ORDERS (
            ORDER_ID INTEGER NOT NULL PRIMARY KEY,
            USER_ID INTEGER NOT NULL REFERENCES USERS(USER_ID),
            TOTAL_AMOUNT NUMERIC(10, 2),
            ORDER_DATE DATE,
            STATUS VARCHAR(20) DEFAULT 'PENDING'
        )
    """)
    fdb_connection.commit()

    fdb_connection.execute_immediate("""
        CREATE TABLE ORDER_ITEMS (
            ITEM_ID INTEGER NOT NULL PRIMARY KEY,
            ORDER_ID INTEGER NOT NULL REFERENCES ORDERS(ORDER_ID),
            PRODUCT_NAME VARCHAR(100),
            QUANTITY INTEGER,
            UNIT_PRICE NUMERIC(8, 2)
        )
    """)
    fdb_connection.commit()

    # Insert sample data
    fdb_connection.execute_immediate("""
        INSERT INTO USERS (USER_ID, USERNAME, EMAIL) VALUES (1, 'john_doe', 'john@example.com')
    """)
    fdb_connection.execute_immediate("""
        INSERT INTO USERS (USER_ID, USERNAME, EMAIL) VALUES (2, 'jane_smith', 'jane@example.com')
    """)
    fdb_connection.commit()

    return fdb_connection


@pytest.fixture
def changes_log_setup(fdb_connection):
    """FDB with CHANGES_LOG table and triggers configured."""
    # Create BOOLEAN domain if it doesn't exist
    try:
        fdb_connection.execute_immediate("""
            CREATE DOMAIN BOOLEAN AS SMALLINT CHECK (value is null or value in (0, 1))
        """)
        fdb_connection.commit()
    except Exception:
        fdb_connection.rollback()

    # Create CHANGES_LOG table
    fdb_connection.execute_immediate("""
        CREATE TABLE CHANGES_LOG (
            LOG_ID INTEGER NOT NULL PRIMARY KEY,
            PK_VAL INTEGER NOT NULL,
            TABLE_ID INTEGER NOT NULL,
            MUTATION VARCHAR(31),
            OCCURRED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PROCESSED SMALLINT DEFAULT 0
        )
    """)
    fdb_connection.commit()

    # Create sequence for LOG_ID
    fdb_connection.execute_immediate("CREATE SEQUENCE SEQ_CHANGES_LOG")
    fdb_connection.commit()

    # Create a test table to trigger changes
    fdb_connection.execute_immediate("""
        CREATE TABLE TEST_DATA (
            ID INTEGER NOT NULL PRIMARY KEY,
            NAME VARCHAR(100),
            VALUE NUMERIC(10, 2)
        )
    """)
    fdb_connection.commit()

    return fdb_connection


@pytest.fixture
def real_cloud_sync_client():
    """Create a real CloudSyncClient with in-memory buffer for testing."""
    from sync.cloud_sync_client import CloudSyncClient

    client = CloudSyncClient(
        endpoint="http://localhost:8080/api/changes",
        buffer_path=":memory:",
        enable_background_retry=False
    )
    yield client
    client.close()


@pytest.fixture
def real_local_buffer():
    """Create a real LocalBuffer with in-memory storage for testing."""
    from sync.local_buffer import LocalBuffer

    buffer = LocalBuffer(":memory:")
    yield buffer
    buffer.close()
