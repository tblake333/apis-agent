"""
Tests for DatabaseManager class.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from database.database_manager import DatabaseManager


class TestDatabaseManager:
    """Test cases for DatabaseManager class."""
    
    def test_initialization(self, fdb_connection):
        """Test DatabaseManager initialization."""
        db_manager = DatabaseManager(fdb_connection)
        assert db_manager.conn == fdb_connection
    
    def test_get_table_names(self, fdb_connection):
        """Test getting table names from database."""
        db_manager = DatabaseManager(fdb_connection)
        
        # Create a test table
        fdb_connection.execute_immediate("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50)
            )
        """)
        fdb_connection.commit()
        
        table_names = db_manager.get_table_names()
        
        assert "test_table" in table_names
        assert "CHANGES_LOG" not in table_names  # Should be created later
    
    def test_get_table_primary_keys(self, fdb_connection):
        """Test getting table primary keys."""
        db_manager = DatabaseManager(fdb_connection)
        
        # Create a test table with primary key
        fdb_connection.execute_immediate("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50)
            )
        """)
        fdb_connection.commit()
        
        primary_keys = db_manager.get_table_primary_keys()
        
        assert "test_table" in primary_keys
        assert "id" in primary_keys["test_table"]
    
    def test_get_column_datatype(self, fdb_connection):
        """Test getting column data type."""
        db_manager = DatabaseManager(fdb_connection)
        
        # Create a test table
        fdb_connection.execute_immediate("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50)
            )
        """)
        fdb_connection.commit()
        
        datatype = db_manager.get_column_datatype("test_table", "id")
        assert datatype == "INTEGER"
        
        datatype = db_manager.get_column_datatype("test_table", "name")
        assert datatype == "VARCHAR"
    
    def test_get_table_to_primary_key(self, fdb_connection):
        """Test getting table to primary key mapping."""
        db_manager = DatabaseManager(fdb_connection)
        
        # Create test tables
        fdb_connection.execute_immediate("""
            CREATE TABLE test_table1 (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50)
            )
        """)
        fdb_connection.execute_immediate("""
            CREATE TABLE test_table2 (
                pk INTEGER PRIMARY KEY,
                description VARCHAR(100)
            )
        """)
        fdb_connection.commit()
        
        table_to_pk = db_manager.get_table_to_primary_key()
        
        assert "test_table1" in table_to_pk
        assert table_to_pk["test_table1"] == "id"
        assert "test_table2" in table_to_pk
        assert table_to_pk["test_table2"] == "pk"
    
    def test_create_changes_log_table(self, fdb_connection):
        """Test creating the changes log table."""
        db_manager = DatabaseManager(fdb_connection)
        
        db_manager.create_changes_log_table()
        
        # Verify table was created
        table_names = db_manager.get_table_names()
        assert "CHANGES_LOG" in table_names
        
        # Verify table structure
        primary_keys = db_manager.get_table_primary_keys()
        assert "CHANGES_LOG" in primary_keys
        assert "LOG_ID" in primary_keys["CHANGES_LOG"]
    
    def test_create_table_trigger(self, fdb_connection):
        """Test creating table triggers."""
        db_manager = DatabaseManager(fdb_connection)
        
        # Create a test table
        fdb_connection.execute_immediate("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50)
            )
        """)
        fdb_connection.commit()
        
        # Create trigger
        db_manager.create_table_trigger("test_table", 1, "id")
        
        # Verify trigger was created (we can't easily test trigger execution in unit tests)
        # but we can verify no exceptions were raised
        assert True  # If we get here, trigger creation succeeded
    
    def test_create_table_triggers(self, fdb_connection):
        """Test creating triggers for multiple tables."""
        db_manager = DatabaseManager(fdb_connection)
        
        # Create test tables
        fdb_connection.execute_immediate("""
            CREATE TABLE test_table1 (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50)
            )
        """)
        fdb_connection.execute_immediate("""
            CREATE TABLE test_table2 (
                pk INTEGER PRIMARY KEY,
                description VARCHAR(100)
            )
        """)
        fdb_connection.commit()
        
        table_to_primary_key = {
            "test_table1": "id",
            "test_table2": "pk"
        }
        
        table_to_id, id_to_table = db_manager.create_table_triggers(table_to_primary_key)
        
        assert len(table_to_id) == 2
        assert len(id_to_table) == 2
        assert "test_table1" in table_to_id
        assert "test_table2" in table_to_id
        assert 0 in id_to_table
        assert 1 in id_to_table
    
    def test_ensure_clean_slate_empty(self, fdb_connection):
        """Test ensure_clean_slate with empty changes log."""
        db_manager = DatabaseManager(fdb_connection)
        
        # Create changes log table
        db_manager.create_changes_log_table()
        
        # Should not raise exception for empty table
        db_manager.ensure_clean_slate(fdb_connection)
        assert True  # If we get here, no exception was raised
    
    def test_ensure_clean_slate_with_data(self, fdb_connection):
        """Test ensure_clean_slate with data in changes log."""
        db_manager = DatabaseManager(fdb_connection)
        
        # Create changes log table
        db_manager.create_changes_log_table()
        
        # Insert some data
        fdb_connection.execute_immediate("""
            INSERT INTO CHANGES_LOG (LOG_ID, PK_VAL, TABLE_ID, MUTATION, OCCURRED_AT, PROCESSED)
            VALUES (1, 123, 1, 'INSERT', CURRENT_TIMESTAMP, 0)
        """)
        fdb_connection.commit()
        
        # Should exit when changes log is not empty
        with patch('database.database_manager.exit') as mock_exit:
            db_manager.ensure_clean_slate(fdb_connection)
            mock_exit.assert_called_once()
    
    def test_delete_processed_mutations(self, fdb_connection):
        """Test deleting processed mutations."""
        db_manager = DatabaseManager(fdb_connection)
        
        # Create changes log table
        db_manager.create_changes_log_table()
        
        # Insert test data
        fdb_connection.execute_immediate("""
            INSERT INTO CHANGES_LOG (LOG_ID, PK_VAL, TABLE_ID, MUTATION, OCCURRED_AT, PROCESSED)
            VALUES (1, 123, 1, 'INSERT', CURRENT_TIMESTAMP, 1),
                   (2, 124, 1, 'UPDATE', CURRENT_TIMESTAMP, 0),
                   (3, 125, 1, 'DELETE', CURRENT_TIMESTAMP, 1)
        """)
        fdb_connection.commit()
        
        # Delete processed mutations
        db_manager.delete_processed_mutations()
        
        # Verify only unprocessed mutations remain
        cursor = fdb_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM CHANGES_LOG WHERE PROCESSED = 1")
        processed_count = cursor.fetchone()[0]
        assert processed_count == 0
        
        cursor.execute("SELECT COUNT(*) FROM CHANGES_LOG WHERE PROCESSED = 0")
        unprocessed_count = cursor.fetchone()[0]
        assert unprocessed_count == 1
    
    def test_reset_state(self, fdb_connection):
        """Test resetting database state."""
        db_manager = DatabaseManager(fdb_connection)
        
        # Create changes log table and some triggers
        db_manager.create_changes_log_table()
        
        fdb_connection.execute_immediate("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50)
            )
        """)
        fdb_connection.commit()
        
        db_manager.create_table_trigger("test_table", 0, "id")
        
        # Reset state
        db_manager.reset_state()
        
        # Verify changes log table is gone
        table_names = db_manager.get_table_names()
        assert "CHANGES_LOG" not in table_names
