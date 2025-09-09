"""
Tests for model classes.
"""
import pytest
from datetime import datetime
from models.change import Change
from models.connection_info import ConnectionInfo


class TestChange:
    """Test cases for Change model."""
    
    def test_change_creation(self):
        """Test creating a Change instance."""
        now = datetime.now()
        change = Change(
            log_id=1,
            pk_val=123,
            table_id=5,
            mutation="INSERT",
            occured_at=now,
            processed=False
        )
        
        assert change.log_id == 1
        assert change.pk_val == 123
        assert change.table_id == 5
        assert change.mutation == "INSERT"
        assert change.occured_at == now
        assert change.processed is False
    
    def test_change_from_tuple(self):
        """Test creating Change from tuple (database row)."""
        now = datetime.now()
        row = (1, 123, 5, "UPDATE", now, True)
        
        change = Change(*row)
        
        assert change.log_id == 1
        assert change.pk_val == 123
        assert change.table_id == 5
        assert change.mutation == "UPDATE"
        assert change.occured_at == now
        assert change.processed is True
    
    def test_change_mutation_types(self):
        """Test different mutation types."""
        now = datetime.now()
        
        insert_change = Change(1, 123, 5, "INSERT", now, False)
        update_change = Change(2, 124, 5, "UPDATE", now, False)
        delete_change = Change(3, 125, 5, "DELETE", now, False)
        
        assert insert_change.mutation == "INSERT"
        assert update_change.mutation == "UPDATE"
        assert delete_change.mutation == "DELETE"


class TestConnectionInfo:
    """Test cases for ConnectionInfo model."""
    
    def test_connection_info_creation(self):
        """Test creating a ConnectionInfo instance."""
        conn_info = ConnectionInfo(
            db_path="/test/path.fdb",
            db_user="test_user",
            db_password="test_password"
        )
        
        assert conn_info.db_path == "/test/path.fdb"
        assert conn_info.db_user == "test_user"
        assert conn_info.db_password == "test_password"
    
    def test_connection_info_required_fields(self):
        """Test ConnectionInfo requires all fields."""
        # ConnectionInfo requires all fields - no defaults
        conn_info = ConnectionInfo(
            db_path="/test/path.fdb",
            db_user="test_user",
            db_password="test_password"
        )
        
        assert conn_info.db_path == "/test/path.fdb"
        assert conn_info.db_user == "test_user"
        assert conn_info.db_password == "test_password"
    
    def test_connection_info_equality(self):
        """Test ConnectionInfo equality comparison."""
        conn_info1 = ConnectionInfo(
            db_path="/test/path.fdb",
            db_user="test_user",
            db_password="test_password"
        )
        conn_info2 = ConnectionInfo(
            db_path="/test/path.fdb",
            db_user="test_user",
            db_password="test_password"
        )
        conn_info3 = ConnectionInfo(
            db_path="/different/path.fdb",
            db_user="test_user",
            db_password="test_password"
        )
        
        assert conn_info1 == conn_info2
        assert conn_info1 != conn_info3
