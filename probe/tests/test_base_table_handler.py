"""
Tests for BaseTableHandler class.

This module covers:
- Mutation routing
- INSERT, UPDATE, DELETE handling
- Row fetching
- Sync client integration
"""
import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch

from handlers.base_table_handler import BaseTableHandler
from models.change import Change


class TestBaseTableHandlerInit:
    """Test cases for BaseTableHandler initialization."""

    @pytest.mark.unit
    def test_init_with_all_params(self):
        """Test initialization with all parameters."""
        mock_conn = Mock()
        mock_sync_client = Mock()

        handler = BaseTableHandler(
            conn=mock_conn,
            table='USERS',
            primary_key='USER_ID',
            sync_client=mock_sync_client
        )

        assert handler.conn == mock_conn
        assert handler.table == 'USERS'
        assert handler.primary_key == 'USER_ID'
        assert handler.sync_client == mock_sync_client

    @pytest.mark.unit
    def test_init_without_sync_client(self):
        """Test initialization without sync client."""
        mock_conn = Mock()

        handler = BaseTableHandler(
            conn=mock_conn,
            table='ORDERS',
            primary_key='ORDER_ID',
            sync_client=None
        )

        assert handler.sync_client is None


class TestHandleMutation:
    """Test cases for handle_mutation method."""

    @pytest.fixture
    def handler(self):
        """Create a handler with mocks."""
        mock_conn = Mock()
        mock_sync_client = Mock()
        mock_sync_client.send_insert = Mock(return_value=True)
        mock_sync_client.send_update = Mock(return_value=True)
        mock_sync_client.send_delete = Mock(return_value=True)

        h = BaseTableHandler(
            conn=mock_conn,
            table='TEST_TABLE',
            primary_key='ID',
            sync_client=mock_sync_client
        )
        return h

    @pytest.mark.unit
    def test_handle_mutation_routes_insert(self, handler):
        """Test that INSERT mutations are routed correctly."""
        handler.handle_insert = Mock()

        change = Change(
            log_id=1,
            pk_val=100,
            table_id=1,
            mutation='INSERT',
            occured_at=datetime.now(),
            processed=False
        )

        handler.handle_mutation(change, worker_id=0)

        handler.handle_insert.assert_called_once_with(change)

    @pytest.mark.unit
    def test_handle_mutation_routes_update(self, handler):
        """Test that UPDATE mutations are routed correctly."""
        handler.handle_update = Mock()

        change = Change(
            log_id=2,
            pk_val=100,
            table_id=1,
            mutation='UPDATE',
            occured_at=datetime.now(),
            processed=False
        )

        handler.handle_mutation(change, worker_id=1)

        handler.handle_update.assert_called_once_with(change)

    @pytest.mark.unit
    def test_handle_mutation_routes_delete(self, handler):
        """Test that DELETE mutations are routed correctly."""
        handler.handle_delete = Mock()

        change = Change(
            log_id=3,
            pk_val=100,
            table_id=1,
            mutation='DELETE',
            occured_at=datetime.now(),
            processed=False
        )

        handler.handle_mutation(change, worker_id=2)

        handler.handle_delete.assert_called_once_with(change)


class TestHandleInsert:
    """Test cases for handle_insert method."""

    @pytest.mark.unit
    def test_handle_insert_fetches_row(self):
        """Test that handle_insert fetches the inserted row."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (100, 'Test Name', 'test@example.com')
        mock_conn.cursor.return_value = mock_cursor

        mock_sync_client = Mock()
        mock_sync_client.send_insert = Mock(return_value=True)

        handler = BaseTableHandler(
            conn=mock_conn,
            table='USERS',
            primary_key='ID',
            sync_client=mock_sync_client
        )

        change = Change(1, 100, 1, 'INSERT', datetime.now(), False)
        handler.handle_insert(change)

        # Verify SQL was executed
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0][0]
        assert 'SELECT * FROM USERS' in call_args
        assert 'ID = 100' in call_args

    @pytest.mark.unit
    def test_handle_insert_sends_to_sync_client(self):
        """Test that handle_insert sends data to sync client."""
        mock_conn = Mock()
        mock_cursor = Mock()
        row_data = (100, 'Test Name', 'test@example.com')
        mock_cursor.fetchone.return_value = row_data
        mock_conn.cursor.return_value = mock_cursor

        mock_sync_client = Mock()
        mock_sync_client.send_insert = Mock(return_value=True)

        handler = BaseTableHandler(
            conn=mock_conn,
            table='USERS',
            primary_key='ID',
            sync_client=mock_sync_client
        )

        occurred_at = datetime.now()
        change = Change(1, 100, 1, 'INSERT', occurred_at, False)
        handler.handle_insert(change)

        mock_sync_client.send_insert.assert_called_once_with(
            table='USERS',
            row_data=row_data,
            timestamp=occurred_at.timestamp()
        )

    @pytest.mark.unit
    def test_handle_insert_without_sync_client(self):
        """Test handle_insert when sync client is None."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (100, 'Test')
        mock_conn.cursor.return_value = mock_cursor

        handler = BaseTableHandler(
            conn=mock_conn,
            table='USERS',
            primary_key='ID',
            sync_client=None
        )

        change = Change(1, 100, 1, 'INSERT', datetime.now(), False)

        # Should not raise
        handler.handle_insert(change)


class TestHandleUpdate:
    """Test cases for handle_update method."""

    @pytest.mark.unit
    def test_handle_update_fetches_row(self):
        """Test that handle_update fetches the updated row."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (200, 'Updated Name', 'updated@example.com')
        mock_conn.cursor.return_value = mock_cursor

        mock_sync_client = Mock()
        mock_sync_client.send_update = Mock(return_value=True)

        handler = BaseTableHandler(
            conn=mock_conn,
            table='USERS',
            primary_key='USER_ID',
            sync_client=mock_sync_client
        )

        change = Change(2, 200, 1, 'UPDATE', datetime.now(), False)
        handler.handle_update(change)

        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0][0]
        assert 'SELECT * FROM USERS' in call_args
        assert 'USER_ID = 200' in call_args

    @pytest.mark.unit
    def test_handle_update_sends_to_sync_client(self):
        """Test that handle_update sends data to sync client."""
        mock_conn = Mock()
        mock_cursor = Mock()
        row_data = (200, 'Updated Name')
        mock_cursor.fetchone.return_value = row_data
        mock_conn.cursor.return_value = mock_cursor

        mock_sync_client = Mock()
        mock_sync_client.send_update = Mock(return_value=True)

        handler = BaseTableHandler(
            conn=mock_conn,
            table='USERS',
            primary_key='ID',
            sync_client=mock_sync_client
        )

        occurred_at = datetime.now()
        change = Change(2, 200, 1, 'UPDATE', occurred_at, False)
        handler.handle_update(change)

        mock_sync_client.send_update.assert_called_once_with(
            table='USERS',
            row_data=row_data,
            timestamp=occurred_at.timestamp()
        )

    @pytest.mark.unit
    def test_handle_update_without_sync_client(self):
        """Test handle_update when sync client is None."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (200, 'Test')
        mock_conn.cursor.return_value = mock_cursor

        handler = BaseTableHandler(
            conn=mock_conn,
            table='USERS',
            primary_key='ID',
            sync_client=None
        )

        change = Change(2, 200, 1, 'UPDATE', datetime.now(), False)

        # Should not raise
        handler.handle_update(change)


class TestHandleDelete:
    """Test cases for handle_delete method."""

    @pytest.mark.unit
    def test_handle_delete_sends_pk_info(self):
        """Test that handle_delete sends primary key info."""
        mock_conn = Mock()
        mock_sync_client = Mock()
        mock_sync_client.send_delete = Mock(return_value=True)

        handler = BaseTableHandler(
            conn=mock_conn,
            table='USERS',
            primary_key='USER_ID',
            sync_client=mock_sync_client
        )

        occurred_at = datetime.now()
        change = Change(3, 300, 1, 'DELETE', occurred_at, False)
        handler.handle_delete(change)

        mock_sync_client.send_delete.assert_called_once_with(
            table='USERS',
            primary_key='USER_ID',
            value=300,
            timestamp=occurred_at.timestamp()
        )

    @pytest.mark.unit
    def test_handle_delete_without_sync_client(self):
        """Test handle_delete when sync client is None."""
        mock_conn = Mock()

        handler = BaseTableHandler(
            conn=mock_conn,
            table='USERS',
            primary_key='ID',
            sync_client=None
        )

        change = Change(3, 300, 1, 'DELETE', datetime.now(), False)

        # Should not raise
        handler.handle_delete(change)

    @pytest.mark.unit
    def test_handle_delete_does_not_fetch_row(self):
        """Test that handle_delete doesn't fetch the deleted row."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        mock_sync_client = Mock()
        mock_sync_client.send_delete = Mock(return_value=True)

        handler = BaseTableHandler(
            conn=mock_conn,
            table='USERS',
            primary_key='ID',
            sync_client=mock_sync_client
        )

        change = Change(3, 300, 1, 'DELETE', datetime.now(), False)
        handler.handle_delete(change)

        # Cursor should not be used
        mock_conn.cursor.assert_not_called()


class TestRowNotFoundHandling:
    """Test cases for handling rows not found."""

    @pytest.mark.unit
    def test_handle_insert_row_not_found(self):
        """Test handle_insert when row is not found."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value = mock_cursor

        mock_sync_client = Mock()
        mock_sync_client.send_insert = Mock(return_value=True)

        handler = BaseTableHandler(
            conn=mock_conn,
            table='USERS',
            primary_key='ID',
            sync_client=mock_sync_client
        )

        change = Change(1, 999, 1, 'INSERT', datetime.now(), False)
        handler.handle_insert(change)

        # Should still call send_insert with None row_data
        mock_sync_client.send_insert.assert_called_once()
        call_args = mock_sync_client.send_insert.call_args
        assert call_args.kwargs['row_data'] is None

    @pytest.mark.unit
    def test_handle_update_row_not_found(self):
        """Test handle_update when row is not found."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value = mock_cursor

        mock_sync_client = Mock()
        mock_sync_client.send_update = Mock(return_value=True)

        handler = BaseTableHandler(
            conn=mock_conn,
            table='USERS',
            primary_key='ID',
            sync_client=mock_sync_client
        )

        change = Change(2, 999, 1, 'UPDATE', datetime.now(), False)
        handler.handle_update(change)

        # Should still call send_update with None row_data
        mock_sync_client.send_update.assert_called_once()
        call_args = mock_sync_client.send_update.call_args
        assert call_args.kwargs['row_data'] is None


class TestSQLConstruction:
    """Test cases for SQL query construction."""

    @pytest.mark.unit
    def test_sql_uses_table_name(self):
        """Test that SQL uses the correct table name."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value = mock_cursor

        handler = BaseTableHandler(
            conn=mock_conn,
            table='CUSTOM_TABLE_NAME',
            primary_key='ID',
            sync_client=Mock()
        )

        change = Change(1, 100, 1, 'INSERT', datetime.now(), False)
        handler.handle_insert(change)

        call_args = mock_cursor.execute.call_args[0][0]
        assert 'CUSTOM_TABLE_NAME' in call_args

    @pytest.mark.unit
    def test_sql_uses_primary_key(self):
        """Test that SQL uses the correct primary key."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value = mock_cursor

        handler = BaseTableHandler(
            conn=mock_conn,
            table='USERS',
            primary_key='CUSTOM_PK',
            sync_client=Mock()
        )

        change = Change(1, 100, 1, 'INSERT', datetime.now(), False)
        handler.handle_insert(change)

        call_args = mock_cursor.execute.call_args[0][0]
        assert 'CUSTOM_PK = 100' in call_args

    @pytest.mark.unit
    def test_sql_uses_pk_value_from_change(self):
        """Test that SQL uses the pk_val from the change."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value = mock_cursor

        handler = BaseTableHandler(
            conn=mock_conn,
            table='USERS',
            primary_key='ID',
            sync_client=Mock()
        )

        change = Change(1, 12345, 1, 'UPDATE', datetime.now(), False)
        handler.handle_update(change)

        call_args = mock_cursor.execute.call_args[0][0]
        assert '12345' in call_args
