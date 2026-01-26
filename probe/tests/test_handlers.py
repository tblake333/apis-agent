"""
Tests for handler classes.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from queue import Queue
from datetime import datetime

from handlers.changes_intake import ChangesIntake
from handlers.changes_processor import ChangesProcessor
from models.change import Change
from models.connection_info import ConnectionInfo


class TestChangesIntake:
    """Test cases for ChangesIntake class."""
    
    def test_initialization(self, fdb_connection, mock_queue):
        """Test ChangesIntake initialization."""
        intake = ChangesIntake(fdb_connection, 1, mock_queue)
        
        assert intake.conn == fdb_connection
        assert intake.pos == 1
        assert intake.output == mock_queue
        assert intake.events == [ChangesIntake.EVENT_NAME]
        assert not intake.stopped()
    
    def test_stop_and_stopped(self, fdb_connection, mock_queue):
        """Test stop and stopped methods."""
        intake = ChangesIntake(fdb_connection, 1, mock_queue)
        
        assert not intake.stopped()
        intake.stop()
        assert intake.stopped()
    
    def test_run_with_changes(self, fdb_connection, mock_queue):
        """Test running intake with database changes."""
        # Create changes log table
        fdb_connection.execute_immediate("""
            CREATE DOMAIN BOOLEAN AS SMALLINT CHECK (value is null or value in (0, 1));
            CREATE TABLE CHANGES_LOG(
                LOG_ID int not null primary key,
                PK_VAL int not null,
                TABLE_ID int not null,
                MUTATION varchar(31),
                OCCURRED_AT TIMESTAMP,
                PROCESSED BOOLEAN DEFAULT 0
            );
            CREATE SEQUENCE SEQ_CHANGES_LOG;
        """)
        fdb_connection.commit()
        
        # Insert a change
        fdb_connection.execute_immediate("""
            INSERT INTO CHANGES_LOG (LOG_ID, PK_VAL, TABLE_ID, MUTATION, OCCURRED_AT, PROCESSED)
            VALUES (1, 123, 1, 'INSERT', CURRENT_TIMESTAMP, 0)
        """)
        fdb_connection.commit()
        
        intake = ChangesIntake(fdb_connection, 1, mock_queue)
        
        # Mock the event conduit to avoid blocking
        with patch.object(intake.conn, 'event_conduit') as mock_conduit:
            mock_conduit.return_value.__enter__.return_value.wait = Mock()
            
            # Start intake in a thread and stop it quickly
            intake.start()
            intake.stop()
            intake.join(timeout=1)
        
        # Verify that the change was processed
        assert mock_queue.put.called


class TestChangesProcessor:
    """Test cases for ChangesProcessor class."""

    def test_initialization(self, test_connection_info, mock_queue, mock_sync_client):
        """Test ChangesProcessor initialization."""
        id_to_table = {1: 'test_table'}
        table_to_primary_key = {'test_table': 'id'}

        processor = ChangesProcessor(
            test_connection_info,
            mock_queue,
            id_to_table,
            table_to_primary_key,
            mock_sync_client
        )

        assert processor.conn_info == test_connection_info
        assert processor.intake == mock_queue
        assert processor.id_to_table == id_to_table
        assert processor.table_primary_keys == table_to_primary_key
        assert processor.sync_client == mock_sync_client
    
    @patch('handlers.changes_processor.fdb.connect')
    def test_begin_read_with_changes(self, mock_fdb_connect, test_connection_info, mock_queue, mock_sync_client):
        """Test begin_read method with changes to process."""
        # Setup mocks
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_fdb_connect.return_value = mock_conn

        # Create a test change
        test_change = Change(
            log_id=1,
            pk_val=123,
            table_id=1,
            mutation="INSERT",
            occured_at=datetime.now(),
            processed=False
        )

        # Mock queue to return the change then None (to stop)
        mock_queue.get.side_effect = [test_change, None]

        id_to_table = {1: 'test_table'}
        table_to_primary_key = {'test_table': 'id'}

        processor = ChangesProcessor(
            test_connection_info,
            mock_queue,
            id_to_table,
            table_to_primary_key,
            mock_sync_client
        )

        # Mock the process_change method
        processor.process_change = Mock()

        # Run begin_read
        processor.begin_read(worker_id=0)

        # Verify database connection was established
        mock_fdb_connect.assert_called_once_with(
            dsn=test_connection_info.db_path,
            user=test_connection_info.db_user,
            password=test_connection_info.db_password
        )

        # Verify change was processed
        processor.process_change.assert_called_once_with(mock_conn, test_change, 0)

        # Verify change was marked as processed
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called()

        # Verify connection was closed
        mock_conn.close.assert_called_once()
    
    @patch('handlers.changes_processor.fdb.connect')
    def test_begin_read_with_exception(self, mock_fdb_connect, test_connection_info, mock_queue, mock_sync_client):
        """Test begin_read method with exception handling."""
        # Setup mocks
        mock_conn = Mock()
        mock_fdb_connect.return_value = mock_conn

        # Mock queue to raise an exception
        mock_queue.get.side_effect = Exception("Test error")

        id_to_table = {1: 'test_table'}
        table_to_primary_key = {'test_table': 'id'}

        processor = ChangesProcessor(
            test_connection_info,
            mock_queue,
            id_to_table,
            table_to_primary_key,
            mock_sync_client
        )

        # Run begin_read - should handle exception gracefully
        processor.begin_read(worker_id=0)

        # Verify connection was still closed
        mock_conn.close.assert_called_once()
    
    @patch('handlers.changes_processor.BaseTableHandler')
    def test_process_change(self, mock_handler_class, test_connection_info, mock_queue, mock_sync_client):
        """Test process_change method."""
        mock_conn = Mock()
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler

        test_change = Change(
            log_id=1,
            pk_val=123,
            table_id=1,
            mutation="INSERT",
            occured_at=datetime.now(),
            processed=False
        )

        id_to_table = {1: 'test_table'}
        table_to_primary_key = {'test_table': 'id'}

        processor = ChangesProcessor(
            test_connection_info,
            mock_queue,
            id_to_table,
            table_to_primary_key,
            mock_sync_client
        )

        processor.process_change(mock_conn, test_change, worker_id=0)

        # Verify handler was created with correct parameters
        mock_handler_class.assert_called_once_with(mock_conn, 'test_table', 'id', mock_sync_client)

        # Verify mutation was handled
        mock_handler.handle_mutation.assert_called_once_with(test_change, 0)
