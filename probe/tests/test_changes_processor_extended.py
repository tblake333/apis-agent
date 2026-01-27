"""
Extended tests for ChangesProcessor class.

This module covers:
- Sentinel handling
- Multi-worker processing
- Worker isolation
- Duplicate prevention
"""
import pytest
import time
import threading
from queue import Queue
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from concurrent.futures import ThreadPoolExecutor

from handlers.changes_processor import ChangesProcessor
from models.change import Change
from models.connection_info import ConnectionInfo


@pytest.fixture
def mock_conn_info():
    """Create a mock connection info for unit tests."""
    return ConnectionInfo(
        db_path='/tmp/test.fdb',
        db_user='SYSDBA',
        db_password='masterkey'
    )


class TestSentinelHandling:
    """Test cases for sentinel value handling."""

    @pytest.mark.unit
    @patch('handlers.changes_processor.fdb.connect')
    def test_begin_read_sentinel_stops_worker(self, mock_connect, mock_conn_info, mock_sync_client):
        """Test that None sentinel stops the worker."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        output_queue = Queue()
        # Only put sentinel
        output_queue.put(None)

        processor = ChangesProcessor(
            conn_info=mock_conn_info,
            intake=output_queue,
            id_to_table={1: 'TEST'},
            table_primary_keys={'TEST': 'ID'},
            sync_client=mock_sync_client
        )

        # Should exit immediately
        processor.begin_read(worker_id=0)

        mock_conn.close.assert_called_once()

    @pytest.mark.unit
    @patch('handlers.changes_processor.fdb.connect')
    def test_sentinel_requeued_for_other_workers(self, mock_connect, mock_conn_info, mock_sync_client):
        """Test that sentinel is requeued for other workers."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        output_queue = Queue()
        output_queue.put(None)

        processor = ChangesProcessor(
            conn_info=mock_conn_info,
            intake=output_queue,
            id_to_table={1: 'TEST'},
            table_primary_keys={'TEST': 'ID'},
            sync_client=mock_sync_client
        )

        processor.begin_read(worker_id=0)

        # Sentinel should be back in queue
        assert not output_queue.empty()
        assert output_queue.get() is None


class TestMultipleWorkers:
    """Test cases for multiple worker processing."""

    @pytest.mark.concurrency
    @patch('handlers.changes_processor.fdb.connect')
    def test_multiple_workers_process_queue(self, mock_connect, mock_conn_info, mock_sync_client):
        """Test that 4 workers process 100 changes correctly."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        output_queue = Queue()
        processed = []
        lock = threading.Lock()

        def track_processing(change, worker_id):
            with lock:
                processed.append((change.log_id, worker_id))

        # Create 100 changes
        for i in range(100):
            change = Change(
                log_id=i,
                pk_val=i,
                table_id=1,
                mutation='INSERT',
                occured_at=datetime.now(),
                processed=False
            )
            output_queue.put(change)

        # Add sentinels for 4 workers
        for _ in range(4):
            output_queue.put(None)

        processor = ChangesProcessor(
            conn_info=mock_conn_info,
            intake=output_queue,
            id_to_table={1: 'TEST'},
            table_primary_keys={'TEST': 'ID'},
            sync_client=mock_sync_client
        )

        # Patch process_change to track
        def tracking_process(conn, change, worker_id):
            track_processing(change, worker_id)

        processor.process_change = tracking_process

        # Start 4 workers
        threads = []
        for i in range(4):
            t = threading.Thread(target=processor.begin_read, args=(i,))
            threads.append(t)
            t.start()

        # Wait for completion
        for t in threads:
            t.join(timeout=10)

        # All 100 should be processed
        assert len(processed) == 100

    @pytest.mark.concurrency
    @patch('handlers.changes_processor.fdb.connect')
    def test_worker_count_scaling(self, mock_connect, mock_conn_info, mock_sync_client):
        """Test processing with different worker counts."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        for num_workers in [1, 2, 4, 8]:
            output_queue = Queue()
            processed_count = {'value': 0}
            lock = threading.Lock()

            # Add 50 changes
            for i in range(50):
                change = Change(i, i, 1, 'INSERT', datetime.now(), False)
                output_queue.put(change)

            # Add sentinels
            for _ in range(num_workers):
                output_queue.put(None)

            processor = ChangesProcessor(
                conn_info=mock_conn_info,
                intake=output_queue,
                id_to_table={1: 'TEST'},
                table_primary_keys={'TEST': 'ID'},
                sync_client=mock_sync_client
            )

            def counting_process(conn, change, worker_id):
                with lock:
                    processed_count['value'] += 1

            processor.process_change = counting_process

            threads = []
            for i in range(num_workers):
                t = threading.Thread(target=processor.begin_read, args=(i,))
                threads.append(t)
                t.start()

            for t in threads:
                t.join(timeout=10)

            assert processed_count['value'] == 50, f"Failed with {num_workers} workers"


class TestWorkerIsolation:
    """Test cases for worker failure isolation."""

    @pytest.mark.unit
    @patch('handlers.changes_processor.fdb.connect')
    def test_worker_failure_isolated(self, mock_connect, mock_conn_info, mock_sync_client):
        """Test that one worker's crash doesn't affect others."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        output_queue = Queue()
        successful_processes = []
        lock = threading.Lock()

        # Add changes
        for i in range(10):
            change = Change(i, i, 1, 'INSERT', datetime.now(), False)
            output_queue.put(change)

        # Add sentinels for 3 workers
        for _ in range(3):
            output_queue.put(None)

        processor = ChangesProcessor(
            conn_info=mock_conn_info,
            intake=output_queue,
            id_to_table={1: 'TEST'},
            table_primary_keys={'TEST': 'ID'},
            sync_client=mock_sync_client
        )

        call_count = {'value': 0}

        def sometimes_failing_process(conn, change, worker_id):
            with lock:
                call_count['value'] += 1
                # Worker 1 fails on its third call
                if worker_id == 1 and call_count['value'] == 3:
                    raise Exception("Simulated worker failure")
                successful_processes.append(change.log_id)

        processor.process_change = sometimes_failing_process

        threads = []
        for i in range(3):
            t = threading.Thread(target=processor.begin_read, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=10)

        # Some should have succeeded despite the failure
        assert len(successful_processes) > 0


class TestNoDuplicateProcessing:
    """Test cases for preventing duplicate processing."""

    @pytest.mark.concurrency
    @patch('handlers.changes_processor.fdb.connect')
    def test_no_duplicate_processing(self, mock_connect, mock_conn_info, mock_sync_client):
        """Test that each change is processed exactly once."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        output_queue = Queue()
        processed_ids = []
        lock = threading.Lock()

        # Add 50 unique changes
        for i in range(50):
            change = Change(i, i, 1, 'INSERT', datetime.now(), False)
            output_queue.put(change)

        # Add sentinels for 4 workers
        for _ in range(4):
            output_queue.put(None)

        processor = ChangesProcessor(
            conn_info=mock_conn_info,
            intake=output_queue,
            id_to_table={1: 'TEST'},
            table_primary_keys={'TEST': 'ID'},
            sync_client=mock_sync_client
        )

        def tracking_process(conn, change, worker_id):
            with lock:
                processed_ids.append(change.log_id)

        processor.process_change = tracking_process

        threads = []
        for i in range(4):
            t = threading.Thread(target=processor.begin_read, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=10)

        # Verify no duplicates
        assert len(processed_ids) == len(set(processed_ids)), "Duplicate processing detected"
        assert len(processed_ids) == 50, f"Expected 50, got {len(processed_ids)}"


class TestProcessChangeMethod:
    """Test cases for process_change method."""

    @pytest.mark.unit
    @patch('handlers.changes_processor.BaseTableHandler')
    def test_process_change_creates_handler(self, mock_handler_class, mock_conn_info, mock_sync_client):
        """Test that process_change creates a table handler."""
        mock_conn = MagicMock()
        mock_handler = MagicMock()
        mock_handler_class.return_value = mock_handler

        output_queue = Queue()
        processor = ChangesProcessor(
            conn_info=mock_conn_info,
            intake=output_queue,
            id_to_table={1: 'USERS', 2: 'ORDERS'},
            table_primary_keys={'USERS': 'USER_ID', 'ORDERS': 'ORDER_ID'},
            sync_client=mock_sync_client
        )

        change = Change(1, 100, 1, 'INSERT', datetime.now(), False)
        processor.process_change(mock_conn, change, worker_id=0)

        mock_handler_class.assert_called_once_with(
            mock_conn, 'USERS', 'USER_ID', mock_sync_client
        )
        mock_handler.handle_mutation.assert_called_once_with(change, 0)

    @pytest.mark.unit
    @patch('handlers.changes_processor.BaseTableHandler')
    def test_process_change_different_tables(self, mock_handler_class, mock_conn_info, mock_sync_client):
        """Test process_change with different table IDs."""
        mock_conn = MagicMock()
        mock_handler = MagicMock()
        mock_handler_class.return_value = mock_handler

        output_queue = Queue()
        processor = ChangesProcessor(
            conn_info=mock_conn_info,
            intake=output_queue,
            id_to_table={1: 'USERS', 2: 'ORDERS'},
            table_primary_keys={'USERS': 'USER_ID', 'ORDERS': 'ORDER_ID'},
            sync_client=mock_sync_client
        )

        # Process change for ORDERS table
        change = Change(2, 200, 2, 'UPDATE', datetime.now(), False)
        processor.process_change(mock_conn, change, worker_id=1)

        mock_handler_class.assert_called_with(
            mock_conn, 'ORDERS', 'ORDER_ID', mock_sync_client
        )
