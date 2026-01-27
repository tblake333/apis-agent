"""
End-to-end integration tests for the Firebird CDC application.

This module tests the full flow from database changes to cloud sync,
including offline buffering and recovery scenarios.
"""
import pytest
import time
import threading
from datetime import datetime
from queue import Queue
from unittest.mock import Mock, patch, MagicMock

from models.change import Change
from models.connection_info import ConnectionInfo
from handlers.changes_intake import ChangesIntake
from handlers.changes_processor import ChangesProcessor
from handlers.base_table_handler import BaseTableHandler
from sync.cloud_sync_client import CloudSyncClient
from sync.local_buffer import LocalBuffer


class TestFullFlowInsert:
    """Test INSERT flow from trigger to cloud sync."""

    @pytest.mark.integration
    def test_insert_flows_to_cloud_sync(self, changes_log_setup, test_connection_info, temp_fdb_file):
        """Test that INSERT changes flow through the entire system."""
        conn = changes_log_setup
        output_queue = Queue()

        # Create a mock sync client to capture sent data
        sent_inserts = []
        mock_sync_client = Mock()
        mock_sync_client.send_insert = Mock(side_effect=lambda **kwargs: sent_inserts.append(kwargs) or True)
        mock_sync_client.send_update = Mock(return_value=True)
        mock_sync_client.send_delete = Mock(return_value=True)

        # Simulate a change being logged (as would happen via trigger)
        conn.execute_immediate("""
            INSERT INTO CHANGES_LOG (LOG_ID, PK_VAL, TABLE_ID, MUTATION, OCCURRED_AT, PROCESSED)
            VALUES (1, 100, 1, 'INSERT', CURRENT_TIMESTAMP, 0)
        """)
        conn.commit()

        # Also insert the actual data that would be fetched
        conn.execute_immediate("""
            INSERT INTO TEST_DATA (ID, NAME, VALUE) VALUES (100, 'Test Item', 99.99)
        """)
        conn.commit()

        # Set up processor mappings
        id_to_table = {1: 'TEST_DATA'}
        table_primary_keys = {'TEST_DATA': 'ID'}

        # Create processor
        processor = ChangesProcessor(
            conn_info=test_connection_info,
            intake=output_queue,
            id_to_table=id_to_table,
            table_primary_keys=table_primary_keys,
            sync_client=mock_sync_client
        )

        # Manually fetch and queue the change
        cur = conn.cursor()
        cur.execute("SELECT * FROM CHANGES_LOG WHERE LOG_ID = 1")
        row = cur.fetchone()
        change = Change(*row)
        output_queue.put(change)
        output_queue.put(None)  # Sentinel to stop

        # Process the change
        processor.begin_read(worker_id=0)

        # Verify the insert was sent
        assert mock_sync_client.send_insert.called
        assert len(sent_inserts) == 1
        assert sent_inserts[0]['table'] == 'TEST_DATA'


class TestFullFlowUpdate:
    """Test UPDATE flow from trigger to cloud sync."""

    @pytest.mark.integration
    def test_update_flows_to_cloud_sync(self, changes_log_setup, test_connection_info):
        """Test that UPDATE changes flow through the entire system."""
        conn = changes_log_setup
        output_queue = Queue()

        sent_updates = []
        mock_sync_client = Mock()
        mock_sync_client.send_insert = Mock(return_value=True)
        mock_sync_client.send_update = Mock(side_effect=lambda **kwargs: sent_updates.append(kwargs) or True)
        mock_sync_client.send_delete = Mock(return_value=True)

        # Insert initial data
        conn.execute_immediate("""
            INSERT INTO TEST_DATA (ID, NAME, VALUE) VALUES (200, 'Original Name', 50.00)
        """)
        conn.commit()

        # Simulate UPDATE change log
        conn.execute_immediate("""
            INSERT INTO CHANGES_LOG (LOG_ID, PK_VAL, TABLE_ID, MUTATION, OCCURRED_AT, PROCESSED)
            VALUES (2, 200, 1, 'UPDATE', CURRENT_TIMESTAMP, 0)
        """)
        conn.commit()

        # Update the actual data
        conn.execute_immediate("""
            UPDATE TEST_DATA SET NAME = 'Updated Name', VALUE = 75.00 WHERE ID = 200
        """)
        conn.commit()

        id_to_table = {1: 'TEST_DATA'}
        table_primary_keys = {'TEST_DATA': 'ID'}

        processor = ChangesProcessor(
            conn_info=test_connection_info,
            intake=output_queue,
            id_to_table=id_to_table,
            table_primary_keys=table_primary_keys,
            sync_client=mock_sync_client
        )

        cur = conn.cursor()
        cur.execute("SELECT * FROM CHANGES_LOG WHERE LOG_ID = 2")
        row = cur.fetchone()
        change = Change(*row)
        output_queue.put(change)
        output_queue.put(None)

        processor.begin_read(worker_id=0)

        assert mock_sync_client.send_update.called
        assert len(sent_updates) == 1
        assert sent_updates[0]['table'] == 'TEST_DATA'


class TestFullFlowDelete:
    """Test DELETE flow from trigger to cloud sync."""

    @pytest.mark.integration
    def test_delete_flows_to_cloud_sync(self, changes_log_setup, test_connection_info):
        """Test that DELETE changes flow through the entire system."""
        conn = changes_log_setup
        output_queue = Queue()

        sent_deletes = []
        mock_sync_client = Mock()
        mock_sync_client.send_insert = Mock(return_value=True)
        mock_sync_client.send_update = Mock(return_value=True)
        mock_sync_client.send_delete = Mock(side_effect=lambda **kwargs: sent_deletes.append(kwargs) or True)

        # Insert and then delete
        conn.execute_immediate("""
            INSERT INTO TEST_DATA (ID, NAME, VALUE) VALUES (300, 'To Delete', 25.00)
        """)
        conn.commit()

        # Simulate DELETE change log
        conn.execute_immediate("""
            INSERT INTO CHANGES_LOG (LOG_ID, PK_VAL, TABLE_ID, MUTATION, OCCURRED_AT, PROCESSED)
            VALUES (3, 300, 1, 'DELETE', CURRENT_TIMESTAMP, 0)
        """)
        conn.commit()

        # Delete the actual data
        conn.execute_immediate("DELETE FROM TEST_DATA WHERE ID = 300")
        conn.commit()

        id_to_table = {1: 'TEST_DATA'}
        table_primary_keys = {'TEST_DATA': 'ID'}

        processor = ChangesProcessor(
            conn_info=test_connection_info,
            intake=output_queue,
            id_to_table=id_to_table,
            table_primary_keys=table_primary_keys,
            sync_client=mock_sync_client
        )

        cur = conn.cursor()
        cur.execute("SELECT * FROM CHANGES_LOG WHERE LOG_ID = 3")
        row = cur.fetchone()
        change = Change(*row)
        output_queue.put(change)
        output_queue.put(None)

        processor.begin_read(worker_id=0)

        assert mock_sync_client.send_delete.called
        assert len(sent_deletes) == 1
        assert sent_deletes[0]['table'] == 'TEST_DATA'
        assert sent_deletes[0]['primary_key'] == 'ID'
        assert sent_deletes[0]['value'] == 300


class TestMultipleChangesBatch:
    """Test processing multiple changes in a batch."""

    @pytest.mark.integration
    def test_multiple_changes_batch(self, changes_log_setup, test_connection_info):
        """Test processing 50 rapid changes."""
        conn = changes_log_setup
        output_queue = Queue()
        processed_count = {'insert': 0, 'update': 0, 'delete': 0}

        def count_insert(**kwargs):
            processed_count['insert'] += 1
            return True

        def count_update(**kwargs):
            processed_count['update'] += 1
            return True

        def count_delete(**kwargs):
            processed_count['delete'] += 1
            return True

        mock_sync_client = Mock()
        mock_sync_client.send_insert = Mock(side_effect=count_insert)
        mock_sync_client.send_update = Mock(side_effect=count_update)
        mock_sync_client.send_delete = Mock(side_effect=count_delete)

        # Create 50 changes (mix of types)
        for i in range(50):
            conn.execute_immediate(f"""
                INSERT INTO TEST_DATA (ID, NAME, VALUE) VALUES ({1000 + i}, 'Item {i}', {i * 10.0})
            """)
            conn.commit()

            mutation = ['INSERT', 'UPDATE', 'DELETE'][i % 3]
            conn.execute_immediate(f"""
                INSERT INTO CHANGES_LOG (LOG_ID, PK_VAL, TABLE_ID, MUTATION, OCCURRED_AT, PROCESSED)
                VALUES ({100 + i}, {1000 + i}, 1, '{mutation}', CURRENT_TIMESTAMP, 0)
            """)
            conn.commit()

        id_to_table = {1: 'TEST_DATA'}
        table_primary_keys = {'TEST_DATA': 'ID'}

        processor = ChangesProcessor(
            conn_info=test_connection_info,
            intake=output_queue,
            id_to_table=id_to_table,
            table_primary_keys=table_primary_keys,
            sync_client=mock_sync_client
        )

        # Queue all changes
        cur = conn.cursor()
        cur.execute("SELECT * FROM CHANGES_LOG WHERE LOG_ID >= 100 ORDER BY LOG_ID")
        for row in cur.fetchall():
            change = Change(*row)
            output_queue.put(change)
        output_queue.put(None)

        processor.begin_read(worker_id=0)

        # Verify all changes were processed
        total_processed = processed_count['insert'] + processed_count['update'] + processed_count['delete']
        assert total_processed == 50


class TestOfflineBufferingAndRetry:
    """Test offline buffering when cloud is unavailable."""

    @pytest.mark.integration
    def test_offline_buffering_and_retry(self):
        """Test that changes are buffered when cloud is down, then sent when up."""
        buffer = LocalBuffer(":memory:")

        # Create client with failing sends
        client = CloudSyncClient(
            endpoint="http://localhost:9999/nonexistent",
            buffer_path=":memory:",
            enable_background_retry=False
        )
        # Share the buffer for verification
        client.buffer = buffer

        # Attempt to send while "offline"
        with patch.object(client, '_send_request', return_value=False):
            result1 = client.send({"type": "INSERT", "id": 1})
            result2 = client.send({"type": "UPDATE", "id": 2})
            result3 = client.send({"type": "DELETE", "id": 3})

        # All should have failed and been buffered
        assert result1 is False
        assert result2 is False
        assert result3 is False
        assert buffer.count_pending() == 3

        # Now "restore" connectivity and flush
        with patch.object(client, '_send_request', return_value=True):
            sent = client.flush_buffer()

        assert sent == 3
        assert buffer.count_pending() == 0
        client.close()

    @pytest.mark.integration
    def test_partial_retry_success(self):
        """Test that partial retry succeeds leave remaining items buffered."""
        client = CloudSyncClient(
            endpoint="http://localhost:8080/api",
            buffer_path=":memory:",
            enable_background_retry=False
        )

        # Buffer some changes
        client.buffer.add({"type": "INSERT", "id": 1})
        client.buffer.add({"type": "INSERT", "id": 2})
        client.buffer.add({"type": "INSERT", "id": 3})
        client.buffer.add({"type": "INSERT", "id": 4})
        client.buffer.add({"type": "INSERT", "id": 5})

        # First three succeed, last two fail
        with patch.object(client, '_send_request', side_effect=[True, True, True, False, False]):
            sent = client.flush_buffer()

        assert sent == 3
        assert client.buffer.count_pending() == 2
        client.close()


class TestApplicationRestartRecovery:
    """Test recovery of buffered changes after application restart."""

    @pytest.mark.integration
    def test_application_restart_recovery(self, temp_sqlite_buffer):
        """Test that buffered changes survive application restart."""
        # First "session" - buffer some changes
        client1 = CloudSyncClient(
            endpoint="http://localhost:8080/api",
            buffer_path=temp_sqlite_buffer,
            enable_background_retry=False
        )

        # Fail to send some changes
        with patch.object(client1, '_send_request', return_value=False):
            client1.send({"type": "INSERT", "id": 1, "data": "test1"})
            client1.send({"type": "UPDATE", "id": 2, "data": "test2"})
            client1.send({"type": "DELETE", "id": 3, "data": "test3"})

        pending_before = client1.buffer.count_pending()
        client1.close()

        # Simulate "restart" - new client instance
        client2 = CloudSyncClient(
            endpoint="http://localhost:8080/api",
            buffer_path=temp_sqlite_buffer,
            enable_background_retry=False
        )

        # Verify buffered data survived
        assert client2.buffer.count_pending() == pending_before

        # Now send successfully
        sent_payloads = []

        def capture_send(payload):
            sent_payloads.append(payload)
            return True

        with patch.object(client2, '_send_request', side_effect=capture_send):
            sent = client2.flush_buffer()

        assert sent == 3
        assert client2.buffer.count_pending() == 0

        # Verify payloads were preserved
        types = [p['type'] for p in sent_payloads]
        assert 'INSERT' in types
        assert 'UPDATE' in types
        assert 'DELETE' in types

        client2.close()


class TestBaseTableHandlerIntegration:
    """Integration tests for BaseTableHandler with real database."""

    @pytest.mark.integration
    def test_handle_insert_fetches_row(self, changes_log_setup):
        """Test that INSERT handler fetches the correct row."""
        conn = changes_log_setup

        # Insert test data
        conn.execute_immediate("""
            INSERT INTO TEST_DATA (ID, NAME, VALUE) VALUES (500, 'Handler Test', 123.45)
        """)
        conn.commit()

        captured_data = []
        mock_sync_client = Mock()
        mock_sync_client.send_insert = Mock(side_effect=lambda **kwargs: captured_data.append(kwargs) or True)

        handler = BaseTableHandler(conn, 'TEST_DATA', 'ID', mock_sync_client)

        change = Change(
            log_id=10,
            pk_val=500,
            table_id=1,
            mutation='INSERT',
            occured_at=datetime.now(),
            processed=False
        )

        handler.handle_insert(change)

        assert len(captured_data) == 1
        assert captured_data[0]['table'] == 'TEST_DATA'
        row_data = captured_data[0]['row_data']
        assert row_data[0] == 500  # ID
        assert row_data[1] == 'Handler Test'  # NAME

    @pytest.mark.integration
    def test_handle_mutation_routes_correctly(self, changes_log_setup):
        """Test that handle_mutation routes to correct handler."""
        conn = changes_log_setup

        conn.execute_immediate("""
            INSERT INTO TEST_DATA (ID, NAME, VALUE) VALUES (600, 'Route Test', 50.00)
        """)
        conn.commit()

        mock_sync_client = Mock()
        mock_sync_client.send_insert = Mock(return_value=True)
        mock_sync_client.send_update = Mock(return_value=True)
        mock_sync_client.send_delete = Mock(return_value=True)

        handler = BaseTableHandler(conn, 'TEST_DATA', 'ID', mock_sync_client)

        # Test INSERT routing
        insert_change = Change(11, 600, 1, 'INSERT', datetime.now(), False)
        handler.handle_mutation(insert_change, worker_id=0)
        assert mock_sync_client.send_insert.called

        # Test UPDATE routing
        update_change = Change(12, 600, 1, 'UPDATE', datetime.now(), False)
        handler.handle_mutation(update_change, worker_id=0)
        assert mock_sync_client.send_update.called

        # Test DELETE routing
        delete_change = Change(13, 600, 1, 'DELETE', datetime.now(), False)
        handler.handle_mutation(delete_change, worker_id=0)
        assert mock_sync_client.send_delete.called


class TestConcurrentProcessing:
    """Test concurrent processing with multiple workers."""

    @pytest.mark.concurrency
    def test_multiple_workers_no_duplicate_processing(self, changes_log_setup, test_connection_info):
        """Test that multiple workers don't process the same change twice."""
        conn = changes_log_setup
        output_queue = Queue()
        processed_ids = []
        lock = threading.Lock()

        def track_insert(**kwargs):
            # Extract ID from row_data
            if kwargs.get('row_data'):
                with lock:
                    processed_ids.append(kwargs['row_data'][0])
            return True

        mock_sync_client = Mock()
        mock_sync_client.send_insert = Mock(side_effect=track_insert)
        mock_sync_client.send_update = Mock(return_value=True)
        mock_sync_client.send_delete = Mock(return_value=True)

        # Create test data
        for i in range(20):
            conn.execute_immediate(f"""
                INSERT INTO TEST_DATA (ID, NAME, VALUE) VALUES ({2000 + i}, 'Concurrent Test {i}', {i}.0)
            """)
            conn.commit()
            conn.execute_immediate(f"""
                INSERT INTO CHANGES_LOG (LOG_ID, PK_VAL, TABLE_ID, MUTATION, OCCURRED_AT, PROCESSED)
                VALUES ({200 + i}, {2000 + i}, 1, 'INSERT', CURRENT_TIMESTAMP, 0)
            """)
            conn.commit()

        id_to_table = {1: 'TEST_DATA'}
        table_primary_keys = {'TEST_DATA': 'ID'}

        # Create multiple processors (simulating workers)
        processors = []
        for _ in range(4):
            processor = ChangesProcessor(
                conn_info=test_connection_info,
                intake=output_queue,
                id_to_table=id_to_table,
                table_primary_keys=table_primary_keys,
                sync_client=mock_sync_client
            )
            processors.append(processor)

        # Queue all changes
        cur = conn.cursor()
        cur.execute("SELECT * FROM CHANGES_LOG WHERE LOG_ID >= 200 ORDER BY LOG_ID")
        for row in cur.fetchall():
            change = Change(*row)
            output_queue.put(change)

        # Add sentinels for all workers
        for _ in range(4):
            output_queue.put(None)

        # Start workers
        threads = []
        for i, processor in enumerate(processors):
            t = threading.Thread(target=processor.begin_read, args=(i,))
            threads.append(t)
            t.start()

        # Wait for completion
        for t in threads:
            t.join(timeout=10)

        # Verify no duplicates
        assert len(processed_ids) == len(set(processed_ids)), "Duplicate processing detected"
