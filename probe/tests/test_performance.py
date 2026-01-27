"""
Performance and stress tests for the Firebird CDC application.

This module covers:
- Throughput testing
- Burst load handling
- Worker scaling
- Memory stress testing
"""
import pytest
import time
import threading
from queue import Queue
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch

from handlers.changes_processor import ChangesProcessor
from handlers.changes_intake import ChangesIntake
from sync.local_buffer import LocalBuffer
from sync.cloud_sync_client import CloudSyncClient
from models.change import Change


class TestIntakeThroughput:
    """Performance tests for ChangesIntake throughput."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_intake_throughput_1000_changes(self, changes_log_setup, test_connection_info):
        """Test processing 1000 changes through intake."""
        conn = changes_log_setup
        output_queue = Queue()

        # Insert 1000 changes
        start_insert = time.time()
        for i in range(1000):
            conn.execute_immediate(f"""
                INSERT INTO CHANGES_LOG (LOG_ID, PK_VAL, TABLE_ID, MUTATION, OCCURRED_AT, PROCESSED)
                VALUES ({i + 1}, {i + 1}, 1, 'INSERT', CURRENT_TIMESTAMP, 0)
            """)
            if i % 100 == 0:
                conn.commit()
        conn.commit()
        insert_time = time.time() - start_insert

        print(f"\n1000 records inserted in {insert_time:.2f}s")

        # Process through intake
        intake = ChangesIntake(test_connection_info, 1, output_queue)

        start_process = time.time()
        intake.start()

        # Wait for processing
        timeout = 30
        start_wait = time.time()
        while output_queue.qsize() < 1000 and (time.time() - start_wait) < timeout:
            time.sleep(0.1)

        intake.stop()
        intake.join(timeout=5)

        process_time = time.time() - start_process
        processed = output_queue.qsize()

        print(f"Processed {processed} changes in {process_time:.2f}s")
        print(f"Throughput: {processed / process_time:.0f} changes/second")

        assert processed >= 900, f"Expected at least 900 processed, got {processed}"


class TestBufferWriteThroughput:
    """Performance tests for LocalBuffer write throughput."""

    @pytest.mark.slow
    def test_buffer_write_throughput(self):
        """Test LocalBuffer write throughput."""
        buffer = LocalBuffer(":memory:")

        payload = {
            "type": "INSERT",
            "table": "TEST_TABLE",
            "row_data": (1, "Test Name", "test@example.com", 123.45),
            "timestamp": time.time()
        }

        num_writes = 10000
        start = time.time()

        for i in range(num_writes):
            buffer.add(payload)

        elapsed = time.time() - start

        print(f"\n{num_writes} writes in {elapsed:.2f}s")
        print(f"Write throughput: {num_writes / elapsed:.0f} writes/second")

        assert buffer.count_pending() == num_writes
        assert elapsed < 10, f"Write throughput too slow: {elapsed:.2f}s for {num_writes} writes"

        buffer.close()

    @pytest.mark.slow
    def test_buffer_read_throughput(self):
        """Test LocalBuffer read throughput."""
        buffer = LocalBuffer(":memory:")

        # Pre-populate
        for i in range(5000):
            buffer.add({"index": i})

        num_reads = 100
        batch_size = 100
        start = time.time()

        for _ in range(num_reads):
            records = buffer.get_pending(limit=batch_size)

        elapsed = time.time() - start

        print(f"\n{num_reads} batch reads in {elapsed:.2f}s")
        print(f"Read throughput: {num_reads / elapsed:.0f} batches/second")

        assert elapsed < 5, f"Read throughput too slow: {elapsed:.2f}s"

        buffer.close()


class TestBurstLoad:
    """Test handling of burst loads."""

    @pytest.mark.slow
    @pytest.mark.concurrency
    def test_burst_load_1000_changes_1_second(self):
        """Test handling 1000 changes arriving in ~1 second."""
        buffer = LocalBuffer(":memory:")
        client = CloudSyncClient(
            buffer_path=":memory:",
            enable_background_retry=False
        )
        client.buffer = buffer

        # Mock successful sends
        send_count = {'value': 0}
        lock = threading.Lock()

        def mock_send(payload):
            with lock:
                send_count['value'] += 1
            time.sleep(0.0001)  # Tiny delay to simulate network
            return True

        client._send_request = mock_send

        # Burst send
        start = time.time()

        threads = []
        for batch in range(10):
            def send_batch(batch_id):
                for i in range(100):
                    client.send({
                        "type": "INSERT",
                        "batch": batch_id,
                        "index": i,
                        "timestamp": time.time()
                    })

            t = threading.Thread(target=send_batch, args=(batch,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        elapsed = time.time() - start

        print(f"\n1000 sends completed in {elapsed:.2f}s")
        print(f"Send count: {send_count['value']}")
        print(f"Throughput: {send_count['value'] / elapsed:.0f} sends/second")

        # Most should have been sent (some may be buffered)
        assert send_count['value'] >= 900, f"Expected at least 900 sent, got {send_count['value']}"

        client.close()


class TestWorkerCountScaling:
    """Test scaling with different worker counts."""

    @pytest.mark.slow
    @pytest.mark.concurrency
    @patch('handlers.changes_processor.fdb.connect')
    def test_worker_count_scaling(self, mock_connect, test_connection_info):
        """Compare processing time with 1, 2, 4, 8 workers."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        num_changes = 1000
        results = {}

        for num_workers in [1, 2, 4, 8]:
            output_queue = Queue()
            processed_count = {'value': 0}
            lock = threading.Lock()

            # Add changes
            for i in range(num_changes):
                change = Change(i, i, 1, 'INSERT', datetime.now(), False)
                output_queue.put(change)

            # Add sentinels
            for _ in range(num_workers):
                output_queue.put(None)

            mock_sync_client = Mock()
            mock_sync_client.send_insert = Mock(return_value=True)
            mock_sync_client.send_update = Mock(return_value=True)
            mock_sync_client.send_delete = Mock(return_value=True)

            processor = ChangesProcessor(
                conn_info=test_connection_info,
                intake=output_queue,
                id_to_table={1: 'TEST'},
                table_primary_keys={'TEST': 'ID'},
                sync_client=mock_sync_client
            )

            def counting_process(conn, change, worker_id):
                with lock:
                    processed_count['value'] += 1
                time.sleep(0.001)  # Simulate some work

            processor.process_change = counting_process

            start = time.time()

            threads = []
            for i in range(num_workers):
                t = threading.Thread(target=processor.begin_read, args=(i,))
                threads.append(t)
                t.start()

            for t in threads:
                t.join(timeout=60)

            elapsed = time.time() - start
            results[num_workers] = {
                'time': elapsed,
                'processed': processed_count['value'],
                'throughput': processed_count['value'] / elapsed
            }

            print(f"\n{num_workers} workers: {elapsed:.2f}s, {processed_count['value']} processed, "
                  f"{processed_count['value'] / elapsed:.0f} changes/sec")

        # More workers should generally be faster (up to a point)
        # With simulated work, this should show some scaling benefit
        print("\nScaling results:")
        for workers, data in results.items():
            print(f"  {workers} workers: {data['throughput']:.0f} changes/sec")


class TestMemoryStress:
    """Memory stress tests."""

    @pytest.mark.slow
    def test_large_payload_memory(self):
        """Test memory handling with large payloads."""
        buffer = LocalBuffer(":memory:")

        # Create moderately large payloads
        large_data = "x" * 10000  # 10KB

        for i in range(100):
            buffer.add({
                "index": i,
                "data": large_data,
                "nested": {"more_data": large_data[:1000]}
            })

        assert buffer.count_pending() == 100

        # Read them back
        pending = buffer.get_pending(limit=100)
        assert len(pending) == 100

        buffer.close()

    @pytest.mark.slow
    def test_sustained_load(self):
        """Test sustained load over time."""
        buffer = LocalBuffer(":memory:")
        client = CloudSyncClient(
            buffer_path=":memory:",
            enable_background_retry=False
        )
        client.buffer = buffer

        # Simulate successful sends
        client._send_request = Mock(return_value=True)

        duration = 5  # seconds
        start = time.time()
        send_count = 0

        while time.time() - start < duration:
            client.send({
                "type": "INSERT",
                "data": {"value": send_count},
                "timestamp": time.time()
            })
            send_count += 1
            time.sleep(0.001)  # Rate limit a bit

        elapsed = time.time() - start

        print(f"\nSustained load: {send_count} sends in {elapsed:.2f}s")
        print(f"Average throughput: {send_count / elapsed:.0f} sends/second")

        client.close()


class TestConcurrentBufferAccess:
    """Test concurrent buffer access performance."""

    @pytest.mark.slow
    @pytest.mark.concurrency
    def test_concurrent_buffer_performance(self):
        """Test buffer performance under concurrent access."""
        buffer = LocalBuffer(":memory:")
        num_threads = 8
        operations_per_thread = 500
        results = {'writes': 0, 'reads': 0, 'errors': 0}
        lock = threading.Lock()

        def writer(thread_id):
            for i in range(operations_per_thread):
                try:
                    buffer.add({"thread": thread_id, "index": i})
                    with lock:
                        results['writes'] += 1
                except Exception:
                    with lock:
                        results['errors'] += 1

        def reader():
            for _ in range(operations_per_thread):
                try:
                    buffer.get_pending(limit=10)
                    with lock:
                        results['reads'] += 1
                except Exception:
                    with lock:
                        results['errors'] += 1

        start = time.time()

        threads = []
        for i in range(num_threads // 2):
            threads.append(threading.Thread(target=writer, args=(i,)))
            threads.append(threading.Thread(target=reader))

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        elapsed = time.time() - start

        print(f"\nConcurrent access: {results['writes']} writes, {results['reads']} reads, "
              f"{results['errors']} errors in {elapsed:.2f}s")
        print(f"Total ops: {(results['writes'] + results['reads']) / elapsed:.0f} ops/second")

        assert results['errors'] == 0, f"Errors occurred: {results['errors']}"

        buffer.close()


class TestProcessorThroughput:
    """Test ChangesProcessor throughput."""

    @pytest.mark.slow
    @pytest.mark.concurrency
    @patch('handlers.changes_processor.fdb.connect')
    def test_processor_throughput(self, mock_connect, test_connection_info):
        """Test processor throughput with mock database."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        output_queue = Queue()
        num_changes = 5000

        mock_sync_client = Mock()
        mock_sync_client.send_insert = Mock(return_value=True)

        processor = ChangesProcessor(
            conn_info=test_connection_info,
            intake=output_queue,
            id_to_table={1: 'TEST'},
            table_primary_keys={'TEST': 'ID'},
            sync_client=mock_sync_client
        )

        # Add changes
        for i in range(num_changes):
            change = Change(i, i, 1, 'INSERT', datetime.now(), False)
            output_queue.put(change)

        # Add sentinels for 4 workers
        for _ in range(4):
            output_queue.put(None)

        start = time.time()

        threads = []
        for i in range(4):
            t = threading.Thread(target=processor.begin_read, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=60)

        elapsed = time.time() - start

        call_count = mock_sync_client.send_insert.call_count
        print(f"\n{call_count} changes processed in {elapsed:.2f}s")
        print(f"Throughput: {call_count / elapsed:.0f} changes/second")

        assert call_count == num_changes
