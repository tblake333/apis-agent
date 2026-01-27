"""
Tests for LocalBuffer class.

This module covers:
- Basic CRUD operations
- Concurrency and thread safety
- Edge cases with large payloads and unicode
"""
import pytest
import tempfile
import os
import threading
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import patch

from sync.local_buffer import LocalBuffer


class TestLocalBufferInit:
    """Test cases for LocalBuffer initialization."""

    @pytest.mark.unit
    def test_init_default_path(self):
        """Test initialization with default path."""
        buffer = LocalBuffer()
        assert buffer.db_path == LocalBuffer.DEFAULT_DB_PATH
        buffer.close()
        # Cleanup
        if os.path.exists(LocalBuffer.DEFAULT_DB_PATH):
            os.unlink(LocalBuffer.DEFAULT_DB_PATH)

    @pytest.mark.unit
    def test_init_memory_database(self):
        """Test initialization with in-memory database."""
        buffer = LocalBuffer(":memory:")
        assert buffer.db_path == ":memory:"
        assert buffer._is_memory is True
        buffer.close()

    @pytest.mark.unit
    def test_init_custom_path(self):
        """Test initialization with custom path."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            temp_path = f.name

        try:
            buffer = LocalBuffer(temp_path)
            assert buffer.db_path == temp_path
            assert buffer._is_memory is False
            buffer.close()
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    @pytest.mark.unit
    def test_init_creates_schema(self):
        """Test that initialization creates the database schema."""
        buffer = LocalBuffer(":memory:")

        # Verify table exists by adding a record
        record_id = buffer.add({"test": "data"})
        assert record_id == 1
        buffer.close()


class TestLocalBufferAdd:
    """Test cases for LocalBuffer.add method."""

    @pytest.fixture
    def buffer(self):
        """Create an in-memory buffer for testing."""
        buf = LocalBuffer(":memory:")
        yield buf
        buf.close()

    @pytest.mark.unit
    def test_add_single_record(self, buffer):
        """Test adding a single record."""
        payload = {"type": "INSERT", "table": "users", "data": {"id": 1}}
        record_id = buffer.add(payload)

        assert record_id == 1
        assert buffer.count_pending() == 1

    @pytest.mark.unit
    def test_add_multiple_records(self, buffer):
        """Test adding multiple records."""
        for i in range(5):
            buffer.add({"index": i})

        assert buffer.count_pending() == 5

    @pytest.mark.unit
    def test_add_with_error_message(self, buffer):
        """Test adding record with error message."""
        payload = {"type": "INSERT"}
        record_id = buffer.add(payload, error="Connection timeout")

        pending = buffer.get_pending()
        assert len(pending) == 1
        assert pending[0]['last_error'] == "Connection timeout"

    @pytest.mark.unit
    def test_add_returns_incrementing_ids(self, buffer):
        """Test that add returns incrementing IDs."""
        id1 = buffer.add({"test": 1})
        id2 = buffer.add({"test": 2})
        id3 = buffer.add({"test": 3})

        assert id1 == 1
        assert id2 == 2
        assert id3 == 3

    @pytest.mark.unit
    def test_add_complex_payload(self, buffer):
        """Test adding complex nested payload."""
        payload = {
            "type": "UPDATE",
            "table": "orders",
            "data": {
                "id": 123,
                "items": [{"sku": "ABC", "qty": 2}, {"sku": "XYZ", "qty": 1}],
                "metadata": {"source": "api", "version": 2}
            },
            "timestamp": 1704067200.0
        }
        record_id = buffer.add(payload)

        pending = buffer.get_pending()
        assert len(pending) == 1
        assert pending[0]['payload'] == payload


class TestLocalBufferGetPending:
    """Test cases for LocalBuffer.get_pending method."""

    @pytest.fixture
    def buffer(self):
        """Create an in-memory buffer for testing."""
        buf = LocalBuffer(":memory:")
        yield buf
        buf.close()

    @pytest.mark.unit
    def test_get_pending_empty(self, buffer):
        """Test getting pending from empty buffer."""
        pending = buffer.get_pending()
        assert pending == []

    @pytest.mark.unit
    def test_get_pending_fifo_order(self, buffer):
        """Test that pending records are returned in FIFO order."""
        buffer.add({"order": 1})
        time.sleep(0.01)  # Ensure different timestamps
        buffer.add({"order": 2})
        time.sleep(0.01)
        buffer.add({"order": 3})

        pending = buffer.get_pending()
        assert pending[0]['payload']['order'] == 1
        assert pending[1]['payload']['order'] == 2
        assert pending[2]['payload']['order'] == 3

    @pytest.mark.unit
    def test_get_pending_with_limit(self, buffer):
        """Test limiting number of pending records returned."""
        for i in range(10):
            buffer.add({"index": i})

        pending = buffer.get_pending(limit=5)
        assert len(pending) == 5

    @pytest.mark.unit
    def test_get_pending_limit_exceeds_count(self, buffer):
        """Test limit greater than actual count."""
        buffer.add({"test": 1})
        buffer.add({"test": 2})

        pending = buffer.get_pending(limit=100)
        assert len(pending) == 2

    @pytest.mark.unit
    def test_get_pending_payload_deserialization(self, buffer):
        """Test that payloads are properly deserialized from JSON."""
        original = {"type": "DELETE", "id": 456, "nested": {"key": "value"}}
        buffer.add(original)

        pending = buffer.get_pending()
        assert pending[0]['payload'] == original
        assert isinstance(pending[0]['payload'], dict)

    @pytest.mark.unit
    def test_get_pending_includes_metadata(self, buffer):
        """Test that pending records include all metadata fields."""
        buffer.add({"test": "data"}, error="Initial error")

        pending = buffer.get_pending()
        record = pending[0]

        assert 'id' in record
        assert 'payload' in record
        assert 'created_at' in record
        assert 'retry_count' in record
        assert 'last_error' in record


class TestLocalBufferMarkSent:
    """Test cases for LocalBuffer.mark_sent method."""

    @pytest.fixture
    def buffer(self):
        """Create an in-memory buffer for testing."""
        buf = LocalBuffer(":memory:")
        yield buf
        buf.close()

    @pytest.mark.unit
    def test_mark_sent_removes_record(self, buffer):
        """Test that mark_sent removes the record."""
        record_id = buffer.add({"test": "data"})
        assert buffer.count_pending() == 1

        buffer.mark_sent(record_id)
        assert buffer.count_pending() == 0

    @pytest.mark.unit
    def test_mark_sent_specific_record(self, buffer):
        """Test marking only the specific record as sent."""
        id1 = buffer.add({"test": 1})
        id2 = buffer.add({"test": 2})
        id3 = buffer.add({"test": 3})

        buffer.mark_sent(id2)

        pending = buffer.get_pending()
        assert len(pending) == 2
        assert pending[0]['id'] == id1
        assert pending[1]['id'] == id3

    @pytest.mark.unit
    def test_mark_sent_nonexistent_id(self, buffer):
        """Test marking nonexistent ID (should not raise)."""
        buffer.add({"test": "data"})

        # Should not raise
        buffer.mark_sent(99999)

        # Original record should still exist
        assert buffer.count_pending() == 1


class TestLocalBufferMarkFailed:
    """Test cases for LocalBuffer.mark_failed method."""

    @pytest.fixture
    def buffer(self):
        """Create an in-memory buffer for testing."""
        buf = LocalBuffer(":memory:")
        yield buf
        buf.close()

    @pytest.mark.unit
    def test_mark_failed_increments_retry_count(self, buffer):
        """Test that mark_failed increments retry count."""
        record_id = buffer.add({"test": "data"})

        buffer.mark_failed(record_id, "Error 1")
        buffer.mark_failed(record_id, "Error 2")
        buffer.mark_failed(record_id, "Error 3")

        pending = buffer.get_pending()
        assert pending[0]['retry_count'] == 3

    @pytest.mark.unit
    def test_mark_failed_updates_error(self, buffer):
        """Test that mark_failed updates the error message."""
        record_id = buffer.add({"test": "data"})

        buffer.mark_failed(record_id, "First error")
        pending = buffer.get_pending()
        assert pending[0]['last_error'] == "First error"

        buffer.mark_failed(record_id, "Second error")
        pending = buffer.get_pending()
        assert pending[0]['last_error'] == "Second error"

    @pytest.mark.unit
    def test_mark_failed_nonexistent_id(self, buffer):
        """Test marking nonexistent ID as failed (should not raise)."""
        buffer.add({"test": "data"})

        # Should not raise
        buffer.mark_failed(99999, "Error")

        # Original record should be unaffected
        pending = buffer.get_pending()
        assert pending[0]['retry_count'] == 0


class TestLocalBufferCountPending:
    """Test cases for LocalBuffer.count_pending method."""

    @pytest.fixture
    def buffer(self):
        """Create an in-memory buffer for testing."""
        buf = LocalBuffer(":memory:")
        yield buf
        buf.close()

    @pytest.mark.unit
    def test_count_pending_empty(self, buffer):
        """Test count on empty buffer."""
        assert buffer.count_pending() == 0

    @pytest.mark.unit
    def test_count_pending_after_adds(self, buffer):
        """Test count after adding records."""
        buffer.add({"test": 1})
        assert buffer.count_pending() == 1

        buffer.add({"test": 2})
        assert buffer.count_pending() == 2

    @pytest.mark.unit
    def test_count_pending_after_mark_sent(self, buffer):
        """Test count decreases after mark_sent."""
        id1 = buffer.add({"test": 1})
        buffer.add({"test": 2})
        assert buffer.count_pending() == 2

        buffer.mark_sent(id1)
        assert buffer.count_pending() == 1


class TestLocalBufferClear:
    """Test cases for LocalBuffer.clear method."""

    @pytest.fixture
    def buffer(self):
        """Create an in-memory buffer for testing."""
        buf = LocalBuffer(":memory:")
        yield buf
        buf.close()

    @pytest.mark.unit
    def test_clear_empty_buffer(self, buffer):
        """Test clearing empty buffer."""
        deleted = buffer.clear()
        assert deleted == 0

    @pytest.mark.unit
    def test_clear_removes_all(self, buffer):
        """Test that clear removes all records."""
        for i in range(10):
            buffer.add({"index": i})

        deleted = buffer.clear()
        assert deleted == 10
        assert buffer.count_pending() == 0

    @pytest.mark.unit
    def test_clear_returns_count(self, buffer):
        """Test that clear returns the count of deleted records."""
        buffer.add({"test": 1})
        buffer.add({"test": 2})
        buffer.add({"test": 3})

        deleted = buffer.clear()
        assert deleted == 3


class TestLocalBufferConcurrency:
    """Concurrency tests for LocalBuffer."""

    @pytest.mark.concurrency
    def test_concurrent_adds(self):
        """Test concurrent adds from multiple threads."""
        buffer = LocalBuffer(":memory:")
        num_threads = 10
        adds_per_thread = 50

        def add_records(thread_id):
            for i in range(adds_per_thread):
                buffer.add({"thread": thread_id, "index": i})

        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=add_records, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        assert buffer.count_pending() == num_threads * adds_per_thread
        buffer.close()

    @pytest.mark.concurrency
    def test_concurrent_add_and_get(self):
        """Test concurrent adds and gets."""
        buffer = LocalBuffer(":memory:")
        results = []
        lock = threading.Lock()

        def add_records():
            for i in range(50):
                buffer.add({"index": i})
                time.sleep(0.001)

        def get_records():
            for _ in range(50):
                pending = buffer.get_pending(limit=10)
                with lock:
                    results.append(len(pending))
                time.sleep(0.001)

        add_thread = threading.Thread(target=add_records)
        get_thread = threading.Thread(target=get_records)

        add_thread.start()
        get_thread.start()

        add_thread.join()
        get_thread.join()

        # Should have gotten some results
        assert len(results) == 50
        buffer.close()

    @pytest.mark.concurrency
    def test_concurrent_mark_sent(self):
        """Test concurrent mark_sent operations."""
        buffer = LocalBuffer(":memory:")

        # Add records
        ids = [buffer.add({"index": i}) for i in range(100)]

        def mark_sent_batch(id_batch):
            for record_id in id_batch:
                buffer.mark_sent(record_id)

        # Split IDs into batches for different threads
        batch_size = 10
        batches = [ids[i:i+batch_size] for i in range(0, len(ids), batch_size)]

        threads = []
        for batch in batches:
            t = threading.Thread(target=mark_sent_batch, args=(batch,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        assert buffer.count_pending() == 0
        buffer.close()

    @pytest.mark.concurrency
    def test_no_deadlock_under_load(self):
        """Test that no deadlock occurs under heavy load."""
        buffer = LocalBuffer(":memory:")
        stop_event = threading.Event()

        def writer():
            while not stop_event.is_set():
                buffer.add({"timestamp": time.time()})
                time.sleep(0.001)

        def reader():
            while not stop_event.is_set():
                buffer.get_pending(limit=10)
                time.sleep(0.001)

        def cleaner():
            while not stop_event.is_set():
                pending = buffer.get_pending(limit=5)
                for record in pending:
                    buffer.mark_sent(record['id'])
                time.sleep(0.005)

        threads = [
            threading.Thread(target=writer),
            threading.Thread(target=writer),
            threading.Thread(target=reader),
            threading.Thread(target=reader),
            threading.Thread(target=cleaner),
        ]

        for t in threads:
            t.start()

        # Run for a short time
        time.sleep(0.5)
        stop_event.set()

        for t in threads:
            t.join(timeout=2)
            assert not t.is_alive(), "Thread did not terminate - possible deadlock"

        buffer.close()


class TestLocalBufferEdgeCases:
    """Edge case tests for LocalBuffer."""

    @pytest.mark.unit
    def test_large_payload_1mb(self):
        """Test handling of large (~1MB) payload."""
        buffer = LocalBuffer(":memory:")

        # Create a ~1MB payload
        large_data = "x" * (1024 * 1024)
        payload = {"data": large_data}

        record_id = buffer.add(payload)
        pending = buffer.get_pending()

        assert len(pending) == 1
        assert len(pending[0]['payload']['data']) == 1024 * 1024
        buffer.close()

    @pytest.mark.slow
    def test_large_batch_10000_records(self):
        """Test handling of 10000 records."""
        buffer = LocalBuffer(":memory:")

        # Add 10000 records
        for i in range(10000):
            buffer.add({"index": i})

        assert buffer.count_pending() == 10000

        # Get all in batches
        total_retrieved = 0
        while True:
            pending = buffer.get_pending(limit=1000)
            if not pending:
                break
            total_retrieved += len(pending)
            for record in pending:
                buffer.mark_sent(record['id'])

        assert total_retrieved == 10000
        assert buffer.count_pending() == 0
        buffer.close()

    @pytest.mark.unit
    def test_unicode_payload(self):
        """Test handling of unicode characters in payload."""
        buffer = LocalBuffer(":memory:")

        payload = {
            "message": "Hello 世界 🌍 Привет мир",
            "emoji": "🎉🚀💯",
            "chinese": "中文测试",
            "arabic": "مرحبا بالعالم",
            "special": "line1\nline2\ttab"
        }

        buffer.add(payload)
        pending = buffer.get_pending()

        assert pending[0]['payload']['message'] == "Hello 世界 🌍 Привет мир"
        assert pending[0]['payload']['emoji'] == "🎉🚀💯"
        assert pending[0]['payload']['chinese'] == "中文测试"
        buffer.close()

    @pytest.mark.unit
    def test_special_characters_in_error(self):
        """Test special characters in error messages."""
        buffer = LocalBuffer(":memory:")

        error_msg = "Error: Connection failed\n\tDetails: \"Host 'server' not found\"\n\tCode: 500"
        buffer.add({"test": "data"}, error=error_msg)

        pending = buffer.get_pending()
        assert pending[0]['last_error'] == error_msg
        buffer.close()

    @pytest.mark.unit
    def test_none_values_in_payload(self):
        """Test None values in payload."""
        buffer = LocalBuffer(":memory:")

        payload = {"key": None, "nested": {"inner": None}}
        buffer.add(payload)

        pending = buffer.get_pending()
        assert pending[0]['payload']['key'] is None
        assert pending[0]['payload']['nested']['inner'] is None
        buffer.close()

    @pytest.mark.unit
    def test_empty_payload(self):
        """Test empty payload."""
        buffer = LocalBuffer(":memory:")

        buffer.add({})
        pending = buffer.get_pending()

        assert pending[0]['payload'] == {}
        buffer.close()

    @pytest.mark.unit
    def test_list_payload(self):
        """Test list as payload value."""
        buffer = LocalBuffer(":memory:")

        payload = {"items": [1, 2, 3, 4, 5], "nested": [{"a": 1}, {"b": 2}]}
        buffer.add(payload)

        pending = buffer.get_pending()
        assert pending[0]['payload']['items'] == [1, 2, 3, 4, 5]
        assert pending[0]['payload']['nested'] == [{"a": 1}, {"b": 2}]
        buffer.close()


class TestLocalBufferFilePersistence:
    """Test file-based persistence."""

    @pytest.mark.integration
    def test_persistence_across_instances(self, temp_sqlite_buffer):
        """Test that data persists across buffer instances."""
        # Add records to first instance
        buffer1 = LocalBuffer(temp_sqlite_buffer)
        buffer1.add({"test": "data1"})
        buffer1.add({"test": "data2"})
        buffer1.close()

        # Open new instance and verify data
        buffer2 = LocalBuffer(temp_sqlite_buffer)
        assert buffer2.count_pending() == 2

        pending = buffer2.get_pending()
        assert pending[0]['payload']['test'] == "data1"
        assert pending[1]['payload']['test'] == "data2"
        buffer2.close()

    @pytest.mark.integration
    def test_file_created_on_init(self, temp_sqlite_buffer):
        """Test that file is created on initialization."""
        buffer = LocalBuffer(temp_sqlite_buffer)
        assert os.path.exists(temp_sqlite_buffer)
        buffer.close()
