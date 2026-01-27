"""
Extended tests for ChangesIntake class.

This module covers:
- Event handling and processing
- Error recovery
- Connection management
- Multi-threaded scenarios
"""
import pytest
import time
import threading
from queue import Queue
from unittest.mock import Mock, patch, MagicMock
import fdb

from handlers.changes_intake import ChangesIntake
from models.connection_info import ConnectionInfo


class TestChangesIntakeEventHandling:
    """Test cases for event handling in ChangesIntake."""

    @pytest.mark.integration
    def test_run_receives_events(self, changes_log_setup, test_connection_info):
        """Test that POST_EVENT triggers processing."""
        output_queue = Queue()

        # Insert a change before starting intake
        changes_log_setup.execute_immediate("""
            INSERT INTO CHANGES_LOG (LOG_ID, PK_VAL, TABLE_ID, MUTATION, OCCURRED_AT, PROCESSED)
            VALUES (1, 100, 1, 'INSERT', CURRENT_TIMESTAMP, 0)
        """)
        changes_log_setup.commit()

        intake = ChangesIntake(test_connection_info, 1, output_queue)

        # Start intake in background
        intake.start()

        # Give it time to process
        time.sleep(0.5)

        # Stop intake
        intake.stop()
        intake.join(timeout=2)

        # Should have received the change
        assert not output_queue.empty()

    @pytest.mark.integration
    def test_run_handles_multiple_events(self, changes_log_setup, test_connection_info):
        """Test handling multiple sequential events."""
        output_queue = Queue()

        # Insert multiple changes
        for i in range(5):
            changes_log_setup.execute_immediate(f"""
                INSERT INTO CHANGES_LOG (LOG_ID, PK_VAL, TABLE_ID, MUTATION, OCCURRED_AT, PROCESSED)
                VALUES ({i + 1}, {100 + i}, 1, 'INSERT', CURRENT_TIMESTAMP, 0)
            """)
            changes_log_setup.commit()

        intake = ChangesIntake(test_connection_info, 1, output_queue)
        intake.start()

        time.sleep(0.5)

        intake.stop()
        intake.join(timeout=2)

        # Should have received all changes
        received = []
        while not output_queue.empty():
            received.append(output_queue.get_nowait())

        assert len(received) == 5


class TestChangesIntakeErrorRecovery:
    """Test cases for error recovery in ChangesIntake."""

    @pytest.mark.unit
    @patch('handlers.changes_intake.fdb.connect')
    def test_run_recovers_from_db_error(self, mock_connect):
        """Test recovery from database errors."""
        output_queue = Queue()

        # Create a mock connection info
        mock_conn_info = ConnectionInfo(
            db_path='/tmp/test.fdb',
            db_user='SYSDBA',
            db_password='masterkey'
        )

        # First call fails, second succeeds
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.fetchall.return_value = []
        mock_connect.side_effect = [fdb.DatabaseError("Connection failed"), mock_conn]

        intake = ChangesIntake(mock_conn_info, 1, output_queue)

        # Run briefly
        def run_and_stop():
            time.sleep(0.3)
            intake.stop()

        stop_thread = threading.Thread(target=run_and_stop)
        stop_thread.start()

        # This should recover from the first error
        intake.run()

        stop_thread.join()

        # Should have attempted to reconnect
        assert mock_connect.call_count >= 1


class TestChangesIntakeConnectionManagement:
    """Test cases for connection management in ChangesIntake."""

    @pytest.fixture
    def mock_conn_info(self):
        """Create a mock connection info for unit tests."""
        return ConnectionInfo(
            db_path='/tmp/test.fdb',
            db_user='SYSDBA',
            db_password='masterkey'
        )

    @pytest.mark.unit
    @patch('handlers.changes_intake.fdb.connect')
    def test_stop_during_processing(self, mock_connect, mock_conn_info):
        """Test that stop() works during active processing."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        output_queue = Queue()
        intake = ChangesIntake(mock_conn_info, 1, output_queue)

        # Start intake
        intake.start()

        # Immediately stop
        time.sleep(0.1)
        intake.stop()

        # Should stop cleanly
        intake.join(timeout=5)
        assert not intake.is_alive()

    @pytest.mark.unit
    @patch('handlers.changes_intake.fdb.connect')
    def test_connection_lock_prevents_race(self, mock_connect, mock_conn_info):
        """Test that connection lock prevents race conditions."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        output_queue = Queue()
        intake = ChangesIntake(mock_conn_info, 1, output_queue)

        race_detected = {'value': False}

        def try_concurrent_access():
            for _ in range(10):
                try:
                    with intake._conn_lock:
                        if intake.conn:
                            # Simulate some operation
                            time.sleep(0.01)
                except Exception:
                    race_detected['value'] = True

        intake.start()

        # Start multiple threads trying to access connection
        threads = [threading.Thread(target=try_concurrent_access) for _ in range(3)]
        for t in threads:
            t.start()

        time.sleep(0.2)

        intake.stop()
        intake.join(timeout=2)

        for t in threads:
            t.join()

        assert not race_detected['value']

    @pytest.mark.integration
    def test_multiple_start_stop_cycles(self, test_connection_info):
        """Test multiple start/stop cycles."""
        output_queue = Queue()

        for _ in range(3):
            intake = ChangesIntake(test_connection_info, 1, output_queue)
            intake.start()
            time.sleep(0.1)
            intake.stop()
            intake.join(timeout=2)
            assert not intake.is_alive()


class TestChangesIntakePositionTracking:
    """Test cases for position tracking."""

    @pytest.mark.integration
    def test_position_increments_after_processing(self, changes_log_setup, test_connection_info):
        """Test that position increments after processing changes."""
        output_queue = Queue()

        # Insert changes
        for i in range(3):
            changes_log_setup.execute_immediate(f"""
                INSERT INTO CHANGES_LOG (LOG_ID, PK_VAL, TABLE_ID, MUTATION, OCCURRED_AT, PROCESSED)
                VALUES ({i + 1}, {100 + i}, 1, 'INSERT', CURRENT_TIMESTAMP, 0)
            """)
            changes_log_setup.commit()

        initial_pos = 1
        intake = ChangesIntake(test_connection_info, initial_pos, output_queue)

        intake.start()
        time.sleep(0.5)
        intake.stop()
        intake.join(timeout=2)

        # Position should have advanced
        assert intake.pos > initial_pos


class TestChangesIntakeStoppedFlag:
    """Test cases for stopped flag handling."""

    @pytest.fixture
    def mock_conn_info(self):
        """Create a mock connection info for unit tests."""
        return ConnectionInfo(
            db_path='/tmp/test.fdb',
            db_user='SYSDBA',
            db_password='masterkey'
        )

    @pytest.mark.unit
    def test_stopped_returns_false_initially(self, mock_conn_info):
        """Test that stopped() returns False initially."""
        output_queue = Queue()
        intake = ChangesIntake(mock_conn_info, 1, output_queue)

        assert not intake.stopped()

    @pytest.mark.unit
    def test_stopped_returns_true_after_stop(self, mock_conn_info):
        """Test that stopped() returns True after stop()."""
        output_queue = Queue()
        intake = ChangesIntake(mock_conn_info, 1, output_queue)

        intake.stop()
        assert intake.stopped()

    @pytest.mark.unit
    def test_stop_is_idempotent(self, mock_conn_info):
        """Test that calling stop() multiple times is safe."""
        output_queue = Queue()
        intake = ChangesIntake(mock_conn_info, 1, output_queue)

        # Multiple stops should not raise
        intake.stop()
        intake.stop()
        intake.stop()

        assert intake.stopped()
