"""
Integration tests for the probe application.
"""
import pytest
import tempfile
import time
import signal
from unittest.mock import patch, Mock
from queue import Queue

from app.probe_application import ProbeApplication
from config.app_config import AppConfig, DatabaseConfig, WorkerConfig
from handlers.changes_intake import ChangesIntake
from handlers.changes_processor import ChangesProcessor
from models.change import Change
from models.connection_info import ConnectionInfo


class TestProbeApplicationIntegration:
    """Integration tests for the complete probe application."""
    
    def test_full_application_setup(self, temp_fdb_file):
        """Test complete application setup without running."""
        # Create configuration
        config = AppConfig(
            database=DatabaseConfig(
                path=temp_fdb_file,
                user='SYSDBA',
                password='masterkey'
            ),
            workers=WorkerConfig(max_workers=2, intake_position=1)
        )
        
        app = ProbeApplication(config)
        
        # Test setup methods
        app.setup_database_connection()
        assert app.connection is not None
        assert app.db_manager is not None
        
        # Test database schema setup
        id_to_table, table_to_primary_key = app.setup_database_schema()
        assert isinstance(id_to_table, dict)
        assert isinstance(table_to_primary_key, dict)
        
        # Test change monitoring setup
        app.setup_change_monitoring(id_to_table, table_to_primary_key)
        assert app.output_queue is not None
        assert app.changes_intake is not None
        assert app.changes_processor is not None
    
    def test_database_manager_integration(self, temp_fdb_file):
        """Test DatabaseManager integration with real database."""
        import fdb
        from database.database_manager import DatabaseManager
        
        # Connect to test database
        conn = fdb.connect(
            dsn=temp_fdb_file,
            user='SYSDBA',
            password='masterkey'
        )
        
        db_manager = DatabaseManager(conn)
        
        # Create a test table
        conn.execute_immediate("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50)
            )
        """)
        conn.commit()
        
        # Test setup
        id_to_table, table_to_primary_key = db_manager.setup()
        
        # Verify changes log was created
        table_names = db_manager.get_table_names()
        assert "CHANGES_LOG" in table_names
        
        # Verify triggers were created
        assert len(id_to_table) > 0
        assert len(table_to_primary_key) > 0
        
        conn.close()
    
    def test_changes_intake_integration(self, temp_fdb_file):
        """Test ChangesIntake integration with real database."""
        import fdb
        
        # Connect to test database
        conn = fdb.connect(
            dsn=temp_fdb_file,
            user='SYSDBA',
            password='masterkey'
        )
        
        # Create changes log table
        conn.execute_immediate("""
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
            CREATE OR ALTER TRIGGER INTAKE_TRIGGER
                FOR CHANGES_LOG
                ACTIVE AFTER INSERT POSITION 10
            AS
            BEGIN
                POST_EVENT 'INTAKE_SIGNAL';
            END
        """)
        conn.commit()
        
        # Create output queue
        output_queue = Queue()
        
        # Create changes intake
        intake = ChangesIntake(conn, 1, output_queue)
        
        # Insert a change
        conn.execute_immediate("""
            INSERT INTO CHANGES_LOG (LOG_ID, PK_VAL, TABLE_ID, MUTATION, OCCURRED_AT, PROCESSED)
            VALUES (1, 123, 1, 'INSERT', CURRENT_TIMESTAMP, 0)
        """)
        conn.commit()
        
        # Start intake and stop quickly
        intake.start()
        time.sleep(0.1)  # Give it a moment to process
        intake.stop()
        intake.join(timeout=1)
        
        # Verify change was processed
        assert not output_queue.empty()
        change = output_queue.get()
        assert isinstance(change, Change)
        assert change.log_id == 1
        assert change.pk_val == 123
        
        conn.close()
    
    def test_changes_processor_integration(self, temp_fdb_file):
        """Test ChangesProcessor integration."""
        import fdb
        
        # Connect to test database
        conn = fdb.connect(
            dsn=temp_fdb_file,
            user='SYSDBA',
            password='masterkey'
        )
        
        # Create test table
        conn.execute_immediate("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50)
            )
        """)
        conn.commit()
        
        # Create connection info
        conn_info = ConnectionInfo(
            db_path=temp_fdb_file,
            db_user='SYSDBA',
            db_password='masterkey'
        )
        
        # Create input queue with test change
        input_queue = Queue()
        test_change = Change(
            log_id=1,
            pk_val=123,
            table_id=1,
            mutation="INSERT",
            occured_at=time.time(),
            processed=False
        )
        input_queue.put(test_change)
        input_queue.put(None)  # Signal to stop
        
        # Create processor
        id_to_table = {1: 'test_table'}
        table_to_primary_key = {'test_table': 'id'}
        
        processor = ChangesProcessor(
            conn_info,
            input_queue,
            id_to_table,
            table_to_primary_key
        )
        
        # Run processor
        processor.begin_read(worker_id=0)
        
        # Verify change was processed (no exceptions raised)
        assert True
        
        conn.close()
    
    def test_command_line_args_integration(self, temp_fdb_file):
        """Test command line argument handling integration."""
        config = AppConfig(
            database=DatabaseConfig(
                path=temp_fdb_file,
                user='SYSDBA',
                password='masterkey'
            )
        )
        
        app = ProbeApplication(config)
        app.setup_database_connection()
        
        # Test --reset argument
        with patch('sys.argv', ['main.py', '--reset']):
            app.handle_command_line_args()
            # Should not raise exception
        
        # Test --reset-and-exit argument
        with patch('sys.argv', ['main.py', '--reset-and-exit']):
            with patch('sys.exit') as mock_exit:
                app.handle_command_line_args()
                mock_exit.assert_called_once_with(0)
    
    def test_signal_handling_integration(self, temp_fdb_file):
        """Test signal handling integration."""
        config = AppConfig(
            database=DatabaseConfig(
                path=temp_fdb_file,
                user='SYSDBA',
                password='masterkey'
            )
        )
        
        app = ProbeApplication(config)
        app.shutdown = Mock()
        
        # Test signal handler
        app._signal_handler(signal.SIGINT, None)
        
        assert app._shutdown_requested is True
        app.shutdown.assert_called_once()
    
    def test_worker_thread_integration(self, temp_fdb_file):
        """Test worker thread integration."""
        config = AppConfig(
            database=DatabaseConfig(
                path=temp_fdb_file,
                user='SYSDBA',
                password='masterkey'
            ),
            workers=WorkerConfig(max_workers=2)
        )
        
        app = ProbeApplication(config)
        app.setup_database_connection()
        
        # Create mock processor
        mock_processor = Mock()
        app.changes_processor = mock_processor
        
        # Start workers
        app.start_workers()
        
        # Verify workers were started
        assert app.executor is not None
        assert mock_processor.begin_read.call_count == 2
        
        # Cleanup
        app.executor.shutdown(wait=True)
