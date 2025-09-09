"""
Tests for ProbeApplication class.
"""
import pytest
import sys
import signal
from unittest.mock import Mock, patch, MagicMock
from queue import Queue

from app.probe_application import ProbeApplication
from config.app_config import AppConfig, DatabaseConfig, WorkerConfig


class TestProbeApplication:
    """Test cases for ProbeApplication class."""
    
    def test_initialization(self, test_app_config):
        """Test ProbeApplication initialization."""
        app = ProbeApplication(test_app_config)
        
        assert app.config == test_app_config
        assert app.connection is None
        assert app.db_manager is None
        assert app.changes_intake is None
        assert app.changes_processor is None
        assert app.output_queue is None
        assert app.executor is None
        assert app._shutdown_requested is False
    
    @patch('app.probe_application.fdb.connect')
    def test_setup_database_connection(self, mock_fdb_connect, test_app_config):
        """Test database connection setup."""
        mock_conn = Mock()
        mock_fdb_connect.return_value = mock_conn
        
        app = ProbeApplication(test_app_config)
        app.setup_database_connection()
        
        mock_fdb_connect.assert_called_once_with(
            dsn=test_app_config.database.path,
            user=test_app_config.database.user,
            password=test_app_config.database.password,
            charset=test_app_config.database.charset
        )
        assert app.connection == mock_conn
        assert app.db_manager is not None
    
    @patch('app.probe_application.sys.argv', ['main.py', '--reset'])
    def test_handle_command_line_args_reset(self, test_app_config, mock_database_manager):
        """Test handling --reset command line argument."""
        app = ProbeApplication(test_app_config)
        app.db_manager = mock_database_manager
        
        app.handle_command_line_args()
        
        mock_database_manager.reset_state.assert_called_once()
    
    @patch('app.probe_application.sys.argv', ['main.py', '--reset-and-exit'])
    @patch('app.probe_application.sys.exit')
    def test_handle_command_line_args_reset_and_exit(self, mock_exit, test_app_config, mock_database_manager):
        """Test handling --reset-and-exit command line argument."""
        app = ProbeApplication(test_app_config)
        app.db_manager = mock_database_manager
        
        app.handle_command_line_args()
        
        mock_database_manager.reset_state.assert_called_once()
        mock_exit.assert_called_once_with(0)
    
    @patch('app.probe_application.sys.argv', ['main.py'])
    def test_handle_command_line_args_no_args(self, test_app_config, mock_database_manager):
        """Test handling no command line arguments."""
        app = ProbeApplication(test_app_config)
        app.db_manager = mock_database_manager
        
        app.handle_command_line_args()
        
        mock_database_manager.reset_state.assert_not_called()
    
    def test_setup_database_schema(self, test_app_config, mock_database_manager):
        """Test database schema setup."""
        app = ProbeApplication(test_app_config)
        app.db_manager = mock_database_manager
        
        id_to_table, table_to_primary_key = app.setup_database_schema()
        
        mock_database_manager.setup.assert_called_once()
        mock_database_manager.ensure_clean_slate.assert_called_once_with(app.connection)
        assert id_to_table == {1: 'test_table'}
        assert table_to_primary_key == {'test_table': 'id'}
    
    def test_setup_change_monitoring(self, test_app_config):
        """Test change monitoring setup."""
        app = ProbeApplication(test_app_config)
        app.connection = Mock()
        
        id_to_table = {1: 'test_table'}
        table_to_primary_key = {'test_table': 'id'}
        
        app.setup_change_monitoring(id_to_table, table_to_primary_key)
        
        assert app.output_queue is not None
        assert isinstance(app.output_queue, Queue)
        assert app.changes_intake is not None
        assert app.changes_processor is not None
    
    @patch('app.probe_application.signal.signal')
    def test_setup_signal_handlers(self, mock_signal, test_app_config):
        """Test signal handler setup."""
        app = ProbeApplication(test_app_config)
        
        app.setup_signal_handlers()
        
        mock_signal.assert_called_once()
    
    def test_start_workers(self, test_app_config, mock_changes_processor, mock_executor):
        """Test starting worker threads."""
        app = ProbeApplication(test_app_config)
        app.changes_processor = mock_changes_processor
        
        with patch('app.probe_application.ThreadPoolExecutor', return_value=mock_executor):
            app.start_workers()
        
        assert app.executor == mock_executor
        assert mock_executor.submit.call_count == test_app_config.workers.max_workers
    
    def test_start_change_intake(self, test_app_config, mock_changes_intake):
        """Test starting change intake."""
        app = ProbeApplication(test_app_config)
        app.changes_intake = mock_changes_intake
        
        app.start_change_intake()
        
        mock_changes_intake.start.assert_called_once()
    
    def test_signal_handler(self, test_app_config):
        """Test signal handler functionality."""
        app = ProbeApplication(test_app_config)
        app.shutdown = Mock()
        
        app._signal_handler(signal.SIGINT, None)
        
        assert app._shutdown_requested is True
        app.shutdown.assert_called_once()
    
    def test_shutdown(self, test_app_config, mock_changes_intake, mock_executor):
        """Test application shutdown."""
        app = ProbeApplication(test_app_config)
        app.connection = Mock()
        app.output_queue = Queue()
        app.changes_intake = mock_changes_intake
        app.executor = mock_executor
        
        app.shutdown()
        
        # Check that shutdown signal was sent
        assert app.output_queue.get() is None
        
        mock_changes_intake.stop.assert_called_once()
        mock_changes_intake.join.assert_called_once()
        mock_executor.shutdown.assert_called_once_with(wait=True)
        app.connection.close.assert_called_once()
    
    @patch('app.probe_application.ThreadPoolExecutor')
    def test_run_complete_flow(self, mock_executor_class, test_app_config, 
                              mock_database_manager, mock_changes_intake):
        """Test the complete application run flow."""
        # Setup mocks
        mock_conn = Mock()
        mock_executor = Mock()
        mock_executor_class.return_value = mock_executor
        
        app = ProbeApplication(test_app_config)
        
        # Mock all the setup methods
        app.setup_database_connection = Mock()
        app.setup_database_connection.side_effect = lambda: setattr(app, 'connection', mock_conn)
        app.handle_command_line_args = Mock()
        app.setup_database_schema = Mock(return_value=({1: 'test_table'}, {'test_table': 'id'}))
        app.setup_change_monitoring = Mock()
        app.setup_signal_handlers = Mock()
        app.start_change_intake = Mock()
        app.start_workers = Mock()
        app.shutdown = Mock()
        
        # Mock changes_intake to simulate normal operation
        app.changes_intake = mock_changes_intake
        mock_changes_intake.join.side_effect = KeyboardInterrupt()  # Simulate Ctrl+C
        
        # Run the application
        with pytest.raises(KeyboardInterrupt):
            app.run()
        
        # Verify all setup methods were called
        app.setup_database_connection.assert_called_once()
        app.handle_command_line_args.assert_called_once()
        app.setup_database_schema.assert_called_once()
        app.setup_change_monitoring.assert_called_once()
        app.setup_signal_handlers.assert_called_once()
        app.start_change_intake.assert_called_once()
        app.start_workers.assert_called_once()
        app.shutdown.assert_called_once()
    
    def test_run_with_exception(self, test_app_config):
        """Test application run with exception handling."""
        app = ProbeApplication(test_app_config)
        app.setup_database_connection = Mock(side_effect=Exception("Test error"))
        app.shutdown = Mock()
        
        with pytest.raises(Exception, match="Test error"):
            app.run()
        
        app.shutdown.assert_called_once()
