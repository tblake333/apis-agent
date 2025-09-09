"""
Tests for the main module.
"""
import pytest
from unittest.mock import patch, Mock
import sys

# Import the main function
from main import main


class TestMain:
    """Test cases for the main function."""
    
    @patch('main.ProbeApplication')
    @patch('main.AppConfig')
    @patch('main.get_microsip_fdb_file_path')
    def test_main_function(self, mock_get_fdb_path, mock_app_config_class, mock_probe_app_class):
        """Test the main function execution."""
        # Setup mocks
        mock_fdb_path = "/test/path.fdb"
        mock_get_fdb_path.return_value = mock_fdb_path
        
        mock_config = Mock()
        mock_app_config_class.from_microsip_path.return_value = mock_config
        
        mock_app = Mock()
        mock_probe_app_class.return_value = mock_app
        
        # Call main function
        main()
        
        # Verify calls
        mock_get_fdb_path.assert_called_once()
        mock_app_config_class.from_microsip_path.assert_called_once_with(mock_fdb_path)
        mock_probe_app_class.assert_called_once_with(mock_config)
        mock_app.run.assert_called_once()
    
    @patch('main.ProbeApplication')
    @patch('main.AppConfig')
    @patch('main.get_microsip_fdb_file_path')
    def test_main_function_with_exception(self, mock_get_fdb_path, mock_app_config_class, mock_probe_app_class):
        """Test main function with exception handling."""
        # Setup mocks to raise exception
        mock_get_fdb_path.side_effect = Exception("FDB path error")
        
        # Call main function - should raise exception
        with pytest.raises(Exception, match="FDB path error"):
            main()
    
    @patch('main.ProbeApplication')
    @patch('main.AppConfig')
    @patch('main.get_microsip_fdb_file_path')
    def test_main_function_app_run_exception(self, mock_get_fdb_path, mock_app_config_class, mock_probe_app_class):
        """Test main function when app.run raises exception."""
        # Setup mocks
        mock_fdb_path = "/test/path.fdb"
        mock_get_fdb_path.return_value = mock_fdb_path
        
        mock_config = Mock()
        mock_app_config_class.from_microsip_path.return_value = mock_config
        
        mock_app = Mock()
        mock_app.run.side_effect = Exception("App run error")
        mock_probe_app_class.return_value = mock_app
        
        # Call main function - should raise exception
        with pytest.raises(Exception, match="App run error"):
            main()
    
    def test_main_module_imports(self):
        """Test that main module imports work correctly."""
        # This test ensures all imports in main.py work
        from main import main
        from app.probe_application import ProbeApplication
        from config.app_config import AppConfig
        from utils.fdb_helper import get_microsip_fdb_file_path
        
        # If we get here, imports are working
        assert main is not None
        assert ProbeApplication is not None
        assert AppConfig is not None
        assert get_microsip_fdb_file_path is not None
