# Testing Guide for Database Probe Application

## Overview

This document provides a comprehensive guide to the test suite for the database probe application. The test suite ensures the application works correctly and maintains its behavior across changes.

## Test Suite Structure

```
tests/
├── conftest.py                 # Shared fixtures and configuration
├── test_config.py              # Configuration classes tests
├── test_models.py              # Data model tests
├── test_probe_application.py   # Main application class tests
├── test_database_manager.py    # Database operations tests
├── test_handlers.py            # Handler classes tests
├── test_integration.py         # Integration tests
├── test_main.py                # Main module tests
├── run_tests.py                # Test runner script
├── requirements-test.txt       # Test dependencies
└── README.md                   # Detailed test documentation
```

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements-test.txt
```

### 2. Run All Tests
```bash
# Using the simple 't' command (recommended)
t tests.test_config  # Run all tests in a module

# Using the test runner script
python tests/run_tests.py

# Or using pytest directly
pytest tests/ -v
```

### 3. Run Specific Tests
```bash
# Using the simple 't' command (recommended)
t test_ensure_clean_slate_empty                    # Run specific test method
t TestDatabaseManager                              # Run all tests in a class
t tests.test_config.TestDatabaseConfig           # Run specific test class
t tests.test_config.TestDatabaseConfig.test_default_values  # Run specific test method

# Using the test runner script
python tests/run_tests.py test_config.py

# Or using pytest
pytest tests/test_config.py -v
```

## Test Categories

### Unit Tests
- **Configuration Tests**: Test all configuration classes and their behavior
- **Model Tests**: Test data models (Change, ConnectionInfo) 
- **Handler Tests**: Test ChangesIntake and ChangesProcessor classes
- **Database Manager Tests**: Test database operations and schema management

### Integration Tests
- **Application Integration**: Test complete application lifecycle
- **Database Integration**: Test with real Firebird databases
- **Component Integration**: Test interactions between components

### System Tests
- **Main Module Tests**: Test application entry point
- **End-to-End Tests**: Test complete application behavior

## Test Coverage

The test suite covers:

✅ **Configuration Management**
- Database configuration
- Worker configuration  
- Application configuration
- Configuration validation

✅ **Application Lifecycle**
- Initialization
- Database connection setup
- Schema setup and validation
- Change monitoring setup
- Worker thread management
- Graceful shutdown

✅ **Database Operations**
- Table creation and management
- Trigger creation and management
- Change log operations
- Primary key detection
- Schema validation

✅ **Change Processing**
- Change intake monitoring
- Change processing workflows
- Multi-threaded processing
- Error handling and recovery

✅ **Error Handling**
- Database connection errors
- Configuration errors
- Processing errors
- Graceful failure handling

✅ **Signal Handling**
- SIGINT handling
- Graceful shutdown
- Resource cleanup

✅ **Command Line Interface**
- Reset operations
- Reset-and-exit operations
- Argument parsing

## Running Tests

### Basic Commands
```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_config.py -v

# Run with coverage
pytest tests/ --cov=. --cov-report=html

# Run in verbose mode
pytest tests/ -v -s
```

### Using the Test Runner
```bash
# Run all tests
python tests/run_tests.py

# Run specific test
python tests/run_tests.py test_config.py

# Run with verbose output
python tests/run_tests.py --verbose
```

## Test Fixtures

The test suite provides several useful fixtures:

- `temp_fdb_file`: Creates temporary Firebird database files
- `fdb_connection`: Creates database connections for testing
- `test_database_config`: Creates test database configurations
- `test_worker_config`: Creates test worker configurations
- `test_app_config`: Creates complete application configurations
- `mock_queue`: Creates mock queues for testing
- `mock_changes_intake`: Creates mock change intake objects
- `mock_changes_processor`: Creates mock change processor objects
- `mock_database_manager`: Creates mock database manager objects

## Writing New Tests

### Test Structure
```python
class TestMyComponent:
    """Test cases for MyComponent class."""
    
    def test_initialization(self):
        """Test component initialization."""
        # Arrange
        config = TestConfig()
        
        # Act
        component = MyComponent(config)
        
        # Assert
        assert component.config == config
    
    def test_method_with_exception(self):
        """Test method exception handling."""
        # Arrange
        component = MyComponent()
        
        # Act & Assert
        with pytest.raises(ValueError):
            component.invalid_method()
```

### Using Fixtures
```python
def test_with_database(self, fdb_connection, test_app_config):
    """Test using database fixture."""
    app = ProbeApplication(test_app_config)
    # ... test logic
```

## Test Database Setup

Tests use temporary Firebird database files to ensure:
- **Isolation**: Each test gets a fresh database
- **Safety**: No impact on real data
- **Cleanup**: Automatic cleanup after tests
- **Consistency**: Predictable test environment

## Continuous Integration

The test suite is designed for CI environments:
- **Fast Execution**: Most tests complete in seconds
- **No External Dependencies**: Tests don't require external services
- **Deterministic**: Consistent results across runs
- **Isolated**: Tests don't interfere with each other

## Troubleshooting

### Common Issues

1. **Import Errors**
   - Ensure you're running from the probe directory
   - Check that all dependencies are installed

2. **Database Connection Errors**
   - Verify Firebird is properly installed
   - Check file permissions for temporary files

3. **Test Failures**
   - Run with verbose output: `pytest tests/ -v -s`
   - Check for specific error messages
   - Verify test database setup

### Debug Mode
```bash
# Run with detailed output
pytest tests/ -v -s --tb=long

# Run specific test with debugging
pytest tests/test_config.py::TestDatabaseConfig::test_default_values -v -s
```

## Test Results

### Expected Output
```
=========================================== test session starts ============================================
platform linux -- Python 3.9.2, pytest-8.4.2, pluggy-1.6.0
collected 45 items

tests/test_config.py::TestDatabaseConfig::test_default_values PASSED
tests/test_config.py::TestDatabaseConfig::test_custom_values PASSED
...
tests/test_integration.py::TestProbeApplicationIntegration::test_full_application_setup PASSED

============================================ 45 passed in 12.34s ============================================
```

### Coverage Report
```bash
pytest tests/ --cov=. --cov-report=term-missing
```

## Best Practices

1. **Test Isolation**: Each test should be independent
2. **Clear Naming**: Use descriptive test and method names
3. **Arrange-Act-Assert**: Structure tests clearly
4. **Mock External Dependencies**: Use mocks for database operations
5. **Test Edge Cases**: Include error conditions and edge cases
6. **Documentation**: Document complex test scenarios

## Contributing

When adding new features:
1. Write tests first (TDD approach)
2. Ensure all tests pass
3. Add integration tests for new components
4. Update this guide if needed
5. Run the full test suite before submitting

## Support

For test-related issues:
1. Check this guide first
2. Review test documentation in `tests/README.md`
3. Run tests with verbose output for debugging
4. Check the test fixtures in `conftest.py`
