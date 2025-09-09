# Probe Application Test Suite

This directory contains comprehensive tests for the database probe application.

## Test Structure

```
tests/
├── __init__.py                 # Test package initialization
├── conftest.py                 # Pytest fixtures and configuration
├── test_config.py              # Tests for configuration classes
├── test_probe_application.py   # Tests for ProbeApplication class
├── test_models.py              # Tests for model classes
├── test_database_manager.py    # Tests for DatabaseManager
├── test_handlers.py            # Tests for handler classes
├── test_integration.py         # Integration tests
├── test_main.py                # Tests for main module
├── run_tests.py                # Test runner script
└── README.md                   # This file
```

## Test Categories

### 1. Unit Tests
- **Configuration Tests** (`test_config.py`): Test all configuration classes
- **Model Tests** (`test_models.py`): Test data models (Change, ConnectionInfo)
- **Handler Tests** (`test_handlers.py`): Test ChangesIntake and ChangesProcessor
- **Database Manager Tests** (`test_database_manager.py`): Test database operations

### 2. Integration Tests
- **Application Integration** (`test_integration.py`): Test complete application flow
- **Database Integration**: Test with real Firebird databases
- **Component Integration**: Test interactions between components

### 3. System Tests
- **Main Module Tests** (`test_main.py`): Test application entry point
- **End-to-End Tests**: Test complete application lifecycle

## Running Tests

### Prerequisites
Install test dependencies:
```bash
pip install -r requirements-test.txt
```

### Run All Tests
```bash
# Using the test runner script
python tests/run_tests.py

# Or using pytest directly
pytest tests/ -v
```

### Run Specific Test Files
```bash
# Run specific test file
python tests/run_tests.py test_config.py

# Or using pytest
pytest tests/test_config.py -v
```

### Run Tests with Coverage
```bash
pytest tests/ --cov=. --cov-report=html
```

## Test Fixtures

The `conftest.py` file provides several useful fixtures:

- `temp_fdb_file`: Creates a temporary Firebird database file
- `fdb_connection`: Creates a connection to a temporary database
- `test_database_config`: Creates test database configuration
- `test_worker_config`: Creates test worker configuration
- `test_app_config`: Creates complete test application configuration
- `mock_queue`: Creates a mock queue for testing
- `mock_changes_intake`: Creates a mock changes intake
- `mock_changes_processor`: Creates a mock changes processor
- `mock_database_manager`: Creates a mock database manager

## Test Database Setup

Tests use temporary Firebird database files to avoid affecting real data:

1. **Temporary Database**: Each test gets a fresh, temporary FDB file
2. **Automatic Cleanup**: Temporary files are cleaned up after tests
3. **Isolated Tests**: Each test runs in isolation with its own database

## Mocking Strategy

Tests use mocking to:
- **Isolate Components**: Test individual components without dependencies
- **Control Behavior**: Simulate specific conditions and error states
- **Speed Up Tests**: Avoid slow database operations where possible
- **Test Error Handling**: Simulate failures and edge cases

## Test Coverage

The test suite covers:

- ✅ **Configuration Management**: All config classes and methods
- ✅ **Application Lifecycle**: Setup, run, and shutdown phases
- ✅ **Database Operations**: Schema setup, triggers, change tracking
- ✅ **Change Processing**: Intake and processing of database changes
- ✅ **Error Handling**: Exception handling and graceful failures
- ✅ **Signal Handling**: Graceful shutdown on SIGINT
- ✅ **Command Line Arguments**: Reset and reset-and-exit options
- ✅ **Worker Threads**: Multi-threaded change processing

## Writing New Tests

### Test Naming Convention
- Test files: `test_*.py`
- Test classes: `Test*`
- Test methods: `test_*`

### Example Test Structure
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
    # Use the fixtures provided by conftest.py
    app = ProbeApplication(test_app_config)
    # ... test logic
```

## Continuous Integration

The test suite is designed to run in CI environments:
- **No External Dependencies**: Tests don't require external services
- **Fast Execution**: Most tests complete in seconds
- **Deterministic**: Tests produce consistent results
- **Isolated**: Tests don't interfere with each other

## Troubleshooting

### Common Issues

1. **Import Errors**: Make sure you're running tests from the probe directory
2. **Database Connection Errors**: Check that Firebird is properly installed
3. **Permission Errors**: Ensure write permissions for temporary files

### Debug Mode
Run tests with more verbose output:
```bash
pytest tests/ -v -s --tb=long
```

### Test Specific Component
```bash
# Test only configuration
pytest tests/test_config.py -v

# Test only database manager
pytest tests/test_database_manager.py -v
```
