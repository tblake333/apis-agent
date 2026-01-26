# Test Command `t` - Quick Reference

The `t` command is a simple test runner for the probe application that allows you to run specific tests with flexible syntax.

## Installation

To use `t` from anywhere (instead of `./t` from the probe directory):

```bash
# Run the installation script
./install_t_command.sh

# Or manually create a symlink
ln -sf $(pwd)/t /usr/local/bin/t
```

After installation, you can use `t` from any directory!

## Usage

```bash
t <test_identifier>
```

The `t` command automatically finds tests anywhere in your project structure, not just in a single `tests/` directory.

## Examples

### Run a specific test method
```bash
t test_ensure_clean_slate_empty
t test_default_values
```

### Run all tests in a test class
```bash
t TestDatabaseManager
t TestDatabaseConfig
```

### Run all tests in a module
```bash
t tests.test_config
t tests.test_models
t tests.test_database_manager
t handlers.tests.test_changes_intake_tests
t app.test_app_example
```

### Run a specific test class in a module
```bash
t tests.test_config.TestDatabaseConfig
t tests.test_models.TestChange
t app.test_app_example.TestAppExample
```

### Run a specific test method in a specific class
```bash
t tests.test_config.TestDatabaseConfig.test_default_values
t tests.test_database_manager.TestDatabaseManager.test_ensure_clean_slate_empty
t app.test_app_example.TestAppExample.test_app_functionality
```

## How it works

The `t` command automatically:
- **Recursively searches** for test files throughout your project structure
- **Finds the correct test file** for test names using pattern matching
- **Converts dot notation** to proper pytest paths (e.g., `app.test_example.TestClass` → `app/test_example.py::TestClass`)
- **Uses pytest's pattern matching** (`-k`) for flexible test selection
- **Provides verbose output** with colors
- **Shows short tracebacks** for failures
- **Runs from the correct directory** automatically

## Help

```bash
t --help
```

This shows all available test files and usage examples.

## Examples in Action

```bash
# Run a single test method
$ t test_ensure_clean_slate_empty
Running test: test_ensure_clean_slate_empty
Command: /usr/bin/python3 -m pytest -v --tb=short --color=yes tests/test_database_manager.py -k test_ensure_clean_slate_empty
--------------------------------------------------
=========================================== test session starts ============================================
collected 12 items / 11 deselected / 1 selected

tests/test_database_manager.py::TestDatabaseManager::test_ensure_clean_slate_empty PASSED [100%]

===================================== 1 passed, 11 deselected in 0.60s =====================================
✅ Test passed!

# Run all tests in a class
$ t TestDatabaseConfig
Running test: TestDatabaseConfig
Command: /usr/bin/python3 -m pytest -v --tb=short --color=yes tests/test_config.py -k TestDatabaseConfig
--------------------------------------------------
=========================================== test session starts ============================================
collected 7 items / 5 deselected / 2 selected

tests/test_config.py::TestDatabaseConfig::test_default_values PASSED [ 50%]
tests/test_config.py::TestDatabaseConfig::test_custom_values PASSED [100%]

===================================== 2 passed, 5 deselected in 0.11s =====================================
✅ Test passed!

# Run all tests in a module
$ t tests.test_models
Running test: tests.test_models
Command: /usr/bin/python3 -m pytest -v --tb=short --color=yes tests/test_models.py
--------------------------------------------------
=========================================== test session starts ============================================
collected 6 items

tests/test_models.py::TestChange::test_change_creation PASSED [ 16%]
tests/test_models.py::TestChange::test_change_from_tuple PASSED [ 33%]
tests/test_models.py::TestChange::test_change_mutation_types PASSED [ 50%]
tests/test_models.py::TestConnectionInfo::test_connection_info_creation PASSED [ 66%]
tests/test_models.py::TestConnectionInfo::test_connection_info_required_fields PASSED [ 83%]
tests/test_models.py::TestConnectionInfo::test_connection_info_equality PASSED [100%]

============================================ 6 passed in 0.20s =============================================
✅ Test passed!
```

## Tips

- **Create tests anywhere**: You can create test files in any directory (e.g., `app/test_*.py`, `handlers/tests/test_*.py`)
- **Use tab completion** for test names (if your shell supports it)
- **The command automatically finds** the right test file for test method names
- **Use dots to specify exact paths** when you want to be precise
- **Pattern matching works** across all test files if no specific file is found
- **All tests run** with verbose output and colored results
- **Works from any directory** - the command automatically finds the project root
