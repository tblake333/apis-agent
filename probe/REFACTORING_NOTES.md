# Database Probe Refactoring

## Overview
The database probe application has been refactored to improve code readability, maintainability, and structure while maintaining the exact same behavior.

## Changes Made

### 1. Configuration Management
- **New**: `config/app_config.py` - Centralized configuration management
- **Benefits**: 
  - Easy to modify database settings
  - Configurable worker thread count
  - Type-safe configuration with dataclasses

### 2. Application Architecture
- **New**: `app/probe_application.py` - Main application class
- **Benefits**:
  - Clear separation of concerns
  - Better error handling and logging
  - Comprehensive documentation
  - Proper resource management

### 3. Improved Main Function
- **Refactored**: `main.py` - Now clean and focused
- **Benefits**:
  - Single responsibility (entry point only)
  - Clear documentation
  - Easy to understand flow

## File Structure
```
probe/
├── app/
│   ├── __init__.py
│   └── probe_application.py    # Main application logic
├── config/
│   ├── __init__.py
│   └── app_config.py          # Configuration management
├── main.py                    # Entry point (refactored)
└── ... (existing files unchanged)
```

## Behavior Preservation
- All command line arguments work exactly the same (`--reset`, `--reset-and-exit`)
- Database connection and setup process unchanged
- Worker thread behavior identical
- Signal handling and graceful shutdown preserved
- All existing functionality maintained

## Benefits for Engineers
1. **Readability**: Clear class structure and comprehensive documentation
2. **Maintainability**: Separated concerns and modular design
3. **Testability**: Individual components can be tested in isolation
4. **Configuration**: Easy to modify settings without code changes
5. **Error Handling**: Better error reporting and graceful failure handling
6. **Documentation**: Extensive docstrings and type hints

## Usage
The application works exactly the same as before:
```bash
python main.py                    # Start the probe
python main.py --reset           # Reset database state and start
python main.py --reset-and-exit  # Reset database state and exit
```
