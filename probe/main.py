"""
Database Change Monitoring Probe

This application monitors database changes in a Firebird database and processes
them using a multi-threaded architecture. It creates triggers on database tables
to capture INSERT, UPDATE, and DELETE operations, then processes these changes
asynchronously.

Usage:
    python main.py                    # Start the probe
    python main.py --reset           # Reset database state and start
    python main.py --reset-and-exit  # Reset database state and exit
"""

from app.probe_application import ProbeApplication
from config.app_config import AppConfig
from utils.fdb_helper import get_microsip_fdb_file_path


def main() -> None:
    """
    Main entry point for the database probe application.
    
    This function initializes the application configuration and starts
    the probe application.
    """
    # Get database path and create configuration
    db_path = get_microsip_fdb_file_path()
    config = AppConfig.from_microsip_path(db_path)
    
    # Create and run the application
    app = ProbeApplication(config)
    app.run()


if __name__ == "__main__":
    main()
