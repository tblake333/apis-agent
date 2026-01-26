"""
Database Change Monitoring Probe

This application monitors database changes in a Firebird database and processes
them using a multi-threaded architecture. It creates triggers on database tables
to capture INSERT, UPDATE, and DELETE operations, then processes these changes
asynchronously and syncs them to the cloud.

Usage:
    python main.py                    # Start the probe
    python main.py --reset           # Reset database state and start
    python main.py --reset-and-exit  # Reset database state and exit
    python main.py --env             # Load configuration from .env file

Environment Variables:
    See .env.example for full list of supported environment variables.
"""

import os
import sys
from app.probe_application import ProbeApplication
from config.app_config import AppConfig
from utils.fdb_helper import get_microsip_fdb_file_path


def load_dotenv_if_available() -> bool:
    """
    Attempt to load .env file using python-dotenv.

    Returns:
        True if .env was loaded, False otherwise.
    """
    try:
        from dotenv import load_dotenv
        # Look for .env in the probe directory (where main.py is)
        env_path = os.path.join(os.path.dirname(__file__), '.env')
        if os.path.exists(env_path):
            load_dotenv(env_path)
            return True
        else:
            print(f"Warning: .env file not found at {env_path}")
            return False
    except ImportError:
        print("Warning: python-dotenv not installed. Using existing environment variables.")
        print("Install with: pip install python-dotenv")
        return False


def main() -> None:
    """
    Main entry point for the database probe application.

    This function initializes the application configuration and starts
    the probe application.
    """
    # Determine configuration method
    use_env = "--env" in sys.argv

    if use_env:
        # Load configuration from .env file
        print("Loading configuration from .env file...")
        load_dotenv_if_available()
        config = AppConfig.from_env()
        if not config.database.path:
            print("Error: PROBE_DB_PATH not set in .env or environment")
            sys.exit(1)
    else:
        # Use default microsip database path discovery
        db_path = get_microsip_fdb_file_path()
        config = AppConfig.from_microsip_path(db_path)

    # Create and run the application
    app = ProbeApplication(config)
    app.run()


if __name__ == "__main__":
    main()
