"""
Main application class for the database probe.
"""
import fdb
import signal
import sys
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from config.app_config import AppConfig
from handlers.changes_intake import ChangesIntake
from handlers.changes_processor import ChangesProcessor
from models.connection_info import ConnectionInfo
from database.database_manager import DatabaseManager
from sync.cloud_sync_client import CloudSyncClient


class ProbeApplication:
    """
    Main application class that orchestrates the database change monitoring.

    This class manages the lifecycle of the probe application including:
    - Database connection and setup
    - Cloud sync client initialization
    - Change intake monitoring
    - Change processing with worker threads
    - Graceful shutdown handling
    """

    def __init__(self, config: AppConfig):
        """
        Initialize the probe application.

        Args:
            config: Application configuration
        """
        self.config = config
        self.connection: Optional[fdb.Connection] = None
        self.db_manager: Optional[DatabaseManager] = None
        self.sync_client: Optional[CloudSyncClient] = None
        self.changes_intake: Optional[ChangesIntake] = None
        self.changes_processor: Optional[ChangesProcessor] = None
        self.output_queue: Optional[Queue] = None
        self.executor: Optional[ThreadPoolExecutor] = None
        self._shutdown_requested = False

    def setup_database_connection(self) -> None:
        """Establish database connection and setup database manager."""
        print(f"Connecting to database: {self.config.database.path}")
        self.connection = fdb.connect(
            dsn=self.config.database.path,
            user=self.config.database.user,
            password=self.config.database.password,
            charset=self.config.database.charset
        )
        self.db_manager = DatabaseManager(self.connection)
        print("Database connection established successfully")

    def setup_cloud_sync(self) -> None:
        """Initialize the cloud sync client."""
        if not self.config.cloud_sync.enabled:
            print("Cloud sync is disabled")
            return

        print(f"Setting up cloud sync to: {self.config.cloud_sync.endpoint}")
        self.sync_client = CloudSyncClient(
            endpoint=self.config.cloud_sync.endpoint,
            api_key=self.config.cloud_sync.api_key,
            buffer_path=self.config.cloud_sync.buffer_path,
            enable_background_retry=self.config.cloud_sync.enable_background_retry
        )
        print("Cloud sync client initialized")

        # Check for buffered events from previous runs
        stats = self.sync_client.get_buffer_stats()
        if stats['pending_count'] > 0:
            print(f"Found {stats['pending_count']} buffered events from previous runs")

    def handle_command_line_args(self) -> None:
        """Handle command line arguments for reset operations."""
        if "--reset-and-exit" in sys.argv:
            print("Resetting database state and exiting...")
            self.db_manager.reset_state()
            sys.exit(0)

        if "--reset" in sys.argv:
            print("Resetting database state...")
            self.db_manager.reset_state()

    def setup_database_schema(self) -> tuple[dict[int, str], dict[str, str]]:
        """
        Setup database schema and return table mappings.

        Returns:
            Tuple of (id_to_table, table_to_primary_key) mappings
        """
        print("Setting up database schema...")
        id_to_table, table_to_primary_key = self.db_manager.setup(self.sync_client)
        self.db_manager.ensure_clean_slate(self.connection)
        print("Database schema setup completed")
        return id_to_table, table_to_primary_key

    def setup_change_monitoring(self, id_to_table: dict[int, str], table_to_primary_key: dict[str, str]) -> None:
        """
        Setup change monitoring components.

        Args:
            id_to_table: Mapping from table ID to table name
            table_to_primary_key: Mapping from table name to primary key column
        """
        print("Setting up change monitoring...")

        # Create output queue for change processing
        self.output_queue = Queue()

        # Create connection info for intake and processor
        conn_info = ConnectionInfo(
            db_path=self.config.database.path,
            db_user=self.config.database.user,
            db_password=self.config.database.password
        )

        # Setup changes intake
        self.changes_intake = ChangesIntake(
            conn_info,
            self.config.workers.intake_position,
            self.output_queue
        )

        # Setup changes processor
        self.changes_processor = ChangesProcessor(
            conn_info,
            self.output_queue,
            id_to_table,
            table_to_primary_key,
            self.sync_client
        )

        print("Change monitoring setup completed")

    def setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self._signal_handler)
        print("Signal handlers configured")

    def _signal_handler(self, sig: int, frame) -> None:
        """
        Handle shutdown signals.

        Args:
            sig: Signal number
            frame: Current stack frame
        """
        print('Shutdown signal received. Exiting gracefully...')
        self._shutdown_requested = True
        self.shutdown()

    def start_workers(self) -> None:
        """Start worker threads for change processing."""
        print(f"Starting {self.config.workers.max_workers} worker threads...")

        self.executor = ThreadPoolExecutor(max_workers=self.config.workers.max_workers)

        for i in range(self.config.workers.max_workers):
            self.executor.submit(self.changes_processor.begin_read, i)

        print("Worker threads started successfully")

    def start_change_intake(self) -> None:
        """Start the change intake monitoring thread."""
        print("Starting change intake monitoring...")
        self.changes_intake.start()
        print("Change intake monitoring started")

    def run(self) -> None:
        """
        Run the main application loop.

        This method orchestrates the entire application lifecycle.
        """
        try:
            # Setup phase
            self.setup_database_connection()
            self.setup_cloud_sync()
            self.handle_command_line_args()
            id_to_table, table_to_primary_key = self.setup_database_schema()
            self.setup_change_monitoring(id_to_table, table_to_primary_key)
            self.setup_signal_handlers()

            # Start processing
            self.start_change_intake()
            self.start_workers()

            print("Probe application is running. Press Ctrl+C to stop.")

            # Keep the main thread alive
            self.changes_intake.join()

        except Exception as e:
            print(f"Application error: {e}")
            self.shutdown()
            raise

    def shutdown(self) -> None:
        """
        Gracefully shutdown the application.

        This method ensures all resources are properly cleaned up.
        """
        print("Shutting down application...")

        try:
            # Signal ALL workers to stop (one None per worker)
            if self.output_queue is not None:
                for _ in range(self.config.workers.max_workers):
                    self.output_queue.put(None)

            if self.changes_intake is not None:
                self.changes_intake.stop()

                # Trigger the intake event to wake up the thread
                if self.connection is not None:
                    self.connection.execute_immediate(
                        f"EXECUTE BLOCK AS BEGIN POST_EVENT '{ChangesIntake.EVENT_NAME}'; END"
                    )
                    self.connection.commit()

                self.changes_intake.join(timeout=5.0)  # Don't wait forever

            # Shutdown executor
            if self.executor is not None:
                self.executor.shutdown(wait=True, cancel_futures=True)

            # Close sync client
            if self.sync_client is not None:
                # Attempt to flush any remaining buffered events
                stats = self.sync_client.get_buffer_stats()
                if stats['pending_count'] > 0:
                    print(f"Attempting to flush {stats['pending_count']} buffered events...")
                    sent = self.sync_client.flush_buffer()
                    print(f"Flushed {sent} events")
                self.sync_client.close()

            # Close database connection
            if self.connection is not None:
                self.connection.close()

            print("Application shutdown completed")

        except Exception as e:
            print(f"Error during shutdown: {e}")
