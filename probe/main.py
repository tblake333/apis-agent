from queue import Queue
import fdb
import sys
import signal
import logging
from typing import Dict, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

from handlers.changes_intake import ChangesIntake
from handlers.changes_processor import ChangesProcessor
from models.connection_info import ConnectionInfo
from database.database_manager import DatabaseManager
from utils.fdb_helper import get_microsip_fdb_file_path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    path: str
    user: str
    password: str
    charset: str = 'UTF8'

class Application:
    """Main application class that manages the lifecycle of the change tracking system."""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
        self.db_manager = None
        self.output_queue = Queue()
        self.intake = None
        self.changes_processor = None
        self.executor = None
        self._stop_event = False

    @contextmanager
    def database_connection(self):
        """Context manager for database connection."""
        try:
            self.connection = fdb.connect(
                dsn=self.config.path,
                user=self.config.user,
                password=self.config.password,
                charset=self.config.charset
            )
            yield self.connection
        except fdb.Error as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if self.connection:
                self.connection.close()

    def setup(self) -> Tuple[Dict[int, str], Dict[str, str]]:
        """Initialize the database and return table mappings."""
        with self.database_connection() as conn:
            self.db_manager = DatabaseManager(conn)
            
            if "--reset-and-exit" in sys.argv:
                self.db_manager.reset_state()
                sys.exit(0)

            if "--reset" in sys.argv:
                self.db_manager.reset_state()

            id_to_table, table_to_primary_key = self.db_manager.setup()
            self.db_manager.ensure_clean_slate(conn)
            return id_to_table, table_to_primary_key

    def start_intake(self, conn: fdb.Connection):
        """Start the changes intake process."""
        conn_info = ConnectionInfo(
            db_path=self.config.path,
            db_user=self.config.user,
            db_password=self.config.password
        )
        self.intake = ChangesIntake(conn_info, 1, self.output_queue)
        self.intake.start()
        logger.info("Changes intake process started")

    def start_processor(self, id_to_table: Dict[int, str], table_to_primary_key: Dict[str, str]):
        """Start the changes processor with worker threads."""
        conn_info = ConnectionInfo(
            db_path=self.config.path,
            db_user=self.config.user,
            db_password=self.config.password
        )
        
        self.changes_processor = ChangesProcessor(
            conn_info,
            self.output_queue,
            id_to_table,
            table_to_primary_key
        )

        self.executor = ThreadPoolExecutor(max_workers=10)
        logger.info("Starting worker threads")
        for i in range(10):
            self.executor.submit(self.changes_processor.begin_read, i)

    def stop(self):
        """Gracefully stop all processes."""
        logger.info("Stopping application...")
        self._stop_event = True
        
        if self.output_queue:
            self.output_queue.put(None)
        
        if self.intake:
            self.intake.stop()
            self.intake.join()
        
        if self.executor:
            self.executor.shutdown(wait=True)
        
        logger.info("Application stopped")
        sys.exit(0)  # Force exit after cleanup

    def run(self):
        """Main application entry point."""
        try:
            # Setup database and get table mappings
            id_to_table, table_to_primary_key = self.setup()
            
            # Start the intake process
            with self.database_connection() as conn:
                self.start_intake(conn)
            
            # Start the processor
            self.start_processor(id_to_table, table_to_primary_key)
            
            # Setup signal handlers
            def signal_handler(sig, frame):
                logger.info(f"Received signal {sig}")
                self.stop()
            
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            
            # Keep the main thread alive
            while not self._stop_event:
                signal.pause()
                
        except Exception as e:
            logger.error(f"Application error: {e}")
            self.stop()

def main():
    """Application entry point."""
    config = DatabaseConfig(
        path=get_microsip_fdb_file_path(),
        user="sysdba",
        password="masterkey"
    )
    
    app = Application(config)
    app.run()

if __name__ == "__main__":
    main()

    
    
