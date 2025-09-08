import contextlib
import io
import logging
from queue import Queue
from fdb import Connection
import fdb
from models.change import Change
from utils.apis_types import Mutation
from threading import Event, Thread, Lock
from models.connection_info import ConnectionInfo

logger = logging.getLogger(__name__)

class ChangesIntake(Thread):

    TABLE_NAME = "CHANGES_LOG"
    EVENT_NAME = "INTAKE_SIGNAL"
    PK_COL_NAME = "LOG_ID"

    def __init__(self, conn_info: ConnectionInfo, pos: int, output: Queue):
        super(ChangesIntake, self).__init__()
        self.conn_info = conn_info
        self.conn = None
        self.events = [ChangesIntake.EVENT_NAME]
        self.pos = pos
        self.output = output
        self._stop_event = Event()
        self._conn_lock = Lock()
    
    def stopped(self):
        return self._stop_event.is_set()
    
    def stop(self):
        self._stop_event.set()
        with self._conn_lock:
            if self.conn:
                try:
                    self.conn.execute_immediate(f"EXECUTE BLOCK AS BEGIN POST_EVENT '{self.EVENT_NAME}'; END")
                    self.conn.commit()
                except Exception as e:
                    logger.error(f"Error posting event during stop: {e}")
                finally:
                    try:
                        self.conn.close()
                    except Exception as e:
                        logger.error(f"Error closing connection during stop: {e}")
                    self.conn = None

    def _get_connection(self) -> Connection:
        """Get a new database connection."""
        with self._conn_lock:
            if self.conn:
                try:
                    self.conn.close()
                except Exception as e:
                    logger.error(f"Error closing old connection: {e}")
            
            try:
                self.conn = fdb.connect(
                    dsn=self.conn_info.db_path,
                    user=self.conn_info.db_user,
                    password=self.conn_info.db_password,
                    charset='UTF8'
                )
                return self.conn
            except Exception as e:
                logger.error(f"Error creating new connection: {e}")
                raise

    def _process_changes(self, conn: Connection):
        """Process changes from the database."""
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM {self.TABLE_NAME} WHERE {self.PK_COL_NAME} >= {str(self.pos)} AND PROCESSED = 0")
            logger.debug(f"Current position: {self.pos}")
            rows = cur.fetchall()
            for row in rows:
                change = Change(*row)
                self.output.put(change)
            self.pos += len(rows)
        except Exception as e:
            logger.error(f"Error processing changes: {e}")
            raise

    def run(self):
        """Main intake loop."""
        logger.info(f"Starting intake on {self.TABLE_NAME} table")
        while not self.stopped():
            try:
                with self._conn_lock:
                    if not self.conn:
                        self.conn = self._get_connection()
                    
                    with contextlib.redirect_stderr(io.StringIO()), self.conn.event_conduit(event_names=self.events) as conduit:
                        conduit.wait()
                        logger.info("Received change event")
                        if self.stopped():
                            break
                        self._process_changes(self.conn)
            except fdb.Error as e:
                logger.error(f"Database error in intake loop: {e}")
                with self._conn_lock:
                    if self.conn:
                        try:
                            self.conn.close()
                        except Exception as close_error:
                            logger.error(f"Error closing connection after error: {close_error}")
                        self.conn = None
                if not self.stopped():
                    logger.info("Retrying after database error...")
                    continue
                break
            except Exception as e:
                logger.error(f"Unexpected error in intake loop: {e}")
                if not self.stopped():
                    logger.info("Retrying after error...")
                    continue
                break

        logger.info("Stopping changes intake!")
        with self._conn_lock:
            if self.conn:
                try:
                    self.conn.close()
                except Exception as e:
                    logger.error(f"Error closing connection during shutdown: {e}")
                self.conn = None
    
