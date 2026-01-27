"""
Changes processor for handling database mutations.

This module processes change events from the intake queue, fetches relevant
data, and sends mutations to the cloud via the sync client.
"""

from concurrent.futures import ThreadPoolExecutor
import logging
from queue import Queue
from fdb import Connection
import fdb
from models.change import Change
from handlers.base_table_handler import BaseTableHandler
from models.connection_info import ConnectionInfo
from sync.cloud_sync_client import CloudSyncClient
from utils.apis_types import Mutation
from threading import Event, Thread


class ChangesProcessor:
    """
    Processes database change events and sends them to the cloud.

    This class manages worker threads that consume change events from a queue,
    fetch relevant row data, and send the mutations to the cloud sync client.
    """

    def __init__(
        self,
        conn_info: ConnectionInfo,
        intake: Queue,
        id_to_table: dict[int, str],
        table_primary_keys: dict[str, str],
        sync_client: CloudSyncClient
    ):
        """
        Initialize the changes processor.

        Args:
            conn_info: Database connection information
            intake: Queue of change events to process
            id_to_table: Mapping from table ID to table name
            table_primary_keys: Mapping from table name to primary key column
            sync_client: CloudSyncClient for sending changes to the cloud
        """
        super(ChangesProcessor, self).__init__()
        self.conn_info = conn_info
        self.intake = intake
        self.id_to_table = id_to_table
        self.table_primary_keys = table_primary_keys
        self.sync_client = sync_client
        self._stop_event = Event()

    def begin_read(self, worker_id: int):
        """
        Start reading changes from the intake queue.

        This method runs in a worker thread and processes changes until
        it receives a None sentinel value.

        Args:
            worker_id: ID of this worker thread
        """
        conn = fdb.connect(
            dsn=self.conn_info.db_path,
            user=self.conn_info.db_user,
            password=self.conn_info.db_password
        )
        while True:
            try:
                change: Change = self.intake.get()
                if not change:
                    self.intake.put(change)
                    break
                self.process_change(conn, change, worker_id)
                cur = conn.cursor()
                cur.execute("UPDATE CHANGES_LOG SET PROCESSED = 1 WHERE LOG_ID = ?", (change.log_id,))
                conn.commit()
            except Exception as e:
                print(f"EXCEPTION ON WORKER {worker_id}")
                print(e)
        conn.close()
        print("exiting worker")

    def process_change(self, conn: fdb.Connection, change: Change, worker_id: int):
        """
        Process a single change event.

        Creates a table handler and delegates mutation handling to it.

        Args:
            conn: Database connection
            change: The change event to process
            worker_id: ID of the worker processing this change
        """
        table_name = self.id_to_table[change.table_id]
        primary_key = self.table_primary_keys[table_name]
        table_handler = BaseTableHandler(conn, table_name, primary_key, self.sync_client)
        table_handler.handle_mutation(change, worker_id)
