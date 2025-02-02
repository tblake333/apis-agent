from concurrent.futures import ThreadPoolExecutor
import logging
from queue import Queue
from fdb import Connection
import fdb
from models.change import Change
from handlers.base_table_handler import BaseTableHandler
from models.connection_info import ConnectionInfo
from utils.apis_types import Mutation
from threading import Event, Thread


class ChangesProcessor:

    def __init__(self, conn_info: ConnectionInfo, intake: Queue, id_to_table: dict[int, str], table_primary_keys: dict[str, str]):
        super(ChangesProcessor, self).__init__()
        self.conn_info = conn_info
        self.intake = intake
        self.id_to_table = id_to_table
        self.table_primary_keys = table_primary_keys
        self._stop_event = Event()

    def begin_read(self, worker_id: int):
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
                cur.execute(f"UPDATE CHANGES_LOG SET PROCESSED = 1 WHERE LOG_ID = {change.log_id}")
                conn.commit()
            except Exception as e:
                print("EXCEPTION ON WORKER " + worker_id)
                print(e)
        conn.close()
        print("exiting worker")
    
    def process_change(self, conn: fdb.Connection, change: Change, worker_id: int):
        table_name = self.id_to_table[change.table_id]
        primary_key = self.table_primary_keys[table_name]
        table_handler = BaseTableHandler(conn, table_name, primary_key)
        table_handler.handle_mutation(change, worker_id)
    
