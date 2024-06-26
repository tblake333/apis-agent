import logging
from queue import Queue
from fdb import Connection
from utils.apis_types import Mutation
import signal

from listeners.table_listener import TableListener

class TableChange:
    pass


class BaseTableHandler:

    TABLE_NAME = "table"

    def __init__(self, conn: Connection, table_id: int):
        self.conn = conn
        self.table_id = table_id
        self.queue = Queue(maxsize=10)


    def begin(self):
        try:
            logging.info(f"Starting worker on {self.TABLE_NAME} handler")
            while True:
                data = self.queue.get()
                self.handle_row(data)
                mutation = row_data.mutation
        except KeyboardInterrupt:
            print("Interrupted by user!")
            self.listener.close()

    def handle_row_change(change_data: TableChange) -> None:
        
        
            
    def handle_insert(self, row: int):
        raise NotImplementedError("handle_insert function not implemented")
    
    def handle_update(self, row: int):
        raise NotImplementedError("handle_update function not implemented")
    
    def handle_delete(self, row: int):
        raise NotImplementedError("handle_delete function not implemented")
    
