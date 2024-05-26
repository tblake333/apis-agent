import logging
from fdb import Connection
from utils.apis_types import Mutation
import signal

from listeners.table_listener import TableListener


class BaseTableHandler:

    TABLE_NAME = "table"

    def __init__(self, conn: Connection, table_id: int):
        self.conn = conn
        self.table_id = table_id
        self.listener = TableListener(self.conn, self.table_id)

    def begin(self):
        try:
            self.listener.begin()
            logging.info(f"Starting worker on {self.TABLE_NAME} handler")
            while True:
                mutation, row = self.listener.listen_for_mutation()
                if mutation == Mutation.INSERT:
                    self.handle_insert(row)
                elif mutation == Mutation.UPDATE:
                    self.handle_update(row)
                elif mutation == Mutation.DELETE:
                    self.handle_delete(row)
                else:
                    raise ValueError("Unknown mutation occurred")
        except KeyboardInterrupt:
            print("Interrupted by user!")
            self.listener.close()
        
            
    def handle_insert(self, row: int):
        raise NotImplementedError("handle_insert function not implemented")
    
    def handle_update(self, row: int):
        raise NotImplementedError("handle_update function not implemented")
    
    def handle_delete(self, row: int):
        raise NotImplementedError("handle_delete function not implemented")
    
