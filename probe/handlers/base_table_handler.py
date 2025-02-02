import logging
from queue import Queue
from fdb import Connection

from models.change import Change

class TableChange:
    pass


class BaseTableHandler:

    TABLE_NAME = "table"

    def __init__(self, conn: Connection, table: str, primary_key: str):
        self.conn = conn
        self.table = table
        self.primary_key = primary_key

    def handle_mutation(self, mutation: Change, worker_id: int):
        mutation_type = mutation.mutation
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM {self.table} WHERE {self.primary_key} = {mutation.pk_val}")
        print(f"[Worker {worker_id}] GOT MUTATION: " + str(cur.fetchone()))
        if mutation_type == "INSERT":
            self.handle_insert(mutation)
        elif mutation_type == "UPDATE":
            self.handle_update(mutation)
        elif mutation_type == "DELETE":
            self.handle_delete(mutation)
        
    
        
            
    def handle_insert(self, mutation: Change):
        pass
    
    def handle_update(self, mutation: Change):
        pass
    
    def handle_delete(self, mutation: Change):
        pass
    
