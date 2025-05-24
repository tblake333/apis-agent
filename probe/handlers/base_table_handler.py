from datetime import date
from decimal import Decimal
import json
from queue import Queue
from fdb import Connection
from kafka import KafkaProducer

from models.change import Change

class TableChange:
    pass


class BaseTableHandler:

    TABLE_NAME = "table"

    @staticmethod
    def json_serialize_fallback(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, date):
            return str(obj)
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")



    def __init__(self, conn: Connection, table: str, primary_key: str):
        self.conn = conn
        self.table = table
        self.primary_key = primary_key
        self.producer = KafkaProducer(
            bootstrap_servers='kafka:9093',
            api_version=(3, 9, 0),
            value_serializer=lambda v: json.dumps(v, default=BaseTableHandler.json_serialize_fallback).encode('utf-8')
        )

    def handle_mutation(self, mutation: Change, worker_id: int):
        mutation_type = mutation.mutation
        print(f"[Worker {worker_id}] GOT MUTATION: " + mutation_type)
        if mutation_type == "INSERT":
            self.handle_insert(mutation)
        elif mutation_type == "UPDATE":
            self.handle_update(mutation)
        elif mutation_type == "DELETE":
            self.handle_delete(mutation)
            
    def handle_insert(self, mutation: Change):
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM {self.table} WHERE {self.primary_key} = {mutation.pk_val}")
        mutation_msg = {
            "type": mutation.mutation,
            "table": self.table,
            "row_data": cur.fetchone(),
            "timestamp": mutation.occured_at.timestamp()
        }
        self.producer.send('example-topic', mutation_msg)
    
    def handle_update(self, mutation: Change):
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM {self.table} WHERE {self.primary_key} = {mutation.pk_val}")
        mutation_msg = {
            "type": mutation.mutation,
            "table": self.table,
            "row_data": cur.fetchone(),
            "timestamp": mutation.occured_at.timestamp()
        }
        self.producer.send('example-topic', mutation_msg)
    
    def handle_delete(self, mutation: Change):
        mutation_msg = {
            "type": mutation.mutation,
            "table": self.table,
            "primary_key": self.primary_key,
            "value": mutation.pk_val,
            "timestamp": mutation.occured_at.timestamp()
        }
        self.producer.send('example-topic', mutation_msg)
    
