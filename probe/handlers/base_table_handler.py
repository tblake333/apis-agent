"""
Table change handler for processing database mutations.

This module handles INSERT, UPDATE, and DELETE mutations by fetching
the relevant data and sending it to the cloud sync client.
"""

from fdb import Connection

from models.change import Change
from sync.cloud_sync_client import CloudSyncClient


class TableChange:
    """Placeholder for table change data structure."""
    pass


class BaseTableHandler:
    """
    Handler for processing table mutations.

    This class processes database change events and sends them to the cloud
    via the CloudSyncClient. It replaces the previous Kafka-based implementation
    with a simpler HTTPS-based approach.
    """

    TABLE_NAME = "table"

    def __init__(self, conn: Connection, table: str, primary_key: str, sync_client: CloudSyncClient):
        """
        Initialize the table handler.

        Args:
            conn: Database connection for fetching row data
            table: Name of the table being handled
            primary_key: Name of the primary key column
            sync_client: CloudSyncClient for sending changes to the cloud
        """
        self.conn = conn
        self.table = table
        self.primary_key = primary_key
        self.sync_client = sync_client

    def handle_mutation(self, mutation: Change, worker_id: int):
        """
        Handle a mutation event.

        Routes the mutation to the appropriate handler based on type.

        Args:
            mutation: The change event to process
            worker_id: ID of the worker processing this mutation
        """
        mutation_type = mutation.mutation
        print(f"[Worker {worker_id}] GOT MUTATION: " + mutation_type)
        if mutation_type == "INSERT":
            self.handle_insert(mutation)
        elif mutation_type == "UPDATE":
            self.handle_update(mutation)
        elif mutation_type == "DELETE":
            self.handle_delete(mutation)

    def handle_insert(self, mutation: Change):
        """
        Handle an INSERT mutation.

        Fetches the inserted row data and sends it to the cloud.

        Args:
            mutation: The INSERT change event
        """
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM {self.table} WHERE {self.primary_key} = {mutation.pk_val}")
        row_data = cur.fetchone()
        self.sync_client.send_insert(
            table=self.table,
            row_data=row_data,
            timestamp=mutation.occured_at.timestamp()
        )

    def handle_update(self, mutation: Change):
        """
        Handle an UPDATE mutation.

        Fetches the updated row data and sends it to the cloud.

        Args:
            mutation: The UPDATE change event
        """
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM {self.table} WHERE {self.primary_key} = {mutation.pk_val}")
        row_data = cur.fetchone()
        self.sync_client.send_update(
            table=self.table,
            row_data=row_data,
            timestamp=mutation.occured_at.timestamp()
        )

    def handle_delete(self, mutation: Change):
        """
        Handle a DELETE mutation.

        Sends the deleted row's primary key info to the cloud.

        Args:
            mutation: The DELETE change event
        """
        self.sync_client.send_delete(
            table=self.table,
            primary_key=self.primary_key,
            value=mutation.pk_val,
            timestamp=mutation.occured_at.timestamp()
        )
