from queue import Queue
import tempfile
from unittest import TestCase

import fdb

from handlers.changes_intake import ChangesIntake

class TestChangesIntakeTests(TestCase):

    def setUp(self):
        self.tmp_file = tempfile.NamedTemporaryFile(suffix='.fdb')
        self.tmp_file.close()
        
        self.conn = fdb.create_database(
            dsn=self.tmp_file.name,
            user='SYSDBA',
            password='masterkey',
            page_size=4096
        )
        self.pos = 1
        self.table_name = "test_table"
        self._create_table(self.table_name)
        self.queue = Queue()

        self.changes_intake = ChangesIntake(self.conn, 1, self.queue)
    
    def _create_table(self, table_name: str) -> None:
        test_table_creation_sql_template = """
            CREATE TABLE {table_name}(
                PRIMARY_KEY int not null primary key,
                COL1 int not null,
                COL2 varchar(31)
                );
        """
        self.conn.execute_immediate(test_table_creation_sql_template.format(table_name=table_name))
        self.conn.commit()

    def _insert_row(self, table_name: str, data: dict[str, object]) -> None:
        columns = ', '.join(data.keys())
        values = ', '.join(f"'{value}'" if isinstance(value, str) else str(value) for value in data.values())
        cur = self.conn.cursor()
        sql_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        cur.execute(sql_query)
        self.conn.commit()
    
    def insert_dummy_row(self) -> None:
        data = {
            "PRIMARY_KEY": self.pos,
            "COL1": self.pos + 37,
            "COL2": "dummy_" + str(self.pos)
        }
        self._insert_row(self.table_name, data)
        self.pos += 1
        

    def test_intake(self) -> None:
        self.changes_intake.start()
        self.assertTrue(self.queue.empty())
        self.insert_dummy_row()
        self.assertFalse(self.queue.empty())
        self.changes_intake.stop()
