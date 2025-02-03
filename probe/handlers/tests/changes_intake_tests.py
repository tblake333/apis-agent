import tempfile
from unittest import TestCase

import fdb

class ChangesIntakeTests(TestCase):

    def setUp(self):
        self.tmp_file = tempfile.NamedTemporaryFile(suffix='.fdb', delete=False)
        self.tmp_file.close()
        
        self.conn = fdb.create_database(
            dsn=self.tmp_file.name,
            user='SYSDBA',
            password='masterkey',
            page_size=4096
        )
        

    def test_intake(self) -> None:
