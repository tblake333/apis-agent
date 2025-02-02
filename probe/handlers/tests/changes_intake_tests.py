import tempfile
from unittest import TestCase

class ChangesIntakeTests(TestCase):

    def setUp(self):
        self.tmp_file = tempfile.NamedTemporaryFile(suffix='.fdb', delete=False)
        self.tmp_file.close()

        # Create a new Firebird database
        fdb.create_database(
            dsn=self.tmp_file.name,
            user='SYSDBA',
            password='masterkey',
            page_size=4096
        )

        # Connect to the temporary database
        self.con = fdb.connect(
            dsn=self.tmp_file.name,
            user='SYSDBA',
            password='masterkey'
        )
        

    def test_intake(self) -> None:
