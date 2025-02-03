from queue import Queue
import fdb
import sys
import signal

from concurrent.futures import ThreadPoolExecutor

from handlers.changes_intake import ChangesIntake
from handlers.changes_processor import ChangesProcessor
from models.connection_info import ConnectionInfo
from database.database_manager import DatabaseManager
from utils.fdb_helper import get_microsip_fdb_file_path

if __name__ == "__main__":

    DB_PATH = get_microsip_fdb_file_path()
    DB_USER = "sysdba"
    DB_PASSWORD = "masterkey"

    con = fdb.connect(dsn=DB_PATH, user=DB_USER, password=DB_PASSWORD, charset='UTF8')

    db_manager = DatabaseManager(con)

    cur = con.cursor()

    if "--reset-and-exit" in sys.argv:
        db_manager.reset_state()
        exit()

    if "--reset" in sys.argv:
        db_manager.reset_state()

    id_to_table, table_to_primary_key= db_manager.setup()
    db_manager.ensure_clean_slate(con)

    output = Queue()

    intake = ChangesIntake(con, 1, output)
    intake.start()
    
    def signal_handler(sig, frame):
        print('Exiting gracefully...')
        output.put(None)
        intake.stop()
        con.execute_immediate(f"EXECUTE BLOCK AS BEGIN POST_EVENT '{ChangesIntake.EVENT_NAME}'; END")
        con.commit()
        intake.join()
        con.close()
    
    signal.signal(signal.SIGINT, signal_handler)

    conn_info = ConnectionInfo(db_path=DB_PATH, db_user=DB_USER, db_password=DB_PASSWORD)

    changes_processor = ChangesProcessor(conn_info, output, id_to_table, table_to_primary_key)
        
    with ThreadPoolExecutor(max_workers=10) as executor:
        print("starting workers")
        for i in range(10):
            executor.submit(changes_processor.begin_read, i)

    
    
