from queue import Queue
from threading import Thread
import fdb
import sys
import signal

from concurrent.futures import ThreadPoolExecutor

from handlers.changes_intake import ChangesIntake
from handlers.changes_processor import ChangesProcessor
from models.connection_info import ConnectionInfo
from utils.fdb_helper import create_table_triggers, delete_processed_mutations, ensure_clean_slate, get_microsip_fdb_file_path, get_table_names, get_table_to_primary_key, create_changes_log_table, process_leftover_mutations, reset_state

if __name__ == "__main__":

    DB_PATH = get_microsip_fdb_file_path()
    DB_USER = "sysdba"
    DB_PASSWORD = "masterkey"

    con = fdb.connect(dsn=DB_PATH, user=DB_USER, password=DB_PASSWORD, charset='UTF8')

    cur = con.cursor()

    table_to_primary_key = get_table_to_primary_key(cur)

    if "--reset-and-exit" in sys.argv:
        reset_state(con, cur, table_to_primary_key)
        exit()

    if "--reset" in sys.argv:
        reset_state(con, cur, table_to_primary_key)

    table_names = get_table_names(cur)
    if "CHANGES_LOG" not in table_names:
        print("no changes_log table detected. creating one...")
        create_changes_log_table(con)
    else:
        print("changes_log table found. skipping creation...")


    table_to_id, id_to_table = create_table_triggers(con, cur, table_to_primary_key)

    process_leftover_mutations(con, id_to_table, table_to_primary_key)

    delete_processed_mutations(con)

    ensure_clean_slate(con)

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

    
    
