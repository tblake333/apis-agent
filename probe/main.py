import fdb
import sys

from concurrent.futures import ThreadPoolExecutor

from handlers.table_handler_factory import TableHandlerFactory
from utils.fdb_helper import create_table_triggers, get_microsip_fdb_file_path, get_table_names, create_changes_log_table, reset_state

if __name__ == "__main__":

    DB_PATH = get_microsip_fdb_file_path()
    DB_USER = "sysdba"
    DB_PASSWORD = "masterkey"

    con = fdb.connect(dsn=DB_PATH, user=DB_USER, password=DB_PASSWORD, charset='UTF8')

    cur = con.cursor()

    if "--reset-and-exit" in sys.argv:
        reset_state(con, cur)
        exit()

    if "--reset" in sys.argv:
        reset_state(con, cur)

    table_names = get_table_names(cur)
    if "CHANGES_LOG" not in table_names:
        print("no changes_log table detected. creating one...")
        create_changes_log_table(con)
    else:
        print("changes_log table found. skipping creation...")

    table_to_id, id_to_table = create_table_triggers(con, cur)
        
    with ThreadPoolExecutor(max_workers=len(table_to_id)) as executor:
        for table in table_to_id:
            base_handler = TableHandlerFactory.create(table, con)
            if base_handler is not None:
                executor.submit(base_handler.begin)


    triggers_query = "SELECT RDB$TRIGGER_NAME FROM RDB$TRIGGERS WHERE RDB$SYSTEM_FLAG = 0;"
    cur.execute(triggers_query)

    trigger_names = [row[0].strip() for row in cur.fetchall()]

    cur.close()
    con.close()

    print(len(trigger_names))