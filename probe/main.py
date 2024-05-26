import fdb
from sys import platform
import os

from concurrent.futures import ThreadPoolExecutor

from handlers.table_handler_factory import TableHandlerFactory
from utils.values import ID_TO_TABLE, TABLE_TO_ID

if __name__ == "__main__":
    microsip_dir = ""
    if platform == "linux" or platform == "linux2" or platform == "darwin":
        microsip_dir = "/Microsip datos/"
    elif platform == "win32":
        microsip_dir = "C:\Microsip datos\\"

    if not os.path.isdir(microsip_dir):
        print("Microsip data not found!")
        exit()

    fdb_files = [f for f in os.listdir(microsip_dir) if f.lower().endswith(".fdb")]

    if len(fdb_files) == 0:
        print("FDB file not found!")
        exit()
    elif len(fdb_files) > 1:
        print("Multiple FDB files found: " + ", ".join(fdb_files))
    else:
        print("Found FDB file: " + fdb_files[0])


    most_recent_fdb_modify_time = 0
    most_recent_fdb_file = ""
    for f in fdb_files:
        full_path = microsip_dir + f
        mtime = os.path.getmtime(full_path)
        if mtime > most_recent_fdb_modify_time:
            most_recent_fdb_modify_time = mtime
            most_recent_fdb_file = f

    if len(fdb_files) > 1:
        print("Choosing most recently modified FDB file: " + most_recent_fdb_file)

    DB_PATH = microsip_dir + most_recent_fdb_file
    DB_USER = "sysdba"
    DB_PASSWORD = "masterkey"

    con = fdb.connect(dsn=DB_PATH, user=DB_USER, password=DB_PASSWORD, charset='UTF8')

    cur = con.cursor()

    tables_query = "SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG = 0;"
    triggers_query = "SELECT RDB$TRIGGER_NAME FROM RDB$TRIGGERS WHERE RDB$SYSTEM_FLAG = 0;"
    cur.execute(tables_query)

    table_names = [row[0].strip() for row in cur.fetchall()]

    pk_sql_template = """
    SELECT RDB$FIELD_NAME
            FROM RDB$RELATION_FIELDS
            WHERE RDB$RELATION_NAME = '{table}'
            AND RDB$FIELD_POSITION = 0;
    """

    trigger_sql_template = """
        CREATE OR ALTER TRIGGER TABLE_{table_id}_CHANGES
            FOR {table}
            ACTIVE AFTER INSERT OR UPDATE OR DELETE POSITION 10
        AS
        DECLARE VARIABLE primary_key_value INTEGER;
        DECLARE VARIABLE mutation VARCHAR(6);
        DECLARE VARIABLE pow INTEGER = 0;
        DECLARE VARIABLE remainder INTEGER;
        BEGIN
            primary_key_value = CASE
                    WHEN INSERTING THEN NEW.{pk_column_name}
                    WHEN UPDATING THEN NEW.{pk_column_name}
                    WHEN DELETING THEN OLD.{pk_column_name}
                    END;
            mutation = CASE
                    WHEN INSERTING THEN 'INSERT'
                    WHEN UPDATING THEN 'UPDATE'
                    WHEN DELETING THEN 'DELETE'
                    END;
            
            POST_EVENT 'TABLE_{table_id}_CHANGE';
            POST_EVENT mutation;

            WHILE (primary_key_value > 0 AND pow < 64) DO
            BEGIN
                remainder = BIN_AND(primary_key_value, 1);
                IF (remainder = 1) THEN
                BEGIN
                    POST_EVENT CAST(pow AS VARCHAR(2));
                END
                primary_key_value = BIN_SHR(primary_key_value, 1);
                pow = pow + 1;
            END

            POST_EVENT 'TABLE_{table_id}_CHANGE_END';
        END
        """

    
    
    id = 0
    for table in table_names:
        # try:
        #     con.execute_immediate("DROP TRIGGER " + table + "_CHANGES;")
        #     con.commit()
        # except:
        #     print("unable to process " + table)
        TABLE_TO_ID[table] = id
        ID_TO_TABLE[id] = table


        try:
            cur.execute(pk_sql_template.format(table=table))
            pk_column_name = cur.fetchone()[0].strip()

            con.execute_immediate(trigger_sql_template.format(table=table, table_id=id, pk_column_name=pk_column_name))
            con.commit()
            id = id + 1
        except Exception as e:
            print("unable to process " + table)
            print(e)
        
    with ThreadPoolExecutor(max_workers=len(TABLE_TO_ID)) as executor:
        for table in TABLE_TO_ID:
            base_handler = TableHandlerFactory.create(table, con)
            if base_handler is not None:
                executor.submit(base_handler.begin)


    cur.execute(triggers_query)

    trigger_names = [row[0].strip() for row in cur.fetchall()]

    cur.close()
    con.close()

    print(len(trigger_names))