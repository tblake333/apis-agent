import os
from sys import platform

import fdb


def get_microsip_fdb_file_path() -> str:
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
    
    return microsip_dir + most_recent_fdb_file

def get_table_names(cur: fdb.Cursor) -> list[str]:
    tables_query = "SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG = 0;"
    cur.execute(tables_query)
    table_names = [row[0].strip() for row in cur.fetchall()]

    return table_names

def get_primary_key_name(cur: fdb.Cursor, table: str) -> str:
    pk_sql_template = """
    SELECT RDB$FIELD_NAME
            FROM RDB$RELATION_FIELDS
            WHERE RDB$RELATION_NAME = '{table}'
            AND RDB$FIELD_POSITION = 0;
    """
    cur.execute(pk_sql_template.format(table=table))
    pk_column_name = cur.fetchone()[0].strip()
    return pk_column_name

def create_changes_log_table(conn: fdb.Connection) -> None:
    changes_log_table_sql = """
        CREATE TABLE CHANGES_LOG(
            LOG_ID int not null primary key,
            PK_VAL int not null,
            TABLE_ID int not null,
            MUTATION varchar(31),
            OCCURRED_AT TIMESTAMP );
    """
    changes_log_trigger_sql_template = """
        CREATE OR ALTER TRIGGER INTAKE_TRIGGER
            FOR CHANGES_LOG
            ACTIVE AFTER INSERT OR UPDATE OR DELETE POSITION 10
        AS
        BEGIN
            POST_EVENT 'INTAKE_SIGNAL';
        END
        """
    ## TODO: Refactor this?
    changes_seq_sql = "CREATE SEQUENCE SEQ_CHANGES_LOG;"
    conn.execute_immediate(changes_log_table_sql)
    conn.execute_immediate(changes_seq_sql)
    conn.execute_immediate(changes_log_trigger_sql_template)
    conn.commit()

def create_table_trigger(conn: fdb.Connection, cur: fdb.Cursor, table: str, table_id: int) -> None:
    trigger_sql_template = """
        CREATE OR ALTER TRIGGER TABLE_{table_id}_CHANGES
            FOR {table}
            ACTIVE AFTER INSERT OR UPDATE OR DELETE POSITION 10
        AS
        DECLARE VARIABLE primary_key_value INTEGER;
        DECLARE VARIABLE mutation VARCHAR(6);
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
            
            INSERT INTO CHANGES_LOG (LOG_ID, PK_VAL, TABLE_ID, MUTATION, OCCURRED_AT)
                VALUES (NEXT VALUE FOR SEQ_CHANGES_LOG, :primary_key_value, {table_id}, :mutation, current_timestamp);
        END
        """
    pk_column_name = get_primary_key_name(cur, table)
    conn.execute_immediate(trigger_sql_template.format(table=table, table_id=table_id, pk_column_name=pk_column_name))
    conn.commit()

def create_table_triggers(conn: fdb.Connection, cur: fdb.Cursor) -> tuple[dict[str, int], dict[int, str]]:
    table_names = get_table_names(cur)
    id = 0
    table_to_id = {}
    id_to_table = {}
    for table in table_names:
        if table == "CHANGES_LOG":
            continue
        table_to_id[table] = id
        id_to_table[id] = table

        try:
            create_table_trigger(conn, cur, table, id)
            id = id + 1
        except Exception as e:
            print("unable to create trigger for table: " + table)
            print(e)
    
    return table_to_id, id_to_table

def reset_state(conn: fdb.Connection, cur: fdb.Cursor) -> None:
    print("attempting to reset state...")
    table_names = get_table_names(cur)
    id = 0
    for table in table_names:
        if table == "CHANGES_LOG":
            continue
        try:
            conn.execute_immediate(f"DROP TRIGGER TABLE_{id}_CHANGES;")
            conn.commit()
        except Exception as e:
            print("unable to drop trigger for table: " + table)
            print(e)
            exit()
        id = id + 1
    
    print("successfully dropped all table triggers!")
    print("now dropping changes_log table and sequence...")
    try:
        conn.execute_immediate(f"DROP TRIGGER INTAKE_TRIGGER;")
        conn.execute_immediate(f"DROP TABLE CHANGES_LOG;")
        conn.execute_immediate(f"DROP SEQUENCE SEQ_CHANGES_LOG;")
        conn.commit()
    except Exception as e:
        print("unable to drop changes_log table and sequence")
        print(e)
        exit()
    print("state reset successfully!")
