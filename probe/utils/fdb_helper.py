from functools import reduce
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

def delete_processed_mutations(conn: fdb.Connection):
    print("deleting processed mutations...")
    cur = conn.cursor()
    cur.execute("DELETE FROM CHANGES_LOG WHERE PROCESSED = 1")
    conn.commit()

def process_leftover_mutations(conn: fdb.Connection, id_to_table, table_to_primary_key):
    from handlers.base_table_handler import BaseTableHandler
    from models.change import Change
    
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM CHANGES_LOG WHERE PROCESSED = 0")
    rows = cur.fetchall()
    if len(rows) != 0:
        print("Processing leftover mutations...")
    for row in rows:
        change = Change(*row)
        table_name = id_to_table[change.table_id]
        primary_key = table_to_primary_key[table_name]
        table_handler = BaseTableHandler(conn, table_name, primary_key)
        table_handler.handle_mutation(change, "Main")
        processed_cur = conn.cursor()
        processed_cur.execute(f"UPDATE CHANGES_LOG SET PROCESSED = 1 WHERE LOG_ID = {change.log_id}")
        conn.commit()

def ensure_clean_slate(conn: fdb.Connection):
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM CHANGES_LOG")
    result = cur.fetchone()[0]
    if int(result) != 0:
        print("CHANGES_LOG not empty, exiting...")
        exit()


def get_table_names(cur: fdb.Cursor) -> list[str]:
    tables_query = "SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG = 0;"
    cur.execute(tables_query)
    table_names = [row[0].strip() for row in cur.fetchall()]

    return table_names

def get_table_to_primary_key(cur: fdb.Cursor) -> dict[str, str]:
    table_primary_keys = get_table_primary_keys(cur)
    table_names = get_table_names(cur)
    table_to_primary_key: dict[str, str] = {}
    id = 0
    for table in table_names:
        if table == "CHANGES_LOG":
            continue
        if table not in table_primary_keys:
            continue
        if len(table_primary_keys[table]) != 1 or get_column_datatype(cur, table, pk_column_name := table_primary_keys[table][0]) != "INTEGER":
            continue
            
        table_to_primary_key[table] = pk_column_name
        id = id + 1
    return table_to_primary_key

def get_column_datatype(cur: fdb.Cursor, table: str, column_name: str) -> str:
    cur.execute(f"""
                SELECT
                CASE f.RDB$FIELD_TYPE
                WHEN 261 THEN 'BLOB'
                WHEN 14 THEN 'CHAR'
                WHEN 40 THEN 'CSTRING'
                WHEN 11 THEN 'D_FLOAT'
                WHEN 27 THEN 'DOUBLE'
                WHEN 10 THEN 'FLOAT'
                WHEN 16 THEN 'INT64'
                WHEN 8 THEN 'INTEGER'
                WHEN 9 THEN 'QUAD'
                WHEN 7 THEN 'SMALLINT'
                WHEN 12 THEN 'DATE'
                WHEN 13 THEN 'TIME'
                WHEN 35 THEN 'TIMESTAMP'
                WHEN 37 THEN 'VARCHAR'
                ELSE 'UNKNOWN'
                END AS field_type 
        FROM RDB$RELATION_FIELDS r
        LEFT JOIN RDB$FIELDS f ON r.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
        WHERE r.RDB$RELATION_NAME='{table}'  -- table name
        AND r.RDB$FIELD_NAME='{column_name}'
        ORDER BY r.RDB$FIELD_POSITION
                """)
    row = cur.fetchone()
    return row[0].strip()

def get_table_primary_keys(cur: fdb.Cursor) -> dict[str, tuple]:
    tables_query = """
    select ix.rdb$index_name as index_name,
    sg.rdb$field_name as field_name,
    rc.rdb$relation_name as table_name
        from rdb$indices ix
        left join rdb$index_segments sg on ix.rdb$index_name = sg.rdb$index_name
        left join rdb$relation_constraints rc on rc.rdb$index_name = ix.rdb$index_name
        where rc.rdb$constraint_type = 'PRIMARY KEY';
    """
    cur.execute(tables_query)
    rows = cur.fetchall()
    dict_entries = map(lambda x: {x[2].strip(): (x[1].strip(),)}, rows)
    merged_dict = reduce(lambda x, y: {k: x[k] + y[k] if k in x and k in y else x[k] if k in x else y[k] for k in list(x.keys()) + list(y.keys())}, dict_entries)
    return merged_dict
    

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
    boolean_datatype_sql = """
    CREATE DOMAIN BOOLEAN
    AS SMALLINT
    CHECK (value is null or value in (0, 1));
    """
    changes_log_table_sql = """
        CREATE TABLE CHANGES_LOG(
            LOG_ID int not null primary key,
            PK_VAL int not null,
            TABLE_ID int not null,
            MUTATION varchar(31),
            OCCURRED_AT TIMESTAMP,
            PROCESSED BOOLEAN DEFAULT 0
            );
    """
    changes_log_trigger_sql_template = """
        CREATE OR ALTER TRIGGER INTAKE_TRIGGER
            FOR CHANGES_LOG
            ACTIVE AFTER INSERT POSITION 10
        AS
        BEGIN
            POST_EVENT 'INTAKE_SIGNAL';
        END
        """
    ## TODO: Refactor this?
    changes_seq_sql = "CREATE SEQUENCE SEQ_CHANGES_LOG;"
    conn.execute_immediate(boolean_datatype_sql)
    conn.execute_immediate(changes_log_table_sql)
    conn.execute_immediate(changes_seq_sql)
    conn.execute_immediate(changes_log_trigger_sql_template)
    conn.commit()

def create_table_trigger(conn: fdb.Connection, cur: fdb.Cursor, table: str, table_id: int, pk_column_name: str) -> None:
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
    conn.execute_immediate(trigger_sql_template.format(table=table, table_id=table_id, pk_column_name=pk_column_name))
    conn.commit()

def create_table_triggers(conn: fdb.Connection, cur: fdb.Cursor, table_primary_keys: dict[str, str]) -> tuple[dict[str, int], dict[int, str]]:
    id = 0
    table_to_id = {}
    id_to_table = {}
    for table, pk_column_name in table_primary_keys.items():
        if table == "CHANGES_LOG":
            continue
        table_to_id[table] = id
        id_to_table[id] = table

        try:
            create_table_trigger(conn, cur, table, id, pk_column_name)
            id = id + 1
        except Exception as e:
            print("unable to create trigger for table: " + table)
            print(e)
    
    return table_to_id, id_to_table

def reset_state(conn: fdb.Connection, cur: fdb.Cursor, table_to_primary_key: dict[str, str]) -> None:
    print("attempting to reset state...")
    id = 0
    for table in table_to_primary_key:
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
        conn.execute_immediate(f"DROP DOMAIN BOOLEAN;")
        conn.commit()
    except Exception as e:
        print("unable to drop changes_log table and sequence")
        print(e)
        exit()
    print("state reset successfully!")
