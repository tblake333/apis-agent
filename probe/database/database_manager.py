from functools import reduce
from typing import Optional
import fdb


class DatabaseManager:
    def __init__(self, conn: fdb.Connection):
        self.conn = conn

    def setup(self, sync_client=None) -> tuple[dict[int, str], dict[str, str]]:
        table_names = self.get_table_names()
        if "CHANGES_LOG" not in table_names:
            print("no changes_log table detected. creating one...")
            self.create_changes_log_table()
        else:
            print("changes_log table found. skipping creation...")

        table_to_primary_key = self.get_table_to_primary_key()
        _, id_to_table = self.create_table_triggers(table_to_primary_key)
        self.process_leftover_mutations(id_to_table, table_to_primary_key, sync_client)
        self.delete_processed_mutations()
        return id_to_table, table_to_primary_key


    def delete_processed_mutations(self):
        print("deleting processed mutations...")
        cur = self.conn.cursor()
        cur.execute("DELETE FROM CHANGES_LOG WHERE PROCESSED = 1")
        self.conn.commit()

    def process_leftover_mutations(self, id_to_table, table_to_primary_key, sync_client=None):
        from handlers.base_table_handler import BaseTableHandler
        from models.change import Change

        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM CHANGES_LOG WHERE PROCESSED = 0")
        rows = cur.fetchall()
        if len(rows) != 0:
            print("Processing leftover mutations...")
        for row in rows:
            change = Change(*row)
            table_name = id_to_table[change.table_id]
            primary_key = table_to_primary_key[table_name]
            table_handler = BaseTableHandler(self.conn, table_name, primary_key, sync_client)
            table_handler.handle_mutation(change, "Main")
            processed_cur = self.conn.cursor()
            processed_cur.execute("UPDATE CHANGES_LOG SET PROCESSED = 1 WHERE LOG_ID = ?", (change.log_id,))
            self.conn.commit()

    def ensure_clean_slate(self, conn: fdb.Connection):
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM CHANGES_LOG")
        result = cur.fetchone()[0]
        if int(result) != 0:
            print("CHANGES_LOG not empty, exiting...")
            exit()


    def get_table_names(self) -> list[str]:
        tables_query = "SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG = 0;"
        cur = self.conn.cursor()
        cur.execute(tables_query)
        table_names = [row[0].strip() for row in cur.fetchall()]

        return table_names

    def get_table_to_primary_key(self) -> dict[str, str]:
        table_primary_keys = self.get_table_primary_keys()
        table_names = self.get_table_names()
        table_to_primary_key: dict[str, str] = {}
        id = 0
        for table in table_names:
            if table == "CHANGES_LOG":
                continue
            if table not in table_primary_keys:
                continue
            if len(table_primary_keys[table]) != 1 or self.get_column_datatype(table, pk_column_name := table_primary_keys[table][0]) != "INTEGER":
                continue
                
            table_to_primary_key[table] = pk_column_name
            id = id + 1
        return table_to_primary_key

    def get_column_datatype(self, table: str, column_name: str) -> str:
        cur = self.conn.cursor()
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

    def get_table_primary_keys(self) -> dict[str, tuple]:
        tables_query = """
        select ix.rdb$index_name as index_name,
        sg.rdb$field_name as field_name,
        rc.rdb$relation_name as table_name
            from rdb$indices ix
            left join rdb$index_segments sg on ix.rdb$index_name = sg.rdb$index_name
            left join rdb$relation_constraints rc on rc.rdb$index_name = ix.rdb$index_name
            where rc.rdb$constraint_type = 'PRIMARY KEY';
        """
        cur = self.conn.cursor()
        cur.execute(tables_query)
        rows = cur.fetchall()
        dict_entries = map(lambda x: {x[2].strip(): (x[1].strip(),)}, rows)
        merged_dict = reduce(lambda x, y: {k: x[k] + y[k] if k in x and k in y else x[k] if k in x else y[k] for k in list(x.keys()) + list(y.keys())}, dict_entries)
        return merged_dict
        

    def get_primary_key_name(self, cur: fdb.Cursor, table: str) -> str:
        pk_sql_template = """
        SELECT RDB$FIELD_NAME
                FROM RDB$RELATION_FIELDS
                WHERE RDB$RELATION_NAME = '{table}'
                AND RDB$FIELD_POSITION = 0;
        """
        cur.execute(pk_sql_template.format(table=table))
        pk_column_name = cur.fetchone()[0].strip()
        return pk_column_name

    def create_changes_log_table(self) -> None:
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
        self.conn.execute_immediate(boolean_datatype_sql)
        self.conn.execute_immediate(changes_log_table_sql)
        self.conn.execute_immediate(changes_seq_sql)
        self.conn.execute_immediate(changes_log_trigger_sql_template)
        self.conn.commit()

    def create_table_trigger(self, table: str, table_id: int, pk_column_name: str) -> None:
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
        self.conn.execute_immediate(trigger_sql_template.format(table=table, table_id=table_id, pk_column_name=pk_column_name))
        self.conn.commit()

    def create_table_triggers(self, table_primary_keys: dict[str, str]) -> tuple[dict[str, int], dict[int, str]]:
        id = 0
        table_to_id = {}
        id_to_table = {}
        for table, pk_column_name in table_primary_keys.items():
            if table == "CHANGES_LOG":
                continue
            table_to_id[table] = id
            id_to_table[id] = table

            try:
                self.create_table_trigger(table, id, pk_column_name)
                id = id + 1
            except Exception as e:
                print("unable to create trigger for table: " + table)
                print(e)
        
        return table_to_id, id_to_table

    def reset_state(self) -> None:
        table_to_primary_key = self.get_table_to_primary_key()
        print("attempting to reset state...")
        id = 0
        for table in table_to_primary_key:
            if table == "CHANGES_LOG":
                continue
            try:
                self.conn.execute_immediate(f"DROP TRIGGER TABLE_{id}_CHANGES;")
                self.conn.commit()
            except Exception as e:
                print("unable to drop trigger for table: " + table)
                print(e)
                exit()
            id = id + 1
        
        print("successfully dropped all table triggers!")
        print("now dropping changes_log table and sequence...")
        try:
            self.conn.execute_immediate(f"DROP TRIGGER INTAKE_TRIGGER;")
            self.conn.execute_immediate(f"DROP TABLE CHANGES_LOG;")
            self.conn.execute_immediate(f"DROP SEQUENCE SEQ_CHANGES_LOG;")
            self.conn.execute_immediate(f"DROP DOMAIN BOOLEAN;")
            self.conn.commit()
        except Exception as e:
            print("unable to drop changes_log table and sequence")
            print(e)
            exit()
        print("state reset successfully!")