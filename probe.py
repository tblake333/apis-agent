import fdb
from sys import platform
import os

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

changes_log_table_sql = """
    CREATE TABLE CHANGES_LOG (
        LOG_ID int not null primary key,
        ID_TABLE int not null,
        TABLE_NAME varchar(100),
        MUTATION varchar(31),
        OCCURRED_AT TIMESTAMP );
"""

changes_seq_sql = "CREATE SEQUENCE SEQ_CHANGES_LOG;"

con.execute_immediate(changes_log_table_sql)
con.execute_immediate(changes_seq_sql)
con.commit()

pk_sql_template = """
SELECT RDB$FIELD_NAME
        FROM RDB$RELATION_FIELDS
        WHERE RDB$RELATION_NAME = '{table}'
        AND RDB$FIELD_POSITION = 0;
"""

trigger_sql_template = """
    CREATE OR ALTER TRIGGER {table}_CHANGES
        FOR {table}
        ACTIVE AFTER INSERT OR UPDATE OR DELETE POSITION 10
    AS
    BEGIN
        INSERT INTO CHANGES_LOG (LOG_ID, ID_TABLE, TABLE_NAME, MUTATION, OCCURRED_AT)
            VALUES (NEXT VALUE FOR SEQ_CHANGES_LOG,
            
                CASE
                WHEN INSERTING THEN NEW.{pk_column_name}
                WHEN UPDATING THEN NEW.{pk_column_name}
                WHEN DELETING THEN OLD.{pk_column_name}
                END,
                '{table}',
                CASE
                WHEN INSERTING THEN 'INSERT'
                WHEN UPDATING THEN 'UPDATE'
                WHEN DELETING THEN 'DELETE'
                END,
            current_timestamp);
    END
    """

trigger_sql_template = """
    CREATE OR ALTER TRIGGER {table}_CHANGES
        FOR {table}
        ACTIVE AFTER INSERT OR UPDATE OR DELETE POSITION 10
    AS
    BEGIN
        POST_EVENT '{table}_CHANGES';
    END
    """

for table in table_names:
    # try:
    #     con.execute_immediate("DROP TRIGGER " + table + "_CHANGES;")
    #     con.commit()
    # except:
    #     print("unable to process " + table)


    try:
        cur.execute(pk_sql_template.format(table=table))
        pk_column_name = cur.fetchone()[0].strip()

        con.execute_immediate(trigger_sql_template.format(table=table, pk_column_name=pk_column_name))
        con.commit()
    except:
        print("unable to process " + table)
    

    events = con.event_conduit(['USERS_CHANGES'])
    events.begin()
    e = events.wait()
    events.close()
    print("EVENT DETECTED!!")
    print(e)

cur.execute(triggers_query)

trigger_names = [row[0].strip() for row in cur.fetchall()]

cur.close()
con.close()

print(len(trigger_names))