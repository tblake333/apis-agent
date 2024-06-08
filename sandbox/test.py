import fdb
import threading
import time

# Prepare database
con = fdb.create_database("CREATE DATABASE 'event_test.fdb' USER 'SYSDBA' PASSWORD 'masterkey'")
con.execute_immediate("CREATE TABLE T (PK Integer, C1 Integer)")
con.execute_immediate("""CREATE TRIGGER EVENTS_AU FOR T ACTIVE
BEFORE UPDATE POSITION 0
AS
BEGIN
   if (old.C1 <> new.C1) then
      post_event 'c1_updated' ;
END""")
con.execute_immediate("""CREATE TRIGGER EVENTS_AI FOR T ACTIVE
AFTER INSERT POSITION 0
AS
DECLARE VARIABLE pk_val INTEGER;
BEGIN
   if (new.c1 = 1) then
    BEGIN
     post_event 'insert_1' ;
    END
   else if (new.c1 = 2) then
     post_event 'insert_2' ;
   else if (new.c1 = 3) then
     post_event 'insert_3' ;
   else
     post_event 'insert_other' ;
END""")
con.commit()
cur = con.cursor()

# Utility function
def send_events(command_list):
   for cmd in command_list:
      cur.execute(cmd)
   con.commit()

print("One event")
#      =========
timed_event = threading.Timer(3.0,send_events,args=[["insert into T (PK,C1) values (1,1)",]])
events = con.event_conduit(['insert_1'])
events.begin()
timed_event.start()
e = events.wait()
print(e)
events.close()

print("Multiple events")
#      ===============
cmds = ["insert into T (PK,C1) values (1,1)",
        "insert into T (PK,C1) values (1,2)",
        "insert into T (PK,C1) values (1,3)",
        "insert into T (PK,C1) values (1,1)",
        "insert into T (PK,C1) values (1,2)",]
timed_event = threading.Timer(3.0,send_events,args=[cmds])
events = con.event_conduit(['insert_1','insert_3'])
events.begin()
timed_event.start()
e = events.wait()
events.close()
print(e)

print("20 events")
#      =========
cmds = ["insert into T (PK,C1) values (1,1)",
        "insert into T (PK,C1) values (1,2)",
        "insert into T (PK,C1) values (1,3)",
        "insert into T (PK,C1) values (1,1)",
        "insert into T (PK,C1) values (1,2)",]
timed_event = threading.Timer(1.0,send_events,args=[cmds])
events = con.event_conduit(['insert_1','A','B','C','D',
                            'E','F','G','H','I','J','K','L','M',
                            'N','O','P','Q','R','insert_3'])
events.begin()
timed_event.start()
time.sleep(3)
e = events.wait()
events.close()
print(e)

print("Flush events")
#      ============
timed_event = threading.Timer(3.0,send_events,args=[["insert into T (PK,C1) values (1,1)",]])
events = con.event_conduit(['insert_1'])
events.begin()
send_events(["insert into T (PK,C1) values (1,1)",
             "insert into T (PK,C1) values (1,1)"])
time.sleep(2)
events.flush()
timed_event.start()
e = events.wait()
events.close()
print(e)

# Finalize
con.drop_database()
con.close()