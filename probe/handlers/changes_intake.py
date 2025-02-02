import contextlib
import io
import logging
from queue import Queue
from fdb import Connection
from models.change import Change
from utils.apis_types import Mutation
from threading import Event, Thread


class ChangesIntake(Thread):

    TABLE_NAME = "CHANGES_LOG"
    EVENT_NAME = "INTAKE_SIGNAL"
    PK_COL_NAME = "LOG_ID"

    def __init__(self, conn: Connection, pos: int, output: Queue):
        super(ChangesIntake, self).__init__()
        self.conn = conn
        self.events = [ChangesIntake.EVENT_NAME]
        self.pos = pos
        self.output = output
        self._stop_event = Event()
    
    def stopped(self):
        return self._stop_event.is_set()
    
    def stop(self):
        self._stop_event.set()

    def run(self):
        print(f"Starting intake on {self.TABLE_NAME} table")
        while not self.stopped():
            try:
                with contextlib.redirect_stderr(io.StringIO()), self.conn.event_conduit(event_names=self.events) as conduit:
                    conduit.wait()
                    print("got change :D")
                    if self.stopped():
                        break
                    cur = self.conn.cursor()
                    cur.execute(f"SELECT * FROM {self.TABLE_NAME} WHERE {self.PK_COL_NAME} >= {str(self.pos)} AND PROCESSED = 0")
                    print(f"pos = {self.pos}")
                    rows = cur.fetchall()
                    for row in rows:
                        change = Change(*row)
                        self.output.put(change)
                    self.pos += len(rows)
            except Exception as e:
                print("SOMETHING HAPPENING, trying again...")
                print(e)
                pass
        print("Stopping changes intake!")
    
