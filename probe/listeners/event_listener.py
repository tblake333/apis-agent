from fdb import Connection

class EventListener:

    def __init__(
            self,
            conn: Connection,
            events: list[str]
    ):
        self.conn = conn
        self.events = events
        self.conduit = self.conn.event_conduit(events)
    
    def begin(self):
        self.conduit.begin()


    def listen(self, end_flag_event: str) -> dict:
        hits = {}
        while end_flag_event not in hits or hits[end_flag_event] == 0:
            partition_hits = self.conduit.wait()
            hits = {**hits, **partition_hits}
        self.conduit.flush()
        print(hits)
        return hits

    def close(self):
        self.conduit.close()


        
