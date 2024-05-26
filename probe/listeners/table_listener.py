from fdb import Connection
from listeners.event_listener import EventListener
from utils.apis_types import Mutation
    
class TableListener:
    
    def __init__(
            self,
            conn: Connection,
            table_id: str,
    ):
        self.conn = conn
        self.table_id = table_id
        self.end_flag_event = f"TABLE_{self.table_id}_CHANGE_END"

        bits = [str(n) for n in range(0, 64)]
        mutations = ['INSERT', 'UPDATE', 'DELETE']
        self.events = bits + mutations + [self.end_flag_event]
        
        self.listener = EventListener(self.conn, events=self.events)

    def begin(self):
        self.listener.begin()

    
    def listen_for_mutation(self) -> (Mutation, int):
        hits = self.listener.listen(self.end_flag_event)
        mutation = self._get_mutation(hits)
        row_num = self._get_row_number(hits)

        return mutation, row_num

    def close(self):
        self.listener.close()
    
    def _get_mutation(self, event_map: dict) -> Mutation:
        # TODO: Raise exception if more than one mutation exists
        for mutation in Mutation:
            if event_map[mutation.name] != 0:
                return mutation
        return Mutation.EMPTY

    def _get_row_number(self, event_map: dict) -> int:
        row_num = 0
        # TODO: Raise exception if missing numbers (powers)
        # TODO: Also raise exception if a power is > 1
        for i in range(0, 64):
            power = i
            event = str(power)
            if event_map[event] != 0:
                row_num += 2 ** power
        return row_num
