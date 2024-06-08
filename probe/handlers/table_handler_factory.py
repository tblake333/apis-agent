from fdb import Connection
from handlers.articulos_handler import ArticulosTableHandler
from probe.handlers.base_table_handler import BaseTableHandler

TABLE_HANDLERS: list[BaseTableHandler] = [
    ArticulosTableHandler,
]

ID_TO_HANDLER: dict[int, BaseTableHandler] = {}


class TableHandlerFactory:

    def __init__(self, table_to_id: dict[str, int]):
        self.table_to_id = table_to_id

    def create(self, table_name: str, conn: Connection) -> BaseTableHandler:
        if table_name in ID_TO_HANDLER:
            table_id = self.table_to_id[table_name]
            cls = ID_TO_HANDLER[table_name]
            print("CREATED HANDLER")
            return cls(conn, table_id)
        else:
            return None


def register_handlers():
    for table_handler in TABLE_HANDLERS:
        ID_TO_HANDLER[table_handler.TABLE_NAME] = table_handler
    # print(ID_TO_HANDLER)

register_handlers()