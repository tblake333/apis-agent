from fdb import Connection
from handlers.articulos_handler import ArticulosTableHandler
from handlers.base_table_handler import BaseTableHandler
from utils.values import TABLE_TO_ID

TABLE_HANDLERS: list[BaseTableHandler] = [
    ArticulosTableHandler,
]

ID_TO_HANDLER: dict[int, BaseTableHandler] = {}


class TableHandlerFactory:

    @staticmethod
    def create(table_name: str, conn: Connection) -> BaseTableHandler:
        if table_name in ID_TO_HANDLER:
            table_id = TABLE_TO_ID[table_name]
            cls = ID_TO_HANDLER[table_name]
            print("CREATED HANDLER")
            return cls(conn, table_id)
        else:
            return None


def register_handlers():
    for table_handler in TABLE_HANDLERS:
        ID_TO_HANDLER[table_handler.TABLE_NAME] = table_handler
    print(ID_TO_HANDLER)

register_handlers()