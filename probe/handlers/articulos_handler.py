from handlers.base_table_handler import BaseTableHandler


class ArticulosTableHandler(BaseTableHandler):

    TABLE_NAME = "ARTICULOS"

    def handle_insert(self, row: int):
        print(f"Got insert on row {row}")

    def handle_update(self, row: int):
        print(f"Got update on row {row}")

    def handle_delete(self, row: int):
        print(f"Got delete on row {row}")