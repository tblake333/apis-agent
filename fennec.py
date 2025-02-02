import fdb
import random
from argparse import ArgumentParser
from probe.utils.fdb_helper import get_microsip_fdb_file_path

def populate_data(idx: int) -> dict:
    data = {
        'ARTICULO_ID': idx,
        'NOMBRE': f'Articulo {idx}',
        'ES_ALMACENABLE': 'S',
        'ES_JUEGO': 'N',
        'ESTATUS': 'A',
        'CAUSA_SUSP': None,
        'FECHA_SUSP': None,
        'IMPRIMIR_COMP': 'N',
        'LINEA_ARTICULO_ID': 392,
        'UNIDAD_VENTA': 'PZA',
        'UNIDAD_COMPRA': 'PZA',
        'CONTENIDO_UNIDAD_COMPRA': 1.00000,
        'PESO_UNITARIO': 0.00000,
        'ES_PESO_VARIABLE': 'N',
        'SEGUIMIENTO': 'N',
        'DIAS_GARANTIA': 0,
        'ES_IMPORTADO': 'N',
        'ES_SIEMPRE_IMPORTADO': 'S',
        'PCTJE_ARANCEL': 0.000000,
        'NOTAS_COMPRAS': None,
        'IMPRIMIR_NOTAS_COMPRAS': 'N',
        'NOTAS_VENTAS': None,
        'IMPRIMIR_NOTAS_VENTAS': 'N',
        'ES_PRECIO_VARIABLE': 'N',
        'CUENTA_ALMACEN': None,
        'CUENTA_COSTO_VENTA': None,
        'CUENTA_VENTAS': None,
        'CUENTA_DEVOL_VENTAS': None,
        'CUENTA_COMPRAS': None,
        'CUENTA_DEVOL_COMPRAS': None,
        'FECHA_ULTIMA_COMPRA': '2023-06-12',
        'COSTO_ULTIMA_COMPRA': 80.000000,
        'FPGC_UNITARIO': 0.000000,
        'USUARIO_CREADOR': 'SYSDBA',
        'FECHA_HORA_CREACION': '2012-10-27 14:16:38.6400',
        'USUARIO_AUT_CREACION': None,
        'USUARIO_ULT_MODIF': 'CAJERO',
        'FECHA_HORA_ULT_MODIF': '2014-05-05 16:33:36.5780',
        'USUARIO_AUT_MODIF': None
    }
    return data

def get_db_value(val):
    if val is None:
        return "null"
    else:
        return str(val)

# Firebird database connection parameters
DB_PATH = get_microsip_fdb_file_path()
DB_USER = 'sysdba'
DB_PASSWORD = 'masterkey'

TEST_OFFSET = 65535

# Define the command-line arguments
parser = ArgumentParser(description='Generate dummy data for the "articulos" table')
subparsers = parser.add_subparsers(dest='command')

add_parser = subparsers.add_parser('add', help='Add dummy data to the table')
add_parser.add_argument('num_records', type=int, help='Number of records to add')

delete_parser = subparsers.add_parser('delete', help='Delete records from the table')

# Parse the command-line arguments
args = parser.parse_args()

# Connect to the Firebird database
con = fdb.connect(
    dsn=DB_PATH,
    user=DB_USER,
    password=DB_PASSWORD
)

# Create a cursor object to execute SQL queries
cur = con.cursor()
if args.command == 'add':
    # Query the "articulos" table to find the most recent row added
    sql_query = "SELECT MAX(articulo_id) FROM articulos"
    cur.execute(sql_query)
    result = cur.fetchone()

    # If the table is empty, start with id = 1
    if result[0] is None or result[0] < TEST_OFFSET:
        next_id = TEST_OFFSET
    else:
        next_id = result[0] + 1

    # Generate dummy data and insert it into the "articulos" table
    for i in range(args.num_records):
        data = populate_data(next_id + i)
        columns = ', '.join(data.keys())
        values = ', '.join(f"'{value}'" if isinstance(value, str) else get_db_value(value) for value in data.values())

        sql_query = f"INSERT INTO articulos ({columns}) VALUES ({values})"
        
        # Execute the SQL query
        cur.execute(sql_query)

    # Commit the changes to the database
    con.commit()
    print(f"Added {args.num_records} records to the 'articulos' table.")

elif args.command == 'delete':
    # Delete entries with primary key greater than 50
    sql_query = f"DELETE FROM articulos WHERE articulo_id > {TEST_OFFSET}"
    con.execute_immediate(sql_query)
    con.commit()
    print("Deleted test entries")

# Close the cursor and connection objects
cur.close()
con.close()

