#!/usr/bin/env python3
"""
Multi-Table Test Data Generator for Firebird Databases.

Generates test data for any table using schema information from schema.json.
Respects foreign key relationships and uses TEST_OFFSET to keep test data separate.

Usage:
    python testdata.py add articulos 100      # Add 100 test products
    python testdata.py add clientes 50        # Add 50 test customers
    python testdata.py add ventas 20          # Add 20 test sales
    python testdata.py delete articulos       # Delete test articulos
    python testdata.py delete all             # Delete all test data
    python testdata.py list                   # Show tables with test data
    python testdata.py schema                 # Show available tables from schema

The schema is loaded from schema.json. Generate it using:
    python schema_discovery.py --output schema.json
"""

import argparse
import json
import os
import random
import string
import sys
from datetime import datetime, timedelta
from typing import Any, Optional

import fdb

# Test data offset - all test IDs start from this value
TEST_OFFSET = 65535

# Path to schema file
SCHEMA_FILE = os.path.join(os.path.dirname(__file__), 'schema.json')


def load_schema(schema_path: str = SCHEMA_FILE) -> dict:
    """Load schema from JSON file."""
    if not os.path.exists(schema_path):
        print(f"Error: Schema file not found: {schema_path}")
        print("Generate it using: python schema_discovery.py --output schema.json")
        sys.exit(1)

    with open(schema_path, 'r') as f:
        return json.load(f)


def get_db_connection(db_path: str = None):
    """Get database connection. Auto-detects path in container or uses Microsip path on host."""
    if not db_path:
        # Auto-detect: check container paths first, then host path
        import glob

        # Check common container paths
        container_paths = [
            '/Microsip datos/*.fdb',      # probe container standard path
            '/firebird/data/*.fdb',        # docker-compose mounted path
            '/firebird/data/*.FDB',
        ]

        for pattern in container_paths:
            matches = glob.glob(pattern)
            if matches:
                db_path = matches[0]
                break

        if not db_path:
            # Try to use the default Microsip path (host mode)
            from probe.utils.fdb_helper import get_microsip_fdb_file_path
            db_path = get_microsip_fdb_file_path()

    return fdb.connect(
        dsn=db_path,
        user='sysdba',
        password='masterkey'
    )


def generate_value(col_type: str, col_name: str, idx: int, nullable: bool = True) -> Any:
    """Generate a fake value based on column type and name."""
    col_type_upper = col_type.upper()
    col_name_upper = col_name.upper()

    # Microsip-specific CHECK constraint values (handle before NULL chance)
    if col_name_upper == 'ESTATUS':
        return random.choice(['A', 'V', 'C', 'S', 'B'])
    if col_name_upper == 'SEGUIMIENTO':
        return random.choice(['N', 'L', 'S'])
    # SI_NO domains - ES_*, IMPRIMIR_* columns
    if col_name_upper.startswith('ES_') or col_name_upper.startswith('IMPRIMIR_'):
        return random.choice(['S', 'N'])
    # UNIDADES_1 domain - must be > 0
    if col_name_upper == 'CONTENIDO_UNIDAD_COMPRA':
        return random.randint(1, 100)
    # UNIDADES_0/PORCENTAJE_0 domains - must be >= 0, never null
    if col_name_upper in ('PESO_UNITARIO', 'PCTJE_ARANCEL'):
        return round(random.uniform(0, 100), 2)
    # Cost fields often have domain constraints >= 0
    if col_name_upper.startswith('COSTO_'):
        return round(random.uniform(0, 1000), 2)
    # Other numeric fields that commonly have >= 0 constraints in Microsip
    if col_name_upper in ('FPGC_UNITARIO', 'MARGEN_MINIMO', 'PCTJE_UTILIDAD'):
        return round(random.uniform(0, 100), 2)

    # Handle NULL values for optional fields (only if nullable)
    if nullable and random.random() < 0.1:
        return None

    # Primary key or ID fields - use offset + index
    if col_name_upper.endswith('_ID') or col_name_upper == 'ID':
        return TEST_OFFSET + idx

    # Integer types
    if 'INTEGER' in col_type_upper or 'SMALLINT' in col_type_upper or 'BIGINT' in col_type_upper:
        if 'CANTIDAD' in col_name_upper or 'QTY' in col_name_upper:
            return random.randint(1, 100)
        if 'DIAS' in col_name_upper:
            return random.randint(0, 365)
        if 'PCTJE' in col_name_upper or 'PERCENT' in col_name_upper:
            return random.randint(0, 100)
        return random.randint(1, 1000)

    # Numeric/Decimal types
    if 'NUMERIC' in col_type_upper or 'DECIMAL' in col_type_upper:
        if 'PRECIO' in col_name_upper or 'PRICE' in col_name_upper or 'COSTO' in col_name_upper:
            return round(random.uniform(10.0, 10000.0), 2)
        if 'PCTJE' in col_name_upper or 'PERCENT' in col_name_upper:
            return round(random.uniform(0, 100), 4)
        return round(random.uniform(0, 1000), 6)

    # Float/Double types
    if 'FLOAT' in col_type_upper or 'DOUBLE' in col_type_upper:
        return round(random.uniform(0, 1000), 4)

    # Date types
    if col_type_upper == 'DATE':
        base_date = datetime.now() - timedelta(days=random.randint(0, 365))
        return base_date.strftime('%Y-%m-%d')

    # Timestamp types
    if 'TIMESTAMP' in col_type_upper:
        base_date = datetime.now() - timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        return base_date.strftime('%Y-%m-%d %H:%M:%S.0000')

    # Time types
    if col_type_upper == 'TIME':
        return f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:00"

    # String types
    if 'VARCHAR' in col_type_upper or 'CHAR' in col_type_upper:
        # Extract max length
        max_len = 50
        if '(' in col_type_upper:
            try:
                max_len = int(col_type_upper.split('(')[1].split(')')[0])
            except (ValueError, IndexError):
                pass

        # Generate contextual values based on column name
        if 'NOMBRE' in col_name_upper or 'NAME' in col_name_upper:
            return f"Test {col_name} {idx}"[:max_len]
        if 'DIRECCION' in col_name_upper or 'ADDRESS' in col_name_upper:
            return f"Test Address {idx}"[:max_len]
        if 'TELEFONO' in col_name_upper or 'PHONE' in col_name_upper:
            return f"555-{random.randint(1000, 9999)}"[:max_len]
        if 'EMAIL' in col_name_upper:
            return f"test{idx}@example.com"[:max_len]
        if 'RFC' in col_name_upper:
            return f"TEST{idx:08d}XX"[:max_len]
        if 'USUARIO' in col_name_upper or 'USER' in col_name_upper:
            return "TESTUSER"[:max_len]
        if 'UNIDAD' in col_name_upper or 'UNIT' in col_name_upper:
            return "PZA"[:max_len]

        # Single character flags (S/N, A/I, etc.)
        if max_len == 1:
            if 'ES_' in col_name_upper or 'FLAG' in col_name_upper:
                return random.choice(['S', 'N'])
            if 'ESTATUS' in col_name_upper or 'STATUS' in col_name_upper:
                return random.choice(['A', 'I'])  # Active/Inactive
            return random.choice(string.ascii_uppercase)

        # Generic string
        return f"TEST_{idx}"[:max_len]

    # BLOB types - return None for simplicity
    if 'BLOB' in col_type_upper:
        return None

    # Default
    return None


def format_sql_value(val: Any) -> str:
    """Format a value for SQL insertion."""
    if val is None:
        return "NULL"
    if isinstance(val, str):
        # Escape single quotes
        escaped = val.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(val, bool):
        return "'S'" if val else "'N'"
    return str(val)


def get_next_id(cur, table_name: str, pk_column: str) -> int:
    """Get the next available test ID for a table."""
    cur.execute(f"SELECT MAX({pk_column}) FROM {table_name}")
    result = cur.fetchone()

    if result[0] is None or result[0] < TEST_OFFSET:
        return TEST_OFFSET
    return result[0] + 1


def get_existing_fk_value(cur, ref_table: str, ref_column: str) -> Optional[int]:
    """Get an existing value from a referenced table (for FK constraints)."""
    try:
        cur.execute(f"SELECT {ref_column} FROM {ref_table} WHERE {ref_column} >= {TEST_OFFSET} LIMIT 1")
        result = cur.fetchone()
        if result:
            return result[0]

        # Fall back to any existing value
        cur.execute(f"SELECT FIRST 1 {ref_column} FROM {ref_table}")
        result = cur.fetchone()
        return result[0] if result else None
    except Exception:
        return None


def add_records(table_name: str, num_records: int, schema: dict):
    """Add test records to a table."""
    table_name_upper = table_name.upper()

    if table_name_upper not in schema['tables']:
        print(f"Error: Table '{table_name}' not found in schema")
        print("Available tables:")
        for t in sorted(schema['tables'].keys()):
            print(f"  - {t}")
        sys.exit(1)

    table_schema = schema['tables'][table_name_upper]
    columns = table_schema['columns']
    pk_column = table_schema.get('primary_key')

    con = get_db_connection()
    cur = con.cursor()

    # Determine starting ID
    if pk_column and isinstance(pk_column, str):
        next_id = get_next_id(cur, table_name_upper, pk_column)
    else:
        next_id = TEST_OFFSET

    # Cache FK lookups
    fk_cache = {}

    inserted = 0
    for i in range(num_records):
        row_data = {}
        current_id = next_id + i

        for col_name, col_info in columns.items():
            col_type = col_info['type']
            nullable = col_info.get('nullable', True)
            references = col_info.get('references')

            # Handle primary key
            if pk_column and col_name == pk_column:
                row_data[col_name] = current_id
                continue

            # Handle foreign keys
            if references:
                ref_table, ref_column = references.split('.')
                cache_key = f"{ref_table}.{ref_column}"

                if cache_key not in fk_cache:
                    fk_cache[cache_key] = get_existing_fk_value(cur, ref_table, ref_column)

                fk_value = fk_cache[cache_key]
                if fk_value is not None:
                    row_data[col_name] = fk_value
                elif not nullable:
                    print(f"Warning: No FK value found for {col_name} -> {references}")
                    row_data[col_name] = None
                else:
                    row_data[col_name] = None
                continue

            # Generate value (pass nullable to avoid generating NULL for required fields)
            value = generate_value(col_type, col_name, current_id, nullable)

            # Ensure non-nullable fields have values (fallback)
            if not nullable and value is None:
                if 'VARCHAR' in col_type.upper() or 'CHAR' in col_type.upper():
                    value = 'T'  # Single char safe default
                elif 'INT' in col_type.upper():
                    value = 0
                elif 'NUMERIC' in col_type.upper() or 'DECIMAL' in col_type.upper():
                    value = 0.0
                elif 'DOUBLE' in col_type.upper() or 'FLOAT' in col_type.upper():
                    value = 0.0
                elif 'DATE' in col_type.upper():
                    value = datetime.now().strftime('%Y-%m-%d')
                elif 'TIMESTAMP' in col_type.upper():
                    value = datetime.now().strftime('%Y-%m-%d %H:%M:%S.0000')

            row_data[col_name] = value

        # Build INSERT statement
        col_names = ', '.join(row_data.keys())
        values = ', '.join(format_sql_value(v) for v in row_data.values())
        sql = f"INSERT INTO {table_name_upper} ({col_names}) VALUES ({values})"

        try:
            cur.execute(sql)
            inserted += 1
        except Exception as e:
            print(f"Error inserting record {current_id}: {e}")
            print(f"SQL: {sql[:200]}...")

    con.commit()
    cur.close()
    con.close()

    print(f"Added {inserted} records to '{table_name_upper}' (IDs {next_id} to {next_id + inserted - 1})")


def delete_records(table_name: str, schema: dict):
    """Delete test records from a table or all tables."""
    con = get_db_connection()

    if table_name.lower() == 'all':
        # Delete from all tables (in reverse dependency order would be ideal)
        deleted_counts = {}
        for tbl_name, tbl_info in schema['tables'].items():
            pk = tbl_info.get('primary_key')
            if pk and isinstance(pk, str):
                try:
                    con.execute_immediate(
                        f"DELETE FROM {tbl_name} WHERE {pk} >= {TEST_OFFSET}"
                    )
                    # Get count of affected rows
                    cur = con.cursor()
                    cur.execute(f"SELECT COUNT(*) FROM {tbl_name} WHERE {pk} >= {TEST_OFFSET}")
                    count_before = cur.fetchone()[0]
                    cur.close()
                    if count_before > 0:
                        deleted_counts[tbl_name] = count_before
                except Exception as e:
                    # FK constraint violations are expected - we'll retry
                    pass

        con.commit()
        con.close()

        if deleted_counts:
            print("Deleted test data from:")
            for tbl, count in deleted_counts.items():
                print(f"  - {tbl}")
        else:
            print("No test data found to delete")
    else:
        table_name_upper = table_name.upper()
        if table_name_upper not in schema['tables']:
            print(f"Error: Table '{table_name}' not found in schema")
            con.close()
            sys.exit(1)

        table_schema = schema['tables'][table_name_upper]
        pk = table_schema.get('primary_key')

        if not pk or not isinstance(pk, str):
            print(f"Error: Cannot determine primary key for {table_name_upper}")
            con.close()
            sys.exit(1)

        try:
            con.execute_immediate(
                f"DELETE FROM {table_name_upper} WHERE {pk} >= {TEST_OFFSET}"
            )
            con.commit()
            print(f"Deleted test records from '{table_name_upper}'")
        except Exception as e:
            print(f"Error deleting from {table_name_upper}: {e}")
            print("(This may be due to foreign key constraints)")

        con.close()


def list_test_data(schema: dict):
    """List tables that have test data."""
    con = get_db_connection()
    cur = con.cursor()

    print(f"Test data (IDs >= {TEST_OFFSET}):\n")

    has_data = False
    for tbl_name, tbl_info in sorted(schema['tables'].items()):
        pk = tbl_info.get('primary_key')
        if pk and isinstance(pk, str):
            try:
                cur.execute(
                    f"SELECT COUNT(*), MIN({pk}), MAX({pk}) "
                    f"FROM {tbl_name} WHERE {pk} >= {TEST_OFFSET}"
                )
                count, min_id, max_id = cur.fetchone()
                if count > 0:
                    has_data = True
                    print(f"  {tbl_name}: {count} records (IDs {min_id}-{max_id})")
            except Exception:
                pass

    if not has_data:
        print("  No test data found")

    cur.close()
    con.close()


def show_schema(schema: dict, table_filter: Optional[str] = None):
    """Show available tables from schema."""
    tables = schema['tables']

    if table_filter:
        table_filter_upper = table_filter.upper()
        if table_filter_upper in tables:
            tables = {table_filter_upper: tables[table_filter_upper]}
        else:
            # Partial match
            matches = {k: v for k, v in tables.items() if table_filter_upper in k}
            if matches:
                tables = matches
            else:
                print(f"No tables matching '{table_filter}' found")
                return

    print(f"Schema: {len(tables)} tables\n")

    for tbl_name in sorted(tables.keys()):
        tbl_info = tables[tbl_name]
        pk = tbl_info.get('primary_key', 'N/A')
        col_count = len(tbl_info['columns'])
        print(f"  {tbl_name} ({col_count} columns, PK: {pk})")

        # Show columns with FK references
        fk_cols = [
            (col, info['references'])
            for col, info in tbl_info['columns'].items()
            if info.get('references')
        ]
        for col, ref in fk_cols:
            print(f"    -> {col} references {ref}")


def main():
    parser = argparse.ArgumentParser(
        description='Multi-table test data generator for Firebird databases',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s add articulos 100      # Add 100 test products
  %(prog)s add clientes 50        # Add 50 test customers
  %(prog)s delete articulos       # Delete test articulos
  %(prog)s delete all             # Delete all test data
  %(prog)s list                   # Show tables with test data
  %(prog)s schema                 # Show all available tables
  %(prog)s schema articulos       # Show schema for articulos

First, generate the schema file:
  python schema_discovery.py --output schema.json
        """
    )

    parser.add_argument('--schema-file', default=SCHEMA_FILE,
                        help=f'Path to schema JSON file (default: {SCHEMA_FILE})')

    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # Add command
    add_parser = subparsers.add_parser('add', help='Add test records')
    add_parser.add_argument('table', help='Table name')
    add_parser.add_argument('count', type=int, help='Number of records to add')

    # Delete command
    delete_parser = subparsers.add_parser('delete', help='Delete test records')
    delete_parser.add_argument('table', help='Table name or "all"')

    # List command
    subparsers.add_parser('list', help='List tables with test data')

    # Schema command
    schema_parser = subparsers.add_parser('schema', help='Show available tables')
    schema_parser.add_argument('table', nargs='?', help='Filter by table name')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Load schema
    schema = load_schema(args.schema_file)

    if args.command == 'add':
        add_records(args.table, args.count, schema)
    elif args.command == 'delete':
        delete_records(args.table, schema)
    elif args.command == 'list':
        list_test_data(schema)
    elif args.command == 'schema':
        show_schema(schema, getattr(args, 'table', None))


if __name__ == '__main__':
    main()
