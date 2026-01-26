#!/usr/bin/env python3
"""
Schema Discovery Tool for Firebird Databases.

Connects to a Firebird database and extracts complete schema information
including tables, columns, types, constraints, and foreign key relationships.

Usage:
    # Local docker Firebird (uses default Microsip path)
    python schema_discovery.py

    # Specific database path
    python schema_discovery.py --db /path/to/database.fdb

    # Remote database (via SSH tunnel or direct connection)
    python schema_discovery.py --host remote-server --port 3050 --db /path/to/db.fdb

    # Output to file
    python schema_discovery.py --output schema.json
"""

import argparse
import json
import sys
from dataclasses import dataclass, field, asdict
from typing import Optional
import fdb


@dataclass
class ColumnInfo:
    """Information about a database column."""
    name: str
    type: str
    nullable: bool
    default: Optional[str] = None
    references: Optional[str] = None  # "table.column" for foreign keys


@dataclass
class TableInfo:
    """Information about a database table."""
    name: str
    primary_key: Optional[str] = None
    columns: dict = field(default_factory=dict)  # column_name -> ColumnInfo


@dataclass
class SchemaInfo:
    """Complete schema information."""
    tables: dict = field(default_factory=dict)  # table_name -> TableInfo
    source_db: str = ""
    discovered_at: str = ""


def get_firebird_type(field_type: int, field_sub_type: int, field_length: int,
                       field_precision: int, field_scale: int, char_length: int) -> str:
    """Convert Firebird internal type codes to readable type names."""
    # Firebird type mapping
    types = {
        7: "SMALLINT",
        8: "INTEGER",
        10: "FLOAT",
        12: "DATE",
        13: "TIME",
        14: "CHAR",
        16: "BIGINT",
        27: "DOUBLE PRECISION",
        35: "TIMESTAMP",
        37: "VARCHAR",
        261: "BLOB",
    }

    base_type = types.get(field_type, f"UNKNOWN({field_type})")

    # Handle subtypes for numeric types
    if field_type in (7, 8, 16) and field_scale < 0:
        return f"NUMERIC({field_precision},{-field_scale})"

    # Handle BLOB subtypes
    if field_type == 261:
        if field_sub_type == 1:
            return "BLOB SUB_TYPE TEXT"
        return "BLOB"

    # Handle string lengths
    if field_type in (14, 37) and char_length:
        return f"{base_type}({char_length})"

    return base_type


def discover_schema(host: str, port: int, db_path: str,
                    user: str, password: str) -> SchemaInfo:
    """
    Connect to Firebird and extract complete schema information.

    Args:
        host: Database server hostname
        port: Database server port
        db_path: Path to the database file
        user: Database username
        password: Database password

    Returns:
        SchemaInfo object with complete schema
    """
    from datetime import datetime

    # Build DSN
    if host and host not in ('localhost', '127.0.0.1'):
        dsn = f"{host}/{port}:{db_path}"
    else:
        dsn = db_path

    print(f"Connecting to: {dsn}")

    con = fdb.connect(
        dsn=dsn,
        user=user,
        password=password
    )

    schema = SchemaInfo(
        source_db=dsn,
        discovered_at=datetime.now().isoformat()
    )

    cur = con.cursor()

    # Get all user tables (exclude system tables)
    cur.execute("""
        SELECT TRIM(RDB$RELATION_NAME)
        FROM RDB$RELATIONS
        WHERE RDB$SYSTEM_FLAG = 0
          AND RDB$VIEW_BLR IS NULL
        ORDER BY RDB$RELATION_NAME
    """)

    tables = [row[0] for row in cur.fetchall()]
    print(f"Found {len(tables)} tables")

    for table_name in tables:
        table_info = TableInfo(name=table_name)

        # Get columns for this table
        cur.execute("""
            SELECT
                TRIM(rf.RDB$FIELD_NAME),
                f.RDB$FIELD_TYPE,
                f.RDB$FIELD_SUB_TYPE,
                f.RDB$FIELD_LENGTH,
                f.RDB$FIELD_PRECISION,
                f.RDB$FIELD_SCALE,
                f.RDB$CHARACTER_LENGTH,
                rf.RDB$NULL_FLAG,
                rf.RDB$DEFAULT_SOURCE
            FROM RDB$RELATION_FIELDS rf
            JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
            WHERE rf.RDB$RELATION_NAME = ?
            ORDER BY rf.RDB$FIELD_POSITION
        """, (table_name,))

        for row in cur.fetchall():
            col_name = row[0]
            field_type = row[1] or 0
            field_sub_type = row[2] or 0
            field_length = row[3] or 0
            field_precision = row[4] or 0
            field_scale = row[5] or 0
            char_length = row[6] or 0
            null_flag = row[7]
            default_source = row[8]

            col_type = get_firebird_type(
                field_type, field_sub_type, field_length,
                field_precision, field_scale, char_length
            )

            # Clean up default value
            default_val = None
            if default_source:
                default_val = default_source.strip()
                if default_val.upper().startswith('DEFAULT '):
                    default_val = default_val[8:].strip()

            col_info = ColumnInfo(
                name=col_name,
                type=col_type,
                nullable=(null_flag is None or null_flag == 0),
                default=default_val
            )
            table_info.columns[col_name] = col_info

        # Get primary key
        cur.execute("""
            SELECT TRIM(isg.RDB$FIELD_NAME)
            FROM RDB$RELATION_CONSTRAINTS rc
            JOIN RDB$INDEX_SEGMENTS isg ON rc.RDB$INDEX_NAME = isg.RDB$INDEX_NAME
            WHERE rc.RDB$RELATION_NAME = ?
              AND rc.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY isg.RDB$FIELD_POSITION
        """, (table_name,))

        pk_columns = [row[0] for row in cur.fetchall()]
        if pk_columns:
            table_info.primary_key = pk_columns[0] if len(pk_columns) == 1 else pk_columns

        schema.tables[table_name] = table_info

    # Get foreign key relationships
    cur.execute("""
        SELECT
            TRIM(rc.RDB$RELATION_NAME) as from_table,
            TRIM(isg.RDB$FIELD_NAME) as from_column,
            TRIM(rc2.RDB$RELATION_NAME) as to_table,
            TRIM(isg2.RDB$FIELD_NAME) as to_column
        FROM RDB$RELATION_CONSTRAINTS rc
        JOIN RDB$REF_CONSTRAINTS ref ON rc.RDB$CONSTRAINT_NAME = ref.RDB$CONSTRAINT_NAME
        JOIN RDB$RELATION_CONSTRAINTS rc2 ON ref.RDB$CONST_NAME_UQ = rc2.RDB$CONSTRAINT_NAME
        JOIN RDB$INDEX_SEGMENTS isg ON rc.RDB$INDEX_NAME = isg.RDB$INDEX_NAME
        JOIN RDB$INDEX_SEGMENTS isg2 ON rc2.RDB$INDEX_NAME = isg2.RDB$INDEX_NAME
            AND isg.RDB$FIELD_POSITION = isg2.RDB$FIELD_POSITION
        WHERE rc.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
    """)

    for row in cur.fetchall():
        from_table, from_column, to_table, to_column = row
        if from_table in schema.tables and from_column in schema.tables[from_table].columns:
            schema.tables[from_table].columns[from_column].references = f"{to_table}.{to_column}"

    cur.close()
    con.close()

    return schema


def schema_to_dict(schema: SchemaInfo) -> dict:
    """Convert SchemaInfo to a JSON-serializable dictionary."""
    result = {
        "source_db": schema.source_db,
        "discovered_at": schema.discovered_at,
        "tables": {}
    }

    for table_name, table_info in schema.tables.items():
        table_dict = {
            "primary_key": table_info.primary_key,
            "columns": {}
        }

        for col_name, col_info in table_info.columns.items():
            col_dict = {
                "type": col_info.type,
                "nullable": col_info.nullable
            }
            if col_info.default:
                col_dict["default"] = col_info.default
            if col_info.references:
                col_dict["references"] = col_info.references

            table_dict["columns"][col_name] = col_dict

        result["tables"][table_name] = table_dict

    return result


def main():
    parser = argparse.ArgumentParser(
        description='Discover schema from a Firebird database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Use local Microsip path
  %(prog)s --db /path/to/db.fdb              # Specific database file
  %(prog)s --host remote --port 3050         # Remote database
  %(prog)s --output schema.json              # Save to file
        """
    )

    parser.add_argument('--host', default='localhost',
                        help='Database server hostname (default: localhost)')
    parser.add_argument('--port', type=int, default=3050,
                        help='Database server port (default: 3050)')
    parser.add_argument('--db', dest='db_path',
                        help='Path to the database file')
    parser.add_argument('--user', default='sysdba',
                        help='Database username (default: sysdba)')
    parser.add_argument('--password', default='masterkey',
                        help='Database password (default: masterkey)')
    parser.add_argument('--output', '-o',
                        help='Output file path (default: stdout)')
    parser.add_argument('--table', '-t',
                        help='Only show schema for specific table')

    args = parser.parse_args()

    # Determine database path
    db_path = args.db_path
    if not db_path:
        # Auto-detect: check container paths first, then host path
        import glob
        import os

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
                print(f"Auto-detected database: {db_path}")
                break

        if not db_path:
            # Try to use the default Microsip path (host mode)
            try:
                from probe.utils.fdb_helper import get_microsip_fdb_file_path
                db_path = get_microsip_fdb_file_path()
            except Exception as e:
                print(f"Error: Could not determine database path: {e}")
                print("Please specify --db /path/to/database.fdb")
                sys.exit(1)

    try:
        schema = discover_schema(
            host=args.host,
            port=args.port,
            db_path=db_path,
            user=args.user,
            password=args.password
        )

        schema_dict = schema_to_dict(schema)

        # Filter to specific table if requested
        if args.table:
            table_upper = args.table.upper()
            if table_upper not in schema_dict["tables"]:
                print(f"Error: Table '{args.table}' not found")
                print("Available tables:")
                for t in sorted(schema_dict["tables"].keys()):
                    print(f"  - {t}")
                sys.exit(1)
            schema_dict["tables"] = {table_upper: schema_dict["tables"][table_upper]}

        # Output
        json_output = json.dumps(schema_dict, indent=2)

        if args.output:
            with open(args.output, 'w') as f:
                f.write(json_output)
            print(f"Schema written to {args.output}")
            print(f"Tables discovered: {len(schema_dict['tables'])}")
        else:
            print(json_output)

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
