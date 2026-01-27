"""
Tests for schema_discovery.py module.

This module covers:
- Type conversion for Firebird types
- Dataclass creation and serialization
- Schema discovery with real Firebird databases
"""
import pytest
import sys
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Import the module under test
sys.path.insert(0, str(__file__).rsplit('/probe/', 1)[0])
from schema_discovery import (
    ColumnInfo,
    TableInfo,
    SchemaInfo,
    get_firebird_type,
    schema_to_dict,
    discover_schema,
)


class TestGetFirebirdType:
    """Test cases for get_firebird_type function."""

    @pytest.mark.unit
    def test_smallint(self):
        """Test SMALLINT type conversion."""
        result = get_firebird_type(7, 0, 2, 0, 0, 0)
        assert result == "SMALLINT"

    @pytest.mark.unit
    def test_integer(self):
        """Test INTEGER type conversion."""
        result = get_firebird_type(8, 0, 4, 0, 0, 0)
        assert result == "INTEGER"

    @pytest.mark.unit
    def test_bigint(self):
        """Test BIGINT type conversion."""
        result = get_firebird_type(16, 0, 8, 0, 0, 0)
        assert result == "BIGINT"

    @pytest.mark.unit
    def test_float(self):
        """Test FLOAT type conversion."""
        result = get_firebird_type(10, 0, 4, 0, 0, 0)
        assert result == "FLOAT"

    @pytest.mark.unit
    def test_double_precision(self):
        """Test DOUBLE PRECISION type conversion."""
        result = get_firebird_type(27, 0, 8, 0, 0, 0)
        assert result == "DOUBLE PRECISION"

    @pytest.mark.unit
    def test_date(self):
        """Test DATE type conversion."""
        result = get_firebird_type(12, 0, 4, 0, 0, 0)
        assert result == "DATE"

    @pytest.mark.unit
    def test_time(self):
        """Test TIME type conversion."""
        result = get_firebird_type(13, 0, 4, 0, 0, 0)
        assert result == "TIME"

    @pytest.mark.unit
    def test_timestamp(self):
        """Test TIMESTAMP type conversion."""
        result = get_firebird_type(35, 0, 8, 0, 0, 0)
        assert result == "TIMESTAMP"

    @pytest.mark.unit
    def test_char_with_length(self):
        """Test CHAR type with length."""
        result = get_firebird_type(14, 0, 50, 0, 0, 50)
        assert result == "CHAR(50)"

    @pytest.mark.unit
    def test_varchar_with_length(self):
        """Test VARCHAR type with length."""
        result = get_firebird_type(37, 0, 100, 0, 0, 100)
        assert result == "VARCHAR(100)"

    @pytest.mark.unit
    def test_blob(self):
        """Test BLOB type conversion."""
        result = get_firebird_type(261, 0, 0, 0, 0, 0)
        assert result == "BLOB"

    @pytest.mark.unit
    def test_blob_subtype_text(self):
        """Test BLOB SUB_TYPE TEXT conversion."""
        result = get_firebird_type(261, 1, 0, 0, 0, 0)
        assert result == "BLOB SUB_TYPE TEXT"

    @pytest.mark.unit
    def test_numeric_from_smallint(self):
        """Test NUMERIC type derived from SMALLINT with scale."""
        result = get_firebird_type(7, 0, 2, 5, -2, 0)
        assert result == "NUMERIC(5,2)"

    @pytest.mark.unit
    def test_numeric_from_integer(self):
        """Test NUMERIC type derived from INTEGER with scale."""
        result = get_firebird_type(8, 0, 4, 10, -4, 0)
        assert result == "NUMERIC(10,4)"

    @pytest.mark.unit
    def test_numeric_from_bigint(self):
        """Test NUMERIC type derived from BIGINT with scale."""
        result = get_firebird_type(16, 0, 8, 18, -6, 0)
        assert result == "NUMERIC(18,6)"

    @pytest.mark.unit
    def test_unknown_type(self):
        """Test unknown type code handling."""
        result = get_firebird_type(999, 0, 0, 0, 0, 0)
        assert result == "UNKNOWN(999)"

    @pytest.mark.unit
    def test_varchar_no_length(self):
        """Test VARCHAR without char_length falls back to base type."""
        result = get_firebird_type(37, 0, 100, 0, 0, 0)
        assert result == "VARCHAR"


class TestDataclasses:
    """Test cases for dataclass creation and attributes."""

    @pytest.mark.unit
    def test_column_info_creation(self):
        """Test ColumnInfo dataclass creation."""
        col = ColumnInfo(
            name="TEST_COLUMN",
            type="VARCHAR(100)",
            nullable=True,
            default="'default_value'",
            references="OTHER_TABLE.ID"
        )
        assert col.name == "TEST_COLUMN"
        assert col.type == "VARCHAR(100)"
        assert col.nullable is True
        assert col.default == "'default_value'"
        assert col.references == "OTHER_TABLE.ID"

    @pytest.mark.unit
    def test_column_info_defaults(self):
        """Test ColumnInfo with default values."""
        col = ColumnInfo(
            name="TEST_COLUMN",
            type="INTEGER",
            nullable=False
        )
        assert col.default is None
        assert col.references is None

    @pytest.mark.unit
    def test_table_info_creation(self):
        """Test TableInfo dataclass creation."""
        col = ColumnInfo(name="ID", type="INTEGER", nullable=False)
        table = TableInfo(
            name="TEST_TABLE",
            primary_key="ID",
            columns={"ID": col}
        )
        assert table.name == "TEST_TABLE"
        assert table.primary_key == "ID"
        assert "ID" in table.columns

    @pytest.mark.unit
    def test_table_info_defaults(self):
        """Test TableInfo with default values."""
        table = TableInfo(name="TEST_TABLE")
        assert table.primary_key is None
        assert table.columns == {}

    @pytest.mark.unit
    def test_schema_info_creation(self):
        """Test SchemaInfo dataclass creation."""
        table = TableInfo(name="TEST_TABLE")
        schema = SchemaInfo(
            tables={"TEST_TABLE": table},
            source_db="/path/to/db.fdb",
            discovered_at="2025-01-01T00:00:00"
        )
        assert "TEST_TABLE" in schema.tables
        assert schema.source_db == "/path/to/db.fdb"
        assert schema.discovered_at == "2025-01-01T00:00:00"

    @pytest.mark.unit
    def test_schema_info_defaults(self):
        """Test SchemaInfo with default values."""
        schema = SchemaInfo()
        assert schema.tables == {}
        assert schema.source_db == ""
        assert schema.discovered_at == ""


class TestSchemaToDict:
    """Test cases for schema_to_dict function."""

    @pytest.mark.unit
    def test_empty_schema(self):
        """Test converting empty schema to dict."""
        schema = SchemaInfo(
            source_db="/path/to/db.fdb",
            discovered_at="2025-01-01T00:00:00"
        )
        result = schema_to_dict(schema)

        assert result["source_db"] == "/path/to/db.fdb"
        assert result["discovered_at"] == "2025-01-01T00:00:00"
        assert result["tables"] == {}

    @pytest.mark.unit
    def test_schema_with_table(self):
        """Test converting schema with table to dict."""
        col = ColumnInfo(name="ID", type="INTEGER", nullable=False)
        table = TableInfo(name="TEST_TABLE", primary_key="ID", columns={"ID": col})
        schema = SchemaInfo(
            tables={"TEST_TABLE": table},
            source_db="/path/to/db.fdb",
            discovered_at="2025-01-01T00:00:00"
        )

        result = schema_to_dict(schema)

        assert "TEST_TABLE" in result["tables"]
        table_dict = result["tables"]["TEST_TABLE"]
        assert table_dict["primary_key"] == "ID"
        assert "ID" in table_dict["columns"]
        assert table_dict["columns"]["ID"]["type"] == "INTEGER"
        assert table_dict["columns"]["ID"]["nullable"] is False

    @pytest.mark.unit
    def test_schema_with_default_value(self):
        """Test converting schema with column default value."""
        col = ColumnInfo(
            name="STATUS",
            type="VARCHAR(20)",
            nullable=True,
            default="'ACTIVE'"
        )
        table = TableInfo(name="TEST_TABLE", columns={"STATUS": col})
        schema = SchemaInfo(tables={"TEST_TABLE": table})

        result = schema_to_dict(schema)

        col_dict = result["tables"]["TEST_TABLE"]["columns"]["STATUS"]
        assert col_dict["default"] == "'ACTIVE'"

    @pytest.mark.unit
    def test_schema_with_foreign_key(self):
        """Test converting schema with foreign key reference."""
        col = ColumnInfo(
            name="USER_ID",
            type="INTEGER",
            nullable=False,
            references="USERS.ID"
        )
        table = TableInfo(name="ORDERS", columns={"USER_ID": col})
        schema = SchemaInfo(tables={"ORDERS": table})

        result = schema_to_dict(schema)

        col_dict = result["tables"]["ORDERS"]["columns"]["USER_ID"]
        assert col_dict["references"] == "USERS.ID"

    @pytest.mark.unit
    def test_schema_excludes_none_default(self):
        """Test that None default is not included in dict."""
        col = ColumnInfo(name="ID", type="INTEGER", nullable=False, default=None)
        table = TableInfo(name="TEST_TABLE", columns={"ID": col})
        schema = SchemaInfo(tables={"TEST_TABLE": table})

        result = schema_to_dict(schema)

        col_dict = result["tables"]["TEST_TABLE"]["columns"]["ID"]
        assert "default" not in col_dict

    @pytest.mark.unit
    def test_schema_excludes_none_references(self):
        """Test that None references is not included in dict."""
        col = ColumnInfo(name="ID", type="INTEGER", nullable=False, references=None)
        table = TableInfo(name="TEST_TABLE", columns={"ID": col})
        schema = SchemaInfo(tables={"TEST_TABLE": table})

        result = schema_to_dict(schema)

        col_dict = result["tables"]["TEST_TABLE"]["columns"]["ID"]
        assert "references" not in col_dict

    @pytest.mark.unit
    def test_schema_multiple_tables(self):
        """Test converting schema with multiple tables."""
        col1 = ColumnInfo(name="ID", type="INTEGER", nullable=False)
        col2 = ColumnInfo(name="NAME", type="VARCHAR(100)", nullable=True)
        table1 = TableInfo(name="USERS", primary_key="ID", columns={"ID": col1, "NAME": col2})

        col3 = ColumnInfo(name="ORDER_ID", type="INTEGER", nullable=False)
        col4 = ColumnInfo(name="USER_ID", type="INTEGER", nullable=False, references="USERS.ID")
        table2 = TableInfo(name="ORDERS", primary_key="ORDER_ID", columns={"ORDER_ID": col3, "USER_ID": col4})

        schema = SchemaInfo(tables={"USERS": table1, "ORDERS": table2})

        result = schema_to_dict(schema)

        assert "USERS" in result["tables"]
        assert "ORDERS" in result["tables"]
        assert len(result["tables"]["USERS"]["columns"]) == 2
        assert len(result["tables"]["ORDERS"]["columns"]) == 2


class TestDiscoverSchemaIntegration:
    """Integration tests for discover_schema function with real Firebird DB."""

    @pytest.mark.integration
    def test_discover_schema_empty_db(self, fdb_connection, temp_fdb_file):
        """Test discovering schema from empty database."""
        schema = discover_schema(
            host='localhost',
            port=3050,
            db_path=temp_fdb_file,
            user='SYSDBA',
            password='masterkey'
        )

        assert schema.source_db == temp_fdb_file
        assert schema.discovered_at != ""
        assert schema.tables == {}

    @pytest.mark.integration
    def test_discover_schema_single_table_various_types(self, fdb_connection, temp_fdb_file):
        """Test discovering schema with various column types."""
        fdb_connection.execute_immediate("""
            CREATE TABLE TEST_TYPES (
                ID INTEGER NOT NULL PRIMARY KEY,
                SMALL_COL SMALLINT,
                BIG_COL BIGINT,
                FLOAT_COL FLOAT,
                DOUBLE_COL DOUBLE PRECISION,
                STR_COL VARCHAR(100),
                CHAR_COL CHAR(10),
                DATE_COL DATE,
                TIME_COL TIME,
                TS_COL TIMESTAMP,
                BLOB_COL BLOB,
                TEXT_COL BLOB SUB_TYPE TEXT,
                NUM_COL NUMERIC(10, 2)
            )
        """)
        fdb_connection.commit()

        schema = discover_schema(
            host='localhost',
            port=3050,
            db_path=temp_fdb_file,
            user='SYSDBA',
            password='masterkey'
        )

        assert "TEST_TYPES" in schema.tables
        table = schema.tables["TEST_TYPES"]

        assert table.primary_key == "ID"
        assert "ID" in table.columns
        assert "SMALL_COL" in table.columns
        assert "BIG_COL" in table.columns
        assert "STR_COL" in table.columns

        # Verify types
        assert table.columns["ID"].type == "INTEGER"
        assert table.columns["SMALL_COL"].type == "SMALLINT"
        assert table.columns["BIG_COL"].type == "BIGINT"
        assert "VARCHAR" in table.columns["STR_COL"].type
        assert "CHAR" in table.columns["CHAR_COL"].type

    @pytest.mark.integration
    def test_discover_schema_primary_key_detection(self, fdb_connection, temp_fdb_file):
        """Test primary key detection."""
        fdb_connection.execute_immediate("""
            CREATE TABLE TEST_PK (
                MY_PRIMARY_KEY INTEGER NOT NULL PRIMARY KEY,
                OTHER_COL VARCHAR(50)
            )
        """)
        fdb_connection.commit()

        schema = discover_schema(
            host='localhost',
            port=3050,
            db_path=temp_fdb_file,
            user='SYSDBA',
            password='masterkey'
        )

        assert schema.tables["TEST_PK"].primary_key == "MY_PRIMARY_KEY"

    @pytest.mark.integration
    def test_discover_schema_foreign_key_relationships(self, fdb_connection, temp_fdb_file):
        """Test foreign key relationship discovery."""
        fdb_connection.execute_immediate("""
            CREATE TABLE PARENT_TABLE (
                PARENT_ID INTEGER NOT NULL PRIMARY KEY,
                NAME VARCHAR(100)
            )
        """)
        fdb_connection.commit()

        fdb_connection.execute_immediate("""
            CREATE TABLE CHILD_TABLE (
                CHILD_ID INTEGER NOT NULL PRIMARY KEY,
                PARENT_REF INTEGER NOT NULL REFERENCES PARENT_TABLE(PARENT_ID),
                DESCRIPTION VARCHAR(200)
            )
        """)
        fdb_connection.commit()

        schema = discover_schema(
            host='localhost',
            port=3050,
            db_path=temp_fdb_file,
            user='SYSDBA',
            password='masterkey'
        )

        assert "PARENT_TABLE" in schema.tables
        assert "CHILD_TABLE" in schema.tables

        child_table = schema.tables["CHILD_TABLE"]
        assert child_table.columns["PARENT_REF"].references == "PARENT_TABLE.PARENT_ID"

    @pytest.mark.integration
    def test_discover_schema_nullable_and_defaults(self, fdb_connection, temp_fdb_file):
        """Test nullable detection and default value extraction."""
        fdb_connection.execute_immediate("""
            CREATE TABLE TEST_NULLABLE (
                ID INTEGER NOT NULL PRIMARY KEY,
                REQUIRED_COL VARCHAR(50) NOT NULL,
                OPTIONAL_COL VARCHAR(50),
                DEFAULT_COL VARCHAR(20) DEFAULT 'ACTIVE',
                DEFAULT_NUM INTEGER DEFAULT 0
            )
        """)
        fdb_connection.commit()

        schema = discover_schema(
            host='localhost',
            port=3050,
            db_path=temp_fdb_file,
            user='SYSDBA',
            password='masterkey'
        )

        table = schema.tables["TEST_NULLABLE"]

        assert table.columns["ID"].nullable is False
        assert table.columns["REQUIRED_COL"].nullable is False
        assert table.columns["OPTIONAL_COL"].nullable is True

        # Check defaults
        assert table.columns["DEFAULT_COL"].default == "'ACTIVE'"
        assert table.columns["DEFAULT_NUM"].default == "0"

    @pytest.mark.integration
    def test_discover_schema_multiple_tables(self, fdb_connection, temp_fdb_file):
        """Test discovering multiple tables."""
        fdb_connection.execute_immediate("""
            CREATE TABLE TABLE_ONE (
                ID INTEGER NOT NULL PRIMARY KEY,
                NAME VARCHAR(50)
            )
        """)
        fdb_connection.commit()

        fdb_connection.execute_immediate("""
            CREATE TABLE TABLE_TWO (
                ID INTEGER NOT NULL PRIMARY KEY,
                DESCRIPTION VARCHAR(200)
            )
        """)
        fdb_connection.commit()

        fdb_connection.execute_immediate("""
            CREATE TABLE TABLE_THREE (
                ID INTEGER NOT NULL PRIMARY KEY,
                CODE CHAR(10)
            )
        """)
        fdb_connection.commit()

        schema = discover_schema(
            host='localhost',
            port=3050,
            db_path=temp_fdb_file,
            user='SYSDBA',
            password='masterkey'
        )

        assert len(schema.tables) == 3
        assert "TABLE_ONE" in schema.tables
        assert "TABLE_TWO" in schema.tables
        assert "TABLE_THREE" in schema.tables

    @pytest.mark.integration
    def test_discover_schema_connection_failure(self):
        """Test connection failure handling."""
        with pytest.raises(Exception):
            discover_schema(
                host='localhost',
                port=3050,
                db_path='/nonexistent/path/to/db.fdb',
                user='SYSDBA',
                password='masterkey'
            )


class TestDiscoverSchemaWithMocks:
    """Unit tests for discover_schema using mocks."""

    @pytest.mark.unit
    @patch('schema_discovery.fdb.connect')
    def test_discover_schema_dsn_local(self, mock_connect):
        """Test DSN construction for local host."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        mock_connect.return_value = mock_conn

        discover_schema(
            host='localhost',
            port=3050,
            db_path='/path/to/db.fdb',
            user='SYSDBA',
            password='masterkey'
        )

        mock_connect.assert_called_once_with(
            dsn='/path/to/db.fdb',
            user='SYSDBA',
            password='masterkey'
        )

    @pytest.mark.unit
    @patch('schema_discovery.fdb.connect')
    def test_discover_schema_dsn_remote(self, mock_connect):
        """Test DSN construction for remote host."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        mock_connect.return_value = mock_conn

        discover_schema(
            host='remote-server',
            port=3050,
            db_path='/path/to/db.fdb',
            user='SYSDBA',
            password='masterkey'
        )

        mock_connect.assert_called_once_with(
            dsn='remote-server/3050:/path/to/db.fdb',
            user='SYSDBA',
            password='masterkey'
        )

    @pytest.mark.unit
    @patch('schema_discovery.fdb.connect')
    def test_discover_schema_dsn_127_0_0_1(self, mock_connect):
        """Test DSN construction for 127.0.0.1."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        mock_connect.return_value = mock_conn

        discover_schema(
            host='127.0.0.1',
            port=3050,
            db_path='/path/to/db.fdb',
            user='SYSDBA',
            password='masterkey'
        )

        mock_connect.assert_called_once_with(
            dsn='/path/to/db.fdb',
            user='SYSDBA',
            password='masterkey'
        )
