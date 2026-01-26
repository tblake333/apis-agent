"""
Local SQLite buffer for offline resilience.

This module provides a persistent queue for change events that couldn't be
sent to the cloud API. Events are stored locally and retried when connectivity
is restored.
"""

import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any


class LocalBuffer:
    """
    SQLite-based local buffer for storing change events when offline.

    This class provides:
    - Persistent storage of failed change events
    - FIFO queue semantics for retry
    - Thread-safe operations
    - Automatic cleanup of successfully synced events
    """

    DEFAULT_DB_PATH = "probe_buffer.db"

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the local buffer.

        Args:
            db_path: Path to the SQLite database file. If None, uses default.
                     Use ":memory:" for in-memory database (useful for testing).
        """
        self.db_path = db_path or self.DEFAULT_DB_PATH
        self._lock = threading.Lock()
        self._is_memory = self.db_path == ":memory:"
        self._shared_conn: Optional[sqlite3.Connection] = None
        self._init_database()

    def _init_database(self) -> None:
        """Initialize the SQLite database schema."""
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pending_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    retry_count INTEGER DEFAULT 0,
                    last_error TEXT,
                    last_retry_at TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_created_at
                ON pending_changes(created_at)
            """)
            conn.commit()

    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection."""
        # For in-memory databases, reuse the same connection
        if self._is_memory:
            if self._shared_conn is None:
                self._shared_conn = sqlite3.connect(":memory:", check_same_thread=False)
                self._shared_conn.row_factory = sqlite3.Row
            return self._shared_conn

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def add(self, payload: Dict[str, Any], error: Optional[str] = None) -> int:
        """
        Add a change event to the buffer.

        Args:
            payload: The change event data to store
            error: Optional error message from the failed send attempt

        Returns:
            The ID of the inserted record
        """
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.execute(
                    """
                    INSERT INTO pending_changes (payload, created_at, last_error)
                    VALUES (?, ?, ?)
                    """,
                    (json.dumps(payload), datetime.utcnow().isoformat(), error)
                )
                conn.commit()
                return cursor.lastrowid

    def get_pending(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get pending changes from the buffer in FIFO order.

        Args:
            limit: Maximum number of records to return

        Returns:
            List of pending change records with id, payload, and metadata
        """
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.execute(
                    """
                    SELECT id, payload, created_at, retry_count, last_error
                    FROM pending_changes
                    ORDER BY created_at ASC
                    LIMIT ?
                    """,
                    (limit,)
                )
                rows = cursor.fetchall()
                return [
                    {
                        'id': row['id'],
                        'payload': json.loads(row['payload']),
                        'created_at': row['created_at'],
                        'retry_count': row['retry_count'],
                        'last_error': row['last_error']
                    }
                    for row in rows
                ]

    def mark_sent(self, record_id: int) -> None:
        """
        Remove a successfully sent record from the buffer.

        Args:
            record_id: The ID of the record to remove
        """
        with self._lock:
            with self._get_connection() as conn:
                conn.execute(
                    "DELETE FROM pending_changes WHERE id = ?",
                    (record_id,)
                )
                conn.commit()

    def mark_failed(self, record_id: int, error: str) -> None:
        """
        Update a record after a failed retry attempt.

        Args:
            record_id: The ID of the record
            error: The error message from the failed attempt
        """
        with self._lock:
            with self._get_connection() as conn:
                conn.execute(
                    """
                    UPDATE pending_changes
                    SET retry_count = retry_count + 1,
                        last_error = ?,
                        last_retry_at = ?
                    WHERE id = ?
                    """,
                    (error, datetime.utcnow().isoformat(), record_id)
                )
                conn.commit()

    def count_pending(self) -> int:
        """
        Get the count of pending changes in the buffer.

        Returns:
            Number of pending records
        """
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.execute(
                    "SELECT COUNT(*) as count FROM pending_changes"
                )
                return cursor.fetchone()['count']

    def clear(self) -> int:
        """
        Clear all pending changes from the buffer.

        Returns:
            Number of records deleted
        """
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.execute("DELETE FROM pending_changes")
                conn.commit()
                return cursor.rowcount

    def close(self) -> None:
        """Close the buffer and any open connections."""
        if self._shared_conn is not None:
            self._shared_conn.close()
            self._shared_conn = None
