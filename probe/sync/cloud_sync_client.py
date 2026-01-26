"""
Cloud Sync Client for sending change events to the cloud API.

This module replaces Kafka with a simple HTTPS client that:
- Sends change events directly to a cloud API
- Buffers failed events locally for retry
- Implements exponential backoff for resilience
"""

import json
import logging
import threading
import time
from datetime import date
from decimal import Decimal
from typing import Any, Dict, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from .local_buffer import LocalBuffer

logger = logging.getLogger(__name__)


class CloudSyncClient:
    """
    HTTPS client for syncing changes to the cloud API.

    This class provides:
    - Direct HTTPS communication with cloud API
    - Local SQLite buffering for offline resilience
    - Exponential backoff retry logic
    - Background retry thread for buffered events
    """

    DEFAULT_ENDPOINT = "http://localhost:8080/api/changes"
    MAX_RETRIES = 5
    BASE_RETRY_DELAY = 1.0  # seconds
    MAX_RETRY_DELAY = 60.0  # seconds
    BACKGROUND_RETRY_INTERVAL = 30.0  # seconds

    def __init__(
        self,
        endpoint: Optional[str] = None,
        api_key: Optional[str] = None,
        buffer_path: Optional[str] = None,
        enable_background_retry: bool = True
    ):
        """
        Initialize the cloud sync client.

        Args:
            endpoint: Cloud API endpoint URL
            api_key: API key for authentication
            buffer_path: Path to local SQLite buffer file
            enable_background_retry: Whether to enable background retry thread
        """
        self.endpoint = endpoint or self.DEFAULT_ENDPOINT
        self.api_key = api_key
        self.buffer = LocalBuffer(buffer_path)
        self._stop_event = threading.Event()
        self._retry_thread: Optional[threading.Thread] = None

        if enable_background_retry:
            self._start_background_retry()

    @staticmethod
    def json_serialize_fallback(obj: Any) -> Any:
        """
        JSON serialization fallback for non-standard types.

        Args:
            obj: Object to serialize

        Returns:
            Serializable representation of the object

        Raises:
            TypeError: If object is not serializable
        """
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, date):
            return obj.isoformat()
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

    def _build_headers(self) -> Dict[str, str]:
        """Build HTTP headers for the request."""
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "ApisProbe/0.1"
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _send_request(self, payload: Dict[str, Any]) -> bool:
        """
        Send a single request to the cloud API.

        Args:
            payload: The change event data to send

        Returns:
            True if successful, False otherwise
        """
        try:
            data = json.dumps(payload, default=self.json_serialize_fallback).encode('utf-8')
            request = Request(
                self.endpoint,
                data=data,
                headers=self._build_headers(),
                method='POST'
            )
            with urlopen(request, timeout=30) as response:
                if response.status in (200, 201, 202):
                    return True
                logger.warning(f"Unexpected response status: {response.status}")
                return False
        except HTTPError as e:
            logger.error(f"HTTP error sending change: {e.code} {e.reason}")
            return False
        except URLError as e:
            logger.error(f"URL error sending change: {e.reason}")
            return False
        except Exception as e:
            logger.error(f"Error sending change: {e}")
            return False

    def _send_with_retry(self, payload: Dict[str, Any]) -> bool:
        """
        Send a request with exponential backoff retry.

        Args:
            payload: The change event data to send

        Returns:
            True if eventually successful, False if all retries failed
        """
        delay = self.BASE_RETRY_DELAY
        last_error = None

        for attempt in range(self.MAX_RETRIES):
            if self._send_request(payload):
                if attempt > 0:
                    logger.info(f"Successfully sent after {attempt + 1} attempts")
                return True

            last_error = f"Attempt {attempt + 1} failed"

            if attempt < self.MAX_RETRIES - 1:
                jitter = delay * 0.1 * (0.5 - time.time() % 1)
                sleep_time = min(delay + jitter, self.MAX_RETRY_DELAY)
                logger.debug(f"Retry {attempt + 1}/{self.MAX_RETRIES} in {sleep_time:.1f}s")
                time.sleep(sleep_time)
                delay *= 2

        return False

    def send(self, payload: Dict[str, Any]) -> bool:
        """
        Send a change event to the cloud API.

        If the send fails after retries, the event is buffered locally
        for later retry.

        Args:
            payload: The change event data to send

        Returns:
            True if sent successfully, False if buffered for retry
        """
        if self._send_with_retry(payload):
            return True

        # Buffer for later retry
        self.buffer.add(payload, error="Max retries exceeded")
        logger.warning(f"Buffered change event for later retry. Buffer size: {self.buffer.count_pending()}")
        return False

    def send_insert(self, table: str, row_data: tuple, timestamp: float) -> bool:
        """
        Send an INSERT change event.

        Args:
            table: Name of the table
            row_data: Tuple of row values
            timestamp: Unix timestamp of the change

        Returns:
            True if sent successfully, False if buffered
        """
        payload = {
            "type": "INSERT",
            "table": table,
            "row_data": row_data,
            "timestamp": timestamp
        }
        return self.send(payload)

    def send_update(self, table: str, row_data: tuple, timestamp: float) -> bool:
        """
        Send an UPDATE change event.

        Args:
            table: Name of the table
            row_data: Tuple of row values
            timestamp: Unix timestamp of the change

        Returns:
            True if sent successfully, False if buffered
        """
        payload = {
            "type": "UPDATE",
            "table": table,
            "row_data": row_data,
            "timestamp": timestamp
        }
        return self.send(payload)

    def send_delete(self, table: str, primary_key: str, value: Any, timestamp: float) -> bool:
        """
        Send a DELETE change event.

        Args:
            table: Name of the table
            primary_key: Name of the primary key column
            value: Value of the primary key
            timestamp: Unix timestamp of the change

        Returns:
            True if sent successfully, False if buffered
        """
        payload = {
            "type": "DELETE",
            "table": table,
            "primary_key": primary_key,
            "value": value,
            "timestamp": timestamp
        }
        return self.send(payload)

    def _start_background_retry(self) -> None:
        """Start the background retry thread."""
        self._retry_thread = threading.Thread(
            target=self._background_retry_loop,
            name="CloudSyncRetry",
            daemon=True
        )
        self._retry_thread.start()
        logger.debug("Background retry thread started")

    def _background_retry_loop(self) -> None:
        """Background loop for retrying buffered events."""
        while not self._stop_event.is_set():
            try:
                self._retry_buffered()
            except Exception as e:
                logger.error(f"Error in background retry loop: {e}")

            self._stop_event.wait(self.BACKGROUND_RETRY_INTERVAL)

    def _retry_buffered(self) -> None:
        """Retry sending buffered events."""
        pending = self.buffer.get_pending(limit=50)
        if not pending:
            return

        logger.info(f"Retrying {len(pending)} buffered events")

        for record in pending:
            if self._stop_event.is_set():
                break

            if self._send_request(record['payload']):
                self.buffer.mark_sent(record['id'])
                logger.debug(f"Successfully sent buffered event {record['id']}")
            else:
                self.buffer.mark_failed(record['id'], "Retry failed")

    def flush_buffer(self) -> int:
        """
        Attempt to send all buffered events immediately.

        Returns:
            Number of events successfully sent
        """
        sent_count = 0
        pending = self.buffer.get_pending(limit=1000)

        for record in pending:
            if self._send_request(record['payload']):
                self.buffer.mark_sent(record['id'])
                sent_count += 1
            else:
                self.buffer.mark_failed(record['id'], "Flush retry failed")

        return sent_count

    def get_buffer_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the local buffer.

        Returns:
            Dictionary with buffer statistics
        """
        return {
            "pending_count": self.buffer.count_pending(),
            "endpoint": self.endpoint,
            "connected": self._test_connection()
        }

    def _test_connection(self) -> bool:
        """Test if the cloud API is reachable."""
        try:
            # Send a simple health check or empty payload
            request = Request(
                self.endpoint.rstrip('/') + '/health',
                headers=self._build_headers(),
                method='GET'
            )
            with urlopen(request, timeout=5) as response:
                return response.status == 200
        except Exception:
            return False

    def close(self) -> None:
        """Close the client and stop background threads."""
        self._stop_event.set()
        if self._retry_thread and self._retry_thread.is_alive():
            self._retry_thread.join(timeout=5)
        self.buffer.close()
        logger.debug("Cloud sync client closed")
