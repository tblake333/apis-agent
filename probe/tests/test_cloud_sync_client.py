"""
Tests for CloudSyncClient class.

This module covers:
- Initialization and configuration
- HTTP request handling with mocks
- Retry logic with exponential backoff
- Local buffering on failures
- Background retry thread
"""
import pytest
import json
import time
import threading
from datetime import date
from decimal import Decimal
from unittest.mock import Mock, patch, MagicMock, PropertyMock
from urllib.error import HTTPError, URLError
from io import BytesIO

from sync.cloud_sync_client import CloudSyncClient
from sync.local_buffer import LocalBuffer


class TestCloudSyncClientInit:
    """Test cases for CloudSyncClient initialization."""

    @pytest.mark.unit
    def test_init_default_endpoint(self):
        """Test initialization with default endpoint."""
        with patch.object(CloudSyncClient, '_start_background_retry'):
            client = CloudSyncClient(enable_background_retry=False)
            assert client.endpoint == CloudSyncClient.DEFAULT_ENDPOINT
            client.close()

    @pytest.mark.unit
    def test_init_custom_endpoint(self):
        """Test initialization with custom endpoint."""
        with patch.object(CloudSyncClient, '_start_background_retry'):
            client = CloudSyncClient(
                endpoint="https://api.example.com/changes",
                enable_background_retry=False
            )
            assert client.endpoint == "https://api.example.com/changes"
            client.close()

    @pytest.mark.unit
    def test_init_with_api_key(self):
        """Test initialization with API key."""
        with patch.object(CloudSyncClient, '_start_background_retry'):
            client = CloudSyncClient(
                api_key="test-api-key-123",
                enable_background_retry=False
            )
            assert client.api_key == "test-api-key-123"
            client.close()

    @pytest.mark.unit
    def test_init_with_buffer_path(self):
        """Test initialization with custom buffer path."""
        with patch.object(CloudSyncClient, '_start_background_retry'):
            client = CloudSyncClient(
                buffer_path=":memory:",
                enable_background_retry=False
            )
            assert client.buffer.db_path == ":memory:"
            client.close()

    @pytest.mark.unit
    def test_init_starts_background_retry_by_default(self):
        """Test that background retry starts by default."""
        with patch.object(CloudSyncClient, '_start_background_retry') as mock_start:
            client = CloudSyncClient(buffer_path=":memory:")
            mock_start.assert_called_once()
            client.close()

    @pytest.mark.unit
    def test_init_background_retry_disabled(self):
        """Test that background retry can be disabled."""
        with patch.object(CloudSyncClient, '_start_background_retry') as mock_start:
            client = CloudSyncClient(
                buffer_path=":memory:",
                enable_background_retry=False
            )
            mock_start.assert_not_called()
            client.close()


class TestBuildHeaders:
    """Test cases for _build_headers method."""

    @pytest.mark.unit
    def test_build_headers_without_api_key(self):
        """Test header building without API key."""
        with patch.object(CloudSyncClient, '_start_background_retry'):
            client = CloudSyncClient(
                buffer_path=":memory:",
                enable_background_retry=False
            )
            headers = client._build_headers()

            assert headers["Content-Type"] == "application/json"
            assert headers["User-Agent"] == "ApisProbe/0.1"
            assert "Authorization" not in headers
            client.close()

    @pytest.mark.unit
    def test_build_headers_with_api_key(self):
        """Test header building with API key."""
        with patch.object(CloudSyncClient, '_start_background_retry'):
            client = CloudSyncClient(
                api_key="my-secret-key",
                buffer_path=":memory:",
                enable_background_retry=False
            )
            headers = client._build_headers()

            assert headers["Authorization"] == "Bearer my-secret-key"
            client.close()


class TestJsonSerializeFallback:
    """Test cases for json_serialize_fallback static method."""

    @pytest.mark.unit
    def test_serialize_decimal(self):
        """Test Decimal serialization."""
        result = CloudSyncClient.json_serialize_fallback(Decimal("123.45"))
        assert result == 123.45
        assert isinstance(result, float)

    @pytest.mark.unit
    def test_serialize_date(self):
        """Test date serialization."""
        d = date(2025, 1, 15)
        result = CloudSyncClient.json_serialize_fallback(d)
        assert result == "2025-01-15"

    @pytest.mark.unit
    def test_serialize_unsupported_type(self):
        """Test that unsupported types raise TypeError."""
        with pytest.raises(TypeError) as excinfo:
            CloudSyncClient.json_serialize_fallback(object())
        assert "not JSON serializable" in str(excinfo.value)

    @pytest.mark.unit
    def test_serialize_in_json_dumps(self):
        """Test fallback works with json.dumps."""
        payload = {
            "amount": Decimal("99.99"),
            "created": date(2025, 1, 15)
        }
        result = json.dumps(payload, default=CloudSyncClient.json_serialize_fallback)
        parsed = json.loads(result)

        assert parsed["amount"] == 99.99
        assert parsed["created"] == "2025-01-15"


class TestSendRequest:
    """Test cases for _send_request method."""

    @pytest.fixture
    def client(self):
        """Create a client for testing."""
        c = CloudSyncClient(
            endpoint="http://test.example.com/api",
            buffer_path=":memory:",
            enable_background_retry=False
        )
        yield c
        c.close()

    @pytest.mark.unit
    @patch('sync.cloud_sync_client.urlopen')
    def test_send_request_success_200(self, mock_urlopen, client):
        """Test successful request with 200 status."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = client._send_request({"test": "data"})

        assert result is True
        mock_urlopen.assert_called_once()

    @pytest.mark.unit
    @patch('sync.cloud_sync_client.urlopen')
    def test_send_request_success_201(self, mock_urlopen, client):
        """Test successful request with 201 status."""
        mock_response = MagicMock()
        mock_response.status = 201
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = client._send_request({"test": "data"})
        assert result is True

    @pytest.mark.unit
    @patch('sync.cloud_sync_client.urlopen')
    def test_send_request_success_202(self, mock_urlopen, client):
        """Test successful request with 202 status."""
        mock_response = MagicMock()
        mock_response.status = 202
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = client._send_request({"test": "data"})
        assert result is True

    @pytest.mark.unit
    @patch('sync.cloud_sync_client.urlopen')
    def test_send_request_unexpected_status(self, mock_urlopen, client):
        """Test request with unexpected status code."""
        mock_response = MagicMock()
        mock_response.status = 204  # Unexpected status
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = client._send_request({"test": "data"})
        assert result is False

    @pytest.mark.unit
    @patch('sync.cloud_sync_client.urlopen')
    def test_send_request_http_error_4xx(self, mock_urlopen, client):
        """Test request with 4xx HTTP error."""
        mock_urlopen.side_effect = HTTPError(
            url="http://test.example.com",
            code=400,
            msg="Bad Request",
            hdrs={},
            fp=BytesIO(b"")
        )

        result = client._send_request({"test": "data"})
        assert result is False

    @pytest.mark.unit
    @patch('sync.cloud_sync_client.urlopen')
    def test_send_request_http_error_5xx(self, mock_urlopen, client):
        """Test request with 5xx HTTP error."""
        mock_urlopen.side_effect = HTTPError(
            url="http://test.example.com",
            code=500,
            msg="Internal Server Error",
            hdrs={},
            fp=BytesIO(b"")
        )

        result = client._send_request({"test": "data"})
        assert result is False

    @pytest.mark.unit
    @patch('sync.cloud_sync_client.urlopen')
    def test_send_request_timeout(self, mock_urlopen, client):
        """Test request with timeout error."""
        mock_urlopen.side_effect = URLError("timed out")

        result = client._send_request({"test": "data"})
        assert result is False

    @pytest.mark.unit
    @patch('sync.cloud_sync_client.urlopen')
    def test_send_request_connection_refused(self, mock_urlopen, client):
        """Test request with connection refused error."""
        mock_urlopen.side_effect = URLError("Connection refused")

        result = client._send_request({"test": "data"})
        assert result is False

    @pytest.mark.unit
    @patch('sync.cloud_sync_client.urlopen')
    def test_send_request_dns_failure(self, mock_urlopen, client):
        """Test request with DNS resolution failure."""
        mock_urlopen.side_effect = URLError("Name or service not known")

        result = client._send_request({"test": "data"})
        assert result is False

    @pytest.mark.unit
    @patch('sync.cloud_sync_client.urlopen')
    def test_send_request_generic_exception(self, mock_urlopen, client):
        """Test request with generic exception."""
        mock_urlopen.side_effect = Exception("Unknown error")

        result = client._send_request({"test": "data"})
        assert result is False


class TestSendWithRetry:
    """Test cases for _send_with_retry method."""

    @pytest.fixture
    def client(self):
        """Create a client for testing with minimal retry delay."""
        c = CloudSyncClient(
            endpoint="http://test.example.com/api",
            buffer_path=":memory:",
            enable_background_retry=False
        )
        c.BASE_RETRY_DELAY = 0.01  # Speed up tests
        c.MAX_RETRY_DELAY = 0.05
        yield c
        c.close()

    @pytest.mark.unit
    def test_send_with_retry_success_first_attempt(self, client):
        """Test successful send on first attempt."""
        with patch.object(client, '_send_request', return_value=True) as mock_send:
            result = client._send_with_retry({"test": "data"})

            assert result is True
            assert mock_send.call_count == 1

    @pytest.mark.unit
    def test_send_with_retry_success_after_failures(self, client):
        """Test successful send after initial failures."""
        # Fail twice, then succeed
        with patch.object(client, '_send_request', side_effect=[False, False, True]) as mock_send:
            result = client._send_with_retry({"test": "data"})

            assert result is True
            assert mock_send.call_count == 3

    @pytest.mark.unit
    def test_send_with_retry_all_retries_exhausted(self, client):
        """Test when all retries are exhausted."""
        client.MAX_RETRIES = 3  # Reduce for faster test

        with patch.object(client, '_send_request', return_value=False) as mock_send:
            result = client._send_with_retry({"test": "data"})

            assert result is False
            assert mock_send.call_count == 3

    @pytest.mark.unit
    def test_send_with_retry_exponential_backoff(self, client):
        """Test that retry delay increases exponentially."""
        client.MAX_RETRIES = 3
        client.BASE_RETRY_DELAY = 0.01

        delays = []

        def mock_sleep(duration):
            delays.append(duration)

        with patch.object(client, '_send_request', return_value=False):
            with patch('sync.cloud_sync_client.time.sleep', side_effect=mock_sleep):
                client._send_with_retry({"test": "data"})

        # Should have delays between retries
        assert len(delays) == 2  # 3 attempts = 2 sleeps
        # Second delay should be larger than first (exponential backoff)
        assert delays[1] > delays[0]


class TestSend:
    """Test cases for send method."""

    @pytest.fixture
    def client(self):
        """Create a client for testing."""
        c = CloudSyncClient(
            endpoint="http://test.example.com/api",
            buffer_path=":memory:",
            enable_background_retry=False
        )
        yield c
        c.close()

    @pytest.mark.unit
    def test_send_success(self, client):
        """Test successful send."""
        with patch.object(client, '_send_with_retry', return_value=True):
            result = client.send({"test": "data"})

            assert result is True
            assert client.buffer.count_pending() == 0

    @pytest.mark.unit
    def test_send_buffers_on_failure(self, client):
        """Test that failed sends are buffered."""
        with patch.object(client, '_send_with_retry', return_value=False):
            result = client.send({"test": "data"})

            assert result is False
            assert client.buffer.count_pending() == 1

            pending = client.buffer.get_pending()
            assert pending[0]['payload'] == {"test": "data"}
            assert pending[0]['last_error'] == "Max retries exceeded"


class TestSendInsertUpdateDelete:
    """Test cases for send_insert, send_update, send_delete methods."""

    @pytest.fixture
    def client(self):
        """Create a client for testing."""
        c = CloudSyncClient(
            endpoint="http://test.example.com/api",
            buffer_path=":memory:",
            enable_background_retry=False
        )
        yield c
        c.close()

    @pytest.mark.unit
    def test_send_insert_payload_format(self, client):
        """Test INSERT payload format."""
        captured_payload = None

        def capture_send(payload):
            nonlocal captured_payload
            captured_payload = payload
            return True

        with patch.object(client, 'send', side_effect=capture_send):
            client.send_insert(
                table="users",
                row_data=(1, "John", "john@example.com"),
                timestamp=1704067200.0
            )

        assert captured_payload["type"] == "INSERT"
        assert captured_payload["table"] == "users"
        assert captured_payload["row_data"] == (1, "John", "john@example.com")
        assert captured_payload["timestamp"] == 1704067200.0

    @pytest.mark.unit
    def test_send_update_payload_format(self, client):
        """Test UPDATE payload format."""
        captured_payload = None

        def capture_send(payload):
            nonlocal captured_payload
            captured_payload = payload
            return True

        with patch.object(client, 'send', side_effect=capture_send):
            client.send_update(
                table="users",
                row_data=(1, "Jane", "jane@example.com"),
                timestamp=1704067200.0
            )

        assert captured_payload["type"] == "UPDATE"
        assert captured_payload["table"] == "users"
        assert captured_payload["row_data"] == (1, "Jane", "jane@example.com")
        assert captured_payload["timestamp"] == 1704067200.0

    @pytest.mark.unit
    def test_send_delete_payload_format(self, client):
        """Test DELETE payload format."""
        captured_payload = None

        def capture_send(payload):
            nonlocal captured_payload
            captured_payload = payload
            return True

        with patch.object(client, 'send', side_effect=capture_send):
            client.send_delete(
                table="users",
                primary_key="id",
                value=123,
                timestamp=1704067200.0
            )

        assert captured_payload["type"] == "DELETE"
        assert captured_payload["table"] == "users"
        assert captured_payload["primary_key"] == "id"
        assert captured_payload["value"] == 123
        assert captured_payload["timestamp"] == 1704067200.0


class TestBackgroundRetryThread:
    """Test cases for background retry thread."""

    @pytest.mark.unit
    def test_background_retry_thread_starts(self):
        """Test that background retry thread starts."""
        client = CloudSyncClient(
            buffer_path=":memory:",
            enable_background_retry=True
        )

        assert client._retry_thread is not None
        assert client._retry_thread.is_alive()
        client.close()

    @pytest.mark.unit
    def test_background_retry_thread_stops_on_close(self):
        """Test that background retry thread stops on close."""
        client = CloudSyncClient(
            buffer_path=":memory:",
            enable_background_retry=True
        )

        thread = client._retry_thread
        assert thread.is_alive()

        client.close()
        time.sleep(0.1)

        assert not thread.is_alive()

    @pytest.mark.unit
    def test_background_retry_processes_buffer(self):
        """Test that background retry processes buffered items."""
        client = CloudSyncClient(
            buffer_path=":memory:",
            enable_background_retry=False  # We'll control retry manually
        )

        # Add items to buffer
        client.buffer.add({"test": 1})
        client.buffer.add({"test": 2})

        # Mock successful send
        with patch.object(client, '_send_request', return_value=True):
            client._retry_buffered()

        assert client.buffer.count_pending() == 0
        client.close()

    @pytest.mark.unit
    def test_background_retry_marks_failed(self):
        """Test that failed retries update the record."""
        client = CloudSyncClient(
            buffer_path=":memory:",
            enable_background_retry=False
        )

        client.buffer.add({"test": 1})

        # Mock failed send
        with patch.object(client, '_send_request', return_value=False):
            client._retry_buffered()

        pending = client.buffer.get_pending()
        assert len(pending) == 1
        assert pending[0]['last_error'] == "Retry failed"
        assert pending[0]['retry_count'] == 1
        client.close()


class TestFlushBuffer:
    """Test cases for flush_buffer method."""

    @pytest.fixture
    def client(self):
        """Create a client for testing."""
        c = CloudSyncClient(
            endpoint="http://test.example.com/api",
            buffer_path=":memory:",
            enable_background_retry=False
        )
        yield c
        c.close()

    @pytest.mark.unit
    def test_flush_buffer_empty(self, client):
        """Test flushing empty buffer."""
        sent = client.flush_buffer()
        assert sent == 0

    @pytest.mark.unit
    def test_flush_buffer_all_success(self, client):
        """Test flushing buffer with all successful sends."""
        client.buffer.add({"test": 1})
        client.buffer.add({"test": 2})
        client.buffer.add({"test": 3})

        with patch.object(client, '_send_request', return_value=True):
            sent = client.flush_buffer()

        assert sent == 3
        assert client.buffer.count_pending() == 0

    @pytest.mark.unit
    def test_flush_buffer_partial_success(self, client):
        """Test flushing buffer with some failures."""
        client.buffer.add({"test": 1})
        client.buffer.add({"test": 2})
        client.buffer.add({"test": 3})

        # First and third succeed, second fails
        with patch.object(client, '_send_request', side_effect=[True, False, True]):
            sent = client.flush_buffer()

        assert sent == 2
        assert client.buffer.count_pending() == 1


class TestGetBufferStats:
    """Test cases for get_buffer_stats method."""

    @pytest.fixture
    def client(self):
        """Create a client for testing."""
        c = CloudSyncClient(
            endpoint="http://test.example.com/api",
            buffer_path=":memory:",
            enable_background_retry=False
        )
        yield c
        c.close()

    @pytest.mark.unit
    def test_get_buffer_stats(self, client):
        """Test getting buffer statistics."""
        client.buffer.add({"test": 1})
        client.buffer.add({"test": 2})

        with patch.object(client, '_test_connection', return_value=True):
            stats = client.get_buffer_stats()

        assert stats["pending_count"] == 2
        assert stats["endpoint"] == "http://test.example.com/api"
        assert stats["connected"] is True

    @pytest.mark.unit
    def test_get_buffer_stats_disconnected(self, client):
        """Test buffer stats when disconnected."""
        with patch.object(client, '_test_connection', return_value=False):
            stats = client.get_buffer_stats()

        assert stats["connected"] is False


class TestTestConnection:
    """Test cases for _test_connection method."""

    @pytest.fixture
    def client(self):
        """Create a client for testing."""
        c = CloudSyncClient(
            endpoint="http://test.example.com/api/changes",
            buffer_path=":memory:",
            enable_background_retry=False
        )
        yield c
        c.close()

    @pytest.mark.unit
    @patch('sync.cloud_sync_client.urlopen')
    def test_test_connection_success(self, mock_urlopen, client):
        """Test successful connection test."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = client._test_connection()

        assert result is True
        # Verify health endpoint is called
        call_args = mock_urlopen.call_args
        request = call_args[0][0]
        assert "/health" in request.full_url

    @pytest.mark.unit
    @patch('sync.cloud_sync_client.urlopen')
    def test_test_connection_failure(self, mock_urlopen, client):
        """Test failed connection test."""
        mock_urlopen.side_effect = URLError("Connection refused")

        result = client._test_connection()

        assert result is False


class TestClose:
    """Test cases for close method."""

    @pytest.mark.unit
    def test_close_stops_retry_thread(self):
        """Test that close stops the retry thread."""
        client = CloudSyncClient(
            buffer_path=":memory:",
            enable_background_retry=True
        )

        assert client._retry_thread.is_alive()

        client.close()
        time.sleep(0.1)

        assert not client._retry_thread.is_alive()

    @pytest.mark.unit
    def test_close_closes_buffer(self):
        """Test that close closes the buffer."""
        client = CloudSyncClient(
            buffer_path=":memory:",
            enable_background_retry=False
        )

        with patch.object(client.buffer, 'close') as mock_close:
            client.close()
            mock_close.assert_called_once()

    @pytest.mark.unit
    def test_close_without_retry_thread(self):
        """Test close when retry thread was never started."""
        client = CloudSyncClient(
            buffer_path=":memory:",
            enable_background_retry=False
        )

        # Should not raise
        client.close()
