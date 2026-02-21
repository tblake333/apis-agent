"""
Background probe service for GUI application.

Wraps ProbeApplication with status callbacks for tray icon updates.
"""

import threading
import logging
from enum import Enum
from typing import Callable, Optional
from dataclasses import dataclass, field
from datetime import datetime

from app.probe_application import ProbeApplication
from config.app_config import AppConfig


class ServiceStatus(Enum):
    """Status states for the probe service."""
    STARTING = "starting"
    CONNECTED = "connected"
    SYNCING = "syncing"
    ERROR = "error"
    STOPPED = "stopped"


@dataclass
class ServiceState:
    """Current state of the probe service."""
    status: ServiceStatus = ServiceStatus.STOPPED
    message: str = ""
    last_sync: Optional[datetime] = None
    pending_events: int = 0
    error_count: int = 0
    errors: list = field(default_factory=list)


class ProbeService:
    """
    Background service wrapper for ProbeApplication.

    Provides status callbacks for GUI integration and graceful lifecycle management.
    """

    def __init__(
        self,
        config: AppConfig,
        on_status_change: Optional[Callable[[ServiceState], None]] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the probe service.

        Args:
            config: Application configuration
            on_status_change: Callback for status updates (called on status changes)
            logger: Optional logger instance
        """
        self.config = config
        self.on_status_change = on_status_change
        self.logger = logger or logging.getLogger(__name__)

        self._state = ServiceState()
        self._app: Optional[ProbeApplication] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    @property
    def state(self) -> ServiceState:
        """Get current service state."""
        with self._lock:
            return self._state

    @property
    def is_running(self) -> bool:
        """Check if service is running."""
        return self._thread is not None and self._thread.is_alive()

    def _update_status(
        self,
        status: ServiceStatus,
        message: str = "",
        error: Optional[Exception] = None
    ) -> None:
        """
        Update service status and notify callback.

        Args:
            status: New status
            message: Optional status message
            error: Optional error that occurred
        """
        with self._lock:
            self._state.status = status
            self._state.message = message

            if error:
                self._state.error_count += 1
                self._state.errors.append({
                    'time': datetime.now(),
                    'error': str(error)
                })
                # Keep only last 10 errors
                self._state.errors = self._state.errors[-10:]

            if status == ServiceStatus.SYNCING:
                self._state.last_sync = datetime.now()

        self.logger.info(f"Status: {status.value} - {message}")

        if self.on_status_change:
            try:
                self.on_status_change(self._state)
            except Exception as e:
                self.logger.error(f"Error in status callback: {e}")

    def _update_buffer_stats(self) -> None:
        """Update pending events count from sync client buffer."""
        if self._app and self._app.sync_client:
            try:
                stats = self._app.sync_client.get_buffer_stats()
                with self._lock:
                    self._state.pending_events = stats.get('pending_count', 0)
            except Exception:
                pass

    def _run_service(self) -> None:
        """Main service loop running in background thread."""
        try:
            self._update_status(ServiceStatus.STARTING, "Initializing probe service...")

            # Create application instance
            self._app = ProbeApplication(self.config)

            # Setup phase with status updates
            self._update_status(ServiceStatus.STARTING, "Connecting to database...")
            self._app.setup_database_connection()

            self._update_status(ServiceStatus.STARTING, "Setting up cloud sync...")
            self._app.setup_cloud_sync()

            self._update_status(ServiceStatus.STARTING, "Setting up database schema...")
            id_to_table, table_to_primary_key = self._app.setup_database_schema()

            self._update_status(ServiceStatus.STARTING, "Setting up change monitoring...")
            self._app.setup_change_monitoring(id_to_table, table_to_primary_key)

            # Start processing
            self._update_status(ServiceStatus.STARTING, "Starting change intake...")
            self._app.start_change_intake()
            self._app.start_workers()

            self._update_status(ServiceStatus.CONNECTED, "Probe service running")

            # Main loop - check for stop signal periodically
            while not self._stop_event.is_set():
                # Update buffer stats periodically
                self._update_buffer_stats()

                # Check if intake thread is still alive
                if self._app.changes_intake and not self._app.changes_intake.is_alive():
                    raise RuntimeError("Change intake thread died unexpectedly")

                # Wait for stop signal with timeout
                self._stop_event.wait(timeout=5.0)

        except Exception as e:
            self.logger.exception(f"Service error: {e}")
            self._update_status(ServiceStatus.ERROR, str(e), error=e)

        finally:
            self._shutdown_app()

    def _shutdown_app(self) -> None:
        """Shutdown the application gracefully."""
        if self._app:
            try:
                self._update_status(ServiceStatus.STOPPED, "Shutting down...")
                self._app.shutdown()
            except Exception as e:
                self.logger.error(f"Error during shutdown: {e}")
            finally:
                self._app = None

    def start(self) -> bool:
        """
        Start the probe service in a background thread.

        Returns:
            True if started successfully, False if already running
        """
        if self.is_running:
            self.logger.warning("Service is already running")
            return False

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_service,
            name="ProbeService",
            daemon=True
        )
        self._thread.start()
        return True

    def stop(self, timeout: float = 10.0) -> bool:
        """
        Stop the probe service gracefully.

        Args:
            timeout: Maximum time to wait for shutdown

        Returns:
            True if stopped successfully, False if timeout occurred
        """
        if not self.is_running:
            return True

        self.logger.info("Stopping probe service...")
        self._stop_event.set()

        if self._thread:
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                self.logger.warning("Service did not stop within timeout")
                return False

        self._update_status(ServiceStatus.STOPPED, "Service stopped")
        return True

    def restart(self) -> bool:
        """
        Restart the probe service.

        Returns:
            True if restarted successfully
        """
        self.stop()
        return self.start()

    def get_status_summary(self) -> dict:
        """
        Get a summary of current service status.

        Returns:
            Dictionary with status information
        """
        state = self.state
        return {
            'status': state.status.value,
            'message': state.message,
            'running': self.is_running,
            'last_sync': state.last_sync.isoformat() if state.last_sync else None,
            'pending_events': state.pending_events,
            'error_count': state.error_count,
            'recent_errors': state.errors[-3:] if state.errors else []
        }
