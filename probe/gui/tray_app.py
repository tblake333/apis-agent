"""
System tray application for Otter Probe.

Cross-platform support for Windows and macOS with:
- Status icons (green/yellow/red)
- Right-click context menu
- Background probe service management
"""

import os
import subprocess
import sys
import logging
from pathlib import Path
from typing import Optional

import pystray
from pystray import MenuItem, Menu

from gui.icons import Icons
from app.probe_service import ProbeService, ServiceStatus, ServiceState
from config.app_config import AppConfig
from auth.credentials import CredentialsManager


# Platform detection
IS_WINDOWS = sys.platform == "win32"
IS_MACOS = sys.platform == "darwin"


def setup_logging() -> logging.Logger:
    """Setup logging to file in user's home directory."""
    log_dir = Path.home() / ".otter" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "probe.log"

    # Create handlers
    handlers = [logging.FileHandler(log_file)]

    # Only add stdout handler if not running as a frozen app (to avoid console issues)
    if not getattr(sys, 'frozen', False):
        handlers.append(logging.StreamHandler(sys.stdout))

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

    return logging.getLogger("OtterProbe")


def open_file_with_default_app(file_path: Path) -> bool:
    """
    Open a file with the system's default application.

    Args:
        file_path: Path to file to open

    Returns:
        True if successful, False otherwise
    """
    try:
        if IS_WINDOWS:
            os.startfile(str(file_path))
        elif IS_MACOS:
            subprocess.run(["open", str(file_path)], check=True)
        else:
            subprocess.run(["xdg-open", str(file_path)], check=True)
        return True
    except Exception:
        return False


def open_folder(folder_path: Path) -> bool:
    """
    Open a folder in the system file browser.

    Args:
        folder_path: Path to folder to open

    Returns:
        True if successful, False otherwise
    """
    try:
        if IS_WINDOWS:
            os.startfile(str(folder_path))
        elif IS_MACOS:
            subprocess.run(["open", str(folder_path)], check=True)
        else:
            subprocess.run(["xdg-open", str(folder_path)], check=True)
        return True
    except Exception:
        return False


class TrayApp:
    """
    System tray application for Otter Probe.

    Manages the tray icon, context menu, and background probe service.
    Works on both Windows and macOS.
    """

    APP_NAME = "Otter Probe"
    APP_VERSION = "1.0.0"

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize the tray application.

        Args:
            logger: Optional logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
        self._icon: Optional[pystray.Icon] = None
        self._service: Optional[ProbeService] = None
        self._current_status = ServiceStatus.STOPPED
        self._status_message = "Not started"
        self._pending_events = 0

    def _get_icon_for_status(self, status: ServiceStatus):
        """Get the appropriate icon for the given status."""
        if status == ServiceStatus.CONNECTED:
            return Icons.connected()
        elif status in (ServiceStatus.STARTING, ServiceStatus.SYNCING):
            return Icons.syncing()
        else:
            return Icons.error()

    def _on_status_change(self, state: ServiceState) -> None:
        """
        Handle status changes from the probe service.

        Args:
            state: Current service state
        """
        self._current_status = state.status
        self._status_message = state.message
        self._pending_events = state.pending_events

        # Update tray icon
        if self._icon:
            self._icon.icon = self._get_icon_for_status(state.status)

            # Build tooltip with status info
            tooltip = f"{self.APP_NAME} - {state.status.value.capitalize()}"
            if state.pending_events > 0:
                tooltip += f" ({state.pending_events} pending)"
            self._icon.title = tooltip

    def _get_status_text(self, item) -> str:
        """Get current status text for menu."""
        return f"Status: {self._current_status.value.capitalize()}"

    def _get_message_text(self, item) -> str:
        """Get current message text for menu."""
        if self._status_message:
            return f"  {self._status_message}"
        return ""

    def _get_pending_text(self, item) -> str:
        """Get pending events text for menu."""
        if self._pending_events > 0:
            return f"  Pending events: {self._pending_events}"
        return ""

    def _has_message(self, item) -> bool:
        """Check if there's a status message to display."""
        return bool(self._status_message)

    def _has_pending(self, item) -> bool:
        """Check if there are pending events."""
        return self._pending_events > 0

    def _create_menu(self) -> Menu:
        """Create the right-click context menu."""
        menu_items = [
            # Status section
            MenuItem(self._get_status_text, None, enabled=False),
            MenuItem(
                self._get_message_text,
                None,
                enabled=False,
                visible=self._has_message
            ),
            MenuItem(
                self._get_pending_text,
                None,
                enabled=False,
                visible=self._has_pending
            ),
            Menu.SEPARATOR,

            # Actions
            MenuItem("View Logs", self._on_view_logs),
            MenuItem("Open Logs Folder", self._on_open_logs_folder),
            MenuItem("Reconnect", self._on_reconnect),
            Menu.SEPARATOR,

            # About & Exit
            MenuItem(f"About {self.APP_NAME}", self._on_about),
            MenuItem("Quit" if IS_MACOS else "Exit", self._on_exit),
        ]

        return Menu(*menu_items)

    def _on_view_logs(self, icon, item) -> None:
        """Open log file in default viewer."""
        log_file = Path.home() / ".otter" / "logs" / "probe.log"
        if log_file.exists():
            if not open_file_with_default_app(log_file):
                self.logger.warning(f"Could not open log file: {log_file}")
        else:
            self.logger.warning(f"Log file not found: {log_file}")

    def _on_open_logs_folder(self, icon, item) -> None:
        """Open logs folder in file browser."""
        log_dir = Path.home() / ".otter" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        if not open_folder(log_dir):
            self.logger.warning(f"Could not open logs folder: {log_dir}")

    def _on_reconnect(self, icon, item) -> None:
        """Restart the probe service."""
        self.logger.info("Reconnect requested")
        self._status_message = "Reconnecting..."
        if self._icon:
            self._icon.icon = Icons.syncing()

        if self._service:
            self._service.restart()

    def _on_about(self, icon, item) -> None:
        """Show about information."""
        self.logger.info(f"About: {self.APP_NAME} v{self.APP_VERSION}")
        # On macOS, we could show a native dialog, but for simplicity just log it
        # A future enhancement could use tkinter or native dialogs

    def _on_exit(self, icon, item) -> None:
        """Exit the application."""
        self.logger.info("Exit requested")
        self.stop()

    def _load_config(self) -> Optional[AppConfig]:
        """
        Load application configuration.

        Returns:
            AppConfig if successful, None if credentials not found
        """
        # Check for credentials
        creds_manager = CredentialsManager()
        if not creds_manager.exists():
            self.logger.error("No credentials found. Please run the auth wizard first.")
            self._status_message = "No credentials - run auth wizard"
            return None

        try:
            config = AppConfig.from_env_with_credentials()
            return config
        except Exception as e:
            self.logger.error(f"Failed to load config: {e}")
            self._status_message = f"Config error: {e}"
            return None

    def _start_service(self) -> None:
        """Initialize and start the probe service."""
        config = self._load_config()
        if not config:
            self._current_status = ServiceStatus.ERROR
            if self._icon:
                self._icon.icon = Icons.error()
            return

        self._service = ProbeService(
            config=config,
            on_status_change=self._on_status_change,
            logger=self.logger
        )

        if not self._service.start():
            self.logger.error("Failed to start probe service")
            self._current_status = ServiceStatus.ERROR

    def run(self) -> None:
        """
        Run the tray application.

        This is the main entry point that creates the tray icon and starts
        the probe service in the background.
        """
        self.logger.info(f"Starting {self.APP_NAME} on {sys.platform}")

        # Create tray icon with initial status
        self._icon = pystray.Icon(
            name="otter_probe",
            icon=Icons.syncing(),  # Start with yellow (starting)
            title=f"{self.APP_NAME} - Starting...",
            menu=self._create_menu()
        )

        # Start service after icon is set up
        def setup_and_run():
            self._start_service()

        # Run icon (this blocks until exit)
        self._icon.run(setup=setup_and_run)

    def stop(self) -> None:
        """Stop the tray application and service."""
        self.logger.info("Stopping application")

        if self._service:
            self._service.stop()
            self._service = None

        if self._icon:
            self._icon.stop()
            self._icon = None


def main():
    """Main entry point for the tray application."""
    logger = setup_logging()
    logger.info("=" * 50)
    logger.info(f"Otter Probe starting on {sys.platform}...")

    app = TrayApp(logger=logger)

    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        raise
    finally:
        app.stop()
        logger.info("Otter Probe stopped")


if __name__ == "__main__":
    main()
