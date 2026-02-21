"""
GUI Authentication wizard for Otter Probe installation.

User-friendly tkinter GUI for store owners.
"""

import os
import sys
import socket
import threading
import webbrowser
from pathlib import Path
from typing import Optional

import tkinter as tk
from tkinter import ttk, messagebox

# Add parent directory to path for imports
if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from auth.device_auth import (
    DeviceAuth,
    DeviceAuthError,
    AuthorizationExpired,
    AuthorizationDenied,
)
from auth.credentials import CredentialsManager, Credentials


# Colors
COLORS = {
    "bg": "#ffffff",
    "primary": "#2563eb",  # Blue
    "primary_hover": "#1d4ed8",
    "success": "#22c55e",  # Green
    "error": "#ef4444",  # Red
    "text": "#1f2937",
    "text_light": "#6b7280",
    "border": "#e5e7eb",
}


class AuthWizardApp:
    """GUI application for device authorization."""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Otter Probe Setup")
        self.root.geometry("480x400")
        self.root.resizable(False, False)
        self.root.configure(bg=COLORS["bg"])

        # Center window on screen
        self.root.update_idletasks()
        x = (self.root.winfo_screenwidth() - 480) // 2
        y = (self.root.winfo_screenheight() - 400) // 2
        self.root.geometry(f"480x400+{x}+{y}")

        # State
        self.device_auth: Optional[DeviceAuth] = None
        self.auth_thread: Optional[threading.Thread] = None
        self.cancelled = False

        # Build UI
        self._build_ui()

    def _build_ui(self):
        """Build the user interface."""
        # Main container with padding
        container = tk.Frame(self.root, bg=COLORS["bg"], padx=40, pady=30)
        container.pack(fill="both", expand=True)

        # Title
        title = tk.Label(
            container,
            text="Otter Probe",
            font=("Segoe UI", 24, "bold"),
            fg=COLORS["primary"],
            bg=COLORS["bg"],
        )
        title.pack(pady=(0, 5))

        # Subtitle
        subtitle = tk.Label(
            container,
            text="Connect your store to Otter",
            font=("Segoe UI", 11),
            fg=COLORS["text_light"],
            bg=COLORS["bg"],
        )
        subtitle.pack(pady=(0, 30))

        # Status frame (changes based on state)
        self.status_frame = tk.Frame(container, bg=COLORS["bg"])
        self.status_frame.pack(fill="both", expand=True)

        # Show initial state
        self._show_start_screen()

        # Button frame at bottom
        button_frame = tk.Frame(container, bg=COLORS["bg"])
        button_frame.pack(fill="x", pady=(20, 0))

        # Cancel button
        self.cancel_btn = tk.Button(
            button_frame,
            text="Cancel",
            font=("Segoe UI", 10),
            fg=COLORS["text_light"],
            bg=COLORS["bg"],
            relief="flat",
            cursor="hand2",
            command=self._on_cancel,
        )
        self.cancel_btn.pack(side="left")

        # Primary action button
        self.action_btn = tk.Button(
            button_frame,
            text="Connect",
            font=("Segoe UI", 11, "bold"),
            fg="white",
            bg=COLORS["primary"],
            activebackground=COLORS["primary_hover"],
            activeforeground="white",
            relief="flat",
            cursor="hand2",
            padx=30,
            pady=8,
            command=self._on_connect,
        )
        self.action_btn.pack(side="right")

    def _clear_status_frame(self):
        """Clear the status frame."""
        for widget in self.status_frame.winfo_children():
            widget.destroy()

    def _show_start_screen(self):
        """Show the initial start screen."""
        self._clear_status_frame()

        # Icon placeholder (could add actual icon)
        icon_label = tk.Label(
            self.status_frame,
            text="🔐",
            font=("Segoe UI", 48),
            bg=COLORS["bg"],
        )
        icon_label.pack(pady=(20, 20))

        # Description
        desc = tk.Label(
            self.status_frame,
            text="Click Connect to authorize this device.\n\nA browser window will open where you can\nlog in to your Otter account.",
            font=("Segoe UI", 11),
            fg=COLORS["text"],
            bg=COLORS["bg"],
            justify="center",
        )
        desc.pack()

    def _show_waiting_screen(self, user_code: str, verification_url: str):
        """Show the waiting for authorization screen."""
        self._clear_status_frame()

        # Instruction
        inst = tk.Label(
            self.status_frame,
            text="Enter this code in your browser:",
            font=("Segoe UI", 11),
            fg=COLORS["text_light"],
            bg=COLORS["bg"],
        )
        inst.pack(pady=(10, 15))

        # Code display box
        code_frame = tk.Frame(
            self.status_frame,
            bg=COLORS["border"],
            padx=2,
            pady=2,
        )
        code_frame.pack()

        code_inner = tk.Frame(code_frame, bg="#f8fafc", padx=30, pady=15)
        code_inner.pack()

        code_label = tk.Label(
            code_inner,
            text=user_code,
            font=("Consolas", 28, "bold"),
            fg=COLORS["primary"],
            bg="#f8fafc",
        )
        code_label.pack()

        # URL
        url_label = tk.Label(
            self.status_frame,
            text=verification_url,
            font=("Segoe UI", 10),
            fg=COLORS["text_light"],
            bg=COLORS["bg"],
            cursor="hand2",
        )
        url_label.pack(pady=(20, 15))
        url_label.bind("<Button-1>", lambda e: webbrowser.open(verification_url))

        # Waiting indicator
        self.waiting_label = tk.Label(
            self.status_frame,
            text="⏳ Waiting for authorization...",
            font=("Segoe UI", 10),
            fg=COLORS["text_light"],
            bg=COLORS["bg"],
        )
        self.waiting_label.pack(pady=(10, 0))

        # Update button
        self.action_btn.configure(text="Open Browser", command=lambda: webbrowser.open(verification_url))

    def _show_success_screen(self, store_id: str):
        """Show success screen."""
        self._clear_status_frame()

        # Success icon
        icon_label = tk.Label(
            self.status_frame,
            text="✓",
            font=("Segoe UI", 64, "bold"),
            fg=COLORS["success"],
            bg=COLORS["bg"],
        )
        icon_label.pack(pady=(20, 20))

        # Success message
        msg = tk.Label(
            self.status_frame,
            text="Successfully Connected!",
            font=("Segoe UI", 16, "bold"),
            fg=COLORS["text"],
            bg=COLORS["bg"],
        )
        msg.pack(pady=(0, 10))

        # Store ID
        store_label = tk.Label(
            self.status_frame,
            text=f"Store ID: {store_id}",
            font=("Segoe UI", 11),
            fg=COLORS["text_light"],
            bg=COLORS["bg"],
        )
        store_label.pack()

        # Update buttons
        self.cancel_btn.pack_forget()
        self.action_btn.configure(text="Finish", command=self._on_finish)

    def _show_error_screen(self, error_message: str):
        """Show error screen."""
        self._clear_status_frame()

        # Error icon
        icon_label = tk.Label(
            self.status_frame,
            text="✕",
            font=("Segoe UI", 64, "bold"),
            fg=COLORS["error"],
            bg=COLORS["bg"],
        )
        icon_label.pack(pady=(20, 20))

        # Error message
        msg = tk.Label(
            self.status_frame,
            text="Connection Failed",
            font=("Segoe UI", 16, "bold"),
            fg=COLORS["text"],
            bg=COLORS["bg"],
        )
        msg.pack(pady=(0, 10))

        # Details
        details = tk.Label(
            self.status_frame,
            text=error_message,
            font=("Segoe UI", 10),
            fg=COLORS["text_light"],
            bg=COLORS["bg"],
            wraplength=350,
        )
        details.pack()

        # Update button
        self.action_btn.configure(text="Try Again", command=self._on_connect)

    def _on_connect(self):
        """Handle connect button click."""
        self.cancelled = False
        self.action_btn.configure(state="disabled", text="Connecting...")

        # Start auth in background thread
        self.auth_thread = threading.Thread(target=self._run_auth_flow, daemon=True)
        self.auth_thread.start()

    def _run_auth_flow(self):
        """Run the authentication flow in background thread."""
        try:
            api_url = os.environ.get("OTTER_API_URL", "https://api.otter.com")
            device_name = socket.gethostname()

            creds_manager = CredentialsManager()
            self.device_auth = DeviceAuth(api_url, creds_manager)

            # Request device code
            device_response = self.device_auth.request_device_code(device_name)

            if self.cancelled:
                return

            # Update UI to show code
            self.root.after(0, lambda: self._show_waiting_screen(
                device_response.user_code,
                device_response.verification_url
            ))
            self.root.after(0, lambda: self.action_btn.configure(state="normal"))

            # Open browser
            webbrowser.open(device_response.verification_url)

            # Poll for token
            token_response = self.device_auth.poll_for_token(
                device_code=device_response.device_code,
                interval=device_response.interval,
                timeout=device_response.expires_in,
            )

            if self.cancelled:
                return

            # Save credentials
            credentials = Credentials(
                store_id=token_response.store_id,
                api_key=token_response.api_key,
                device_id=token_response.device_id,
                device_name=device_name,
            )
            creds_manager.save(credentials)

            # Show success
            self.root.after(0, lambda: self._show_success_screen(token_response.store_id))

        except AuthorizationExpired:
            self.root.after(0, lambda: self._show_error_screen(
                "The authorization code expired.\nPlease try again."
            ))
        except AuthorizationDenied:
            self.root.after(0, lambda: self._show_error_screen(
                "Authorization was denied.\nPlease try again and approve the request."
            ))
        except DeviceAuthError as e:
            self.root.after(0, lambda: self._show_error_screen(str(e)))
        except Exception as e:
            self.root.after(0, lambda: self._show_error_screen(f"Unexpected error: {e}"))

    def _on_cancel(self):
        """Handle cancel button click."""
        self.cancelled = True
        self.root.quit()
        self.root.destroy()

    def _on_finish(self):
        """Handle finish button click."""
        self.root.quit()
        self.root.destroy()

    def run(self) -> int:
        """Run the application and return exit code."""
        try:
            self.root.mainloop()
            # Check if credentials exist (success)
            if CredentialsManager().exists():
                return 0
            return 1
        except Exception:
            return 1


def main():
    """Main entry point."""
    app = AuthWizardApp()
    exit_code = app.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
