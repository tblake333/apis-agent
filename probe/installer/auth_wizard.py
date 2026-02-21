"""
Authentication wizard for Otter Probe installation.

Standalone script that runs during installation to authenticate
the probe with the Otter cloud service.

This wizard:
1. Opens browser for OAuth authorization
2. Shows waiting message with user code
3. Polls until authorization completes
4. Saves credentials
5. Exits with success/failure code for installer
"""

import os
import sys
import time
import socket
from typing import Optional

# Add parent directory to path for imports when running standalone
if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from auth.device_auth import DeviceAuth, DeviceAuthError, AuthorizationExpired, AuthorizationDenied
from auth.credentials import CredentialsManager


# Exit codes for installer
EXIT_SUCCESS = 0
EXIT_AUTH_CANCELLED = 1
EXIT_AUTH_EXPIRED = 2
EXIT_AUTH_DENIED = 3
EXIT_AUTH_ERROR = 4
EXIT_ALREADY_AUTHENTICATED = 5


def get_api_base_url() -> str:
    """Get the API base URL from environment or default."""
    return os.environ.get("OTTER_API_URL", "https://api.otter.com")


def get_device_name() -> str:
    """Get a device name for this machine."""
    try:
        return socket.gethostname()
    except Exception:
        return "probe-device"


def print_header(title: str) -> None:
    """Print a formatted header."""
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)
    print()


def print_box(lines: list[str]) -> None:
    """Print lines in a box format."""
    width = max(len(line) for line in lines) + 4
    print("+" + "-" * width + "+")
    for line in lines:
        print(f"|  {line.ljust(width - 2)}|")
    print("+" + "-" * width + "+")


def run_auth_wizard(force: bool = False) -> int:
    """
    Run the authentication wizard.

    Args:
        force: If True, re-authenticate even if credentials exist

    Returns:
        Exit code (0 for success)
    """
    print_header("OTTER PROBE AUTHORIZATION")

    creds_manager = CredentialsManager()
    api_url = get_api_base_url()
    device_name = get_device_name()

    # Check for existing credentials
    if creds_manager.exists() and not force:
        try:
            creds = creds_manager.load()
            print(f"  Device is already authorized.")
            print(f"  Store ID: {creds.store_id}")
            print()
            print("  Use --force to re-authorize.")
            print()
            return EXIT_ALREADY_AUTHENTICATED
        except Exception:
            pass  # Corrupted credentials, proceed with auth

    print(f"  Connecting to: {api_url}")
    print(f"  Device name: {device_name}")
    print()

    # Initialize auth
    device_auth = DeviceAuth(
        api_base_url=api_url,
        credentials_manager=creds_manager
    )

    try:
        # Request device code
        print("Requesting authorization code...")
        device_response = device_auth.request_device_code(device_name)

        # Display user code prominently
        print()
        print_box([
            "AUTHORIZATION CODE",
            "",
            f"    {device_response.user_code}",
            "",
            "Enter this code in your browser",
        ])
        print()
        print(f"  URL: {device_response.verification_url}")
        print()

        # Open browser
        if device_auth.open_browser(device_response.verification_url):
            print("  Browser opened automatically.")
        else:
            print("  Please open the URL above in your browser.")

        print()
        print("  Waiting for authorization...")
        print("  (This window will close automatically when done)")
        print()

        # Show progress dots while polling
        start_time = time.time()
        timeout = device_response.expires_in

        # Poll with visual feedback
        def poll_with_progress():
            last_dot = 0
            while True:
                elapsed = time.time() - start_time
                remaining = int(timeout - elapsed)

                # Print progress dots every interval
                if int(elapsed) // device_response.interval > last_dot:
                    last_dot = int(elapsed) // device_response.interval
                    remaining_min = remaining // 60
                    remaining_sec = remaining % 60
                    sys.stdout.write(f"\r  Waiting... ({remaining_min}:{remaining_sec:02d} remaining)  ")
                    sys.stdout.flush()

                time.sleep(0.5)

        # Start progress in background would be ideal, but for simplicity
        # we'll rely on the device_auth polling

        token_response = device_auth.poll_for_token(
            device_code=device_response.device_code,
            interval=device_response.interval,
            timeout=timeout
        )

        # Save credentials
        from auth.credentials import Credentials
        credentials = Credentials(
            store_id=token_response.store_id,
            api_key=token_response.api_key,
            device_id=token_response.device_id,
            device_name=device_name,
        )
        creds_manager.save(credentials)

        # Success!
        print()
        print_header("AUTHORIZATION SUCCESSFUL")
        print(f"  Store ID: {credentials.store_id}")
        print(f"  Credentials saved to: {creds_manager.credentials_path}")
        print()
        print("  You can close this window.")
        print()

        # Give user time to see the message
        time.sleep(3)

        return EXIT_SUCCESS

    except KeyboardInterrupt:
        print()
        print("  Authorization cancelled by user.")
        return EXIT_AUTH_CANCELLED

    except AuthorizationExpired as e:
        print()
        print(f"  Authorization expired: {e}")
        print("  Please run the wizard again.")
        return EXIT_AUTH_EXPIRED

    except AuthorizationDenied as e:
        print()
        print(f"  Authorization denied: {e}")
        return EXIT_AUTH_DENIED

    except DeviceAuthError as e:
        print()
        print(f"  Authorization error: {e}")
        return EXIT_AUTH_ERROR

    except Exception as e:
        print()
        print(f"  Unexpected error: {e}")
        return EXIT_AUTH_ERROR


def main():
    """Main entry point for auth wizard."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Otter Probe Authorization Wizard",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-authorize even if credentials exist"
    )
    parser.add_argument(
        "--api-url",
        help="Override API base URL"
    )

    args = parser.parse_args()

    if args.api_url:
        os.environ["OTTER_API_URL"] = args.api_url

    exit_code = run_auth_wizard(force=args.force)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
