"""
Cross-platform autostart handler for Otter Probe.

Supports:
- Windows: Registry Run key (HKCU\Software\Microsoft\Windows\CurrentVersion\Run)
- macOS: Launch Agents (~/Library/LaunchAgents/com.otter.probe.plist)
"""

import os
import plistlib
import sys
from pathlib import Path
from typing import Optional


APP_NAME = "OtterProbe"
APP_IDENTIFIER = "com.otter.probe"

# Windows Registry path
WINDOWS_REGISTRY_PATH = r"Software\Microsoft\Windows\CurrentVersion\Run"

# macOS Launch Agents directory
MACOS_LAUNCH_AGENTS_DIR = Path.home() / "Library" / "LaunchAgents"
MACOS_PLIST_PATH = MACOS_LAUNCH_AGENTS_DIR / f"{APP_IDENTIFIER}.plist"


def is_windows() -> bool:
    """Check if running on Windows."""
    return sys.platform == "win32"


def is_macos() -> bool:
    """Check if running on macOS."""
    return sys.platform == "darwin"


def get_executable_path() -> str:
    """
    Get the path to the probe executable.

    Returns the path to either:
    - The PyInstaller executable (if frozen)
    - The Python script (if running from source)
    """
    if getattr(sys, 'frozen', False):
        # Running as PyInstaller bundle
        if is_macos():
            # For .app bundles, get the actual executable inside
            exe_path = sys.executable
            # If inside an .app bundle, return the .app path for launchctl
            if '.app/Contents/MacOS/' in exe_path:
                app_path = exe_path.split('.app/Contents/MacOS/')[0] + '.app'
                return app_path
            return exe_path
        return sys.executable
    else:
        # Running from source
        probe_dir = Path(__file__).parent.parent
        return str(probe_dir / "main_tray.py")


# =============================================================================
# Windows Implementation
# =============================================================================

def _get_winreg():
    """Import and return winreg module (Windows only)."""
    if not is_windows():
        raise OSError("Windows registry is only available on Windows")
    import winreg
    return winreg


def _windows_add_autostart(executable_path: Optional[str] = None) -> bool:
    """Add to Windows autostart via Registry."""
    winreg = _get_winreg()
    exe_path = executable_path or get_executable_path()

    # For Python scripts, include the interpreter
    if exe_path.endswith('.py'):
        exe_path = f'"{sys.executable}" "{exe_path}"'
    else:
        exe_path = f'"{exe_path}"'

    try:
        key = winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            WINDOWS_REGISTRY_PATH,
            0,
            winreg.KEY_SET_VALUE
        )
        try:
            winreg.SetValueEx(key, APP_NAME, 0, winreg.REG_SZ, exe_path)
            print(f"Added to Windows autostart: {APP_NAME}")
            print(f"Executable: {exe_path}")
            return True
        finally:
            winreg.CloseKey(key)
    except PermissionError:
        print("Permission denied. Run as administrator.")
        return False
    except Exception as e:
        print(f"Failed to add to autostart: {e}")
        return False


def _windows_remove_autostart() -> bool:
    """Remove from Windows autostart."""
    winreg = _get_winreg()

    try:
        key = winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            WINDOWS_REGISTRY_PATH,
            0,
            winreg.KEY_SET_VALUE
        )
        try:
            winreg.DeleteValue(key, APP_NAME)
            print(f"Removed from Windows autostart: {APP_NAME}")
            return True
        except FileNotFoundError:
            print(f"Not in autostart: {APP_NAME}")
            return True
        finally:
            winreg.CloseKey(key)
    except PermissionError:
        print("Permission denied. Run as administrator.")
        return False
    except Exception as e:
        print(f"Failed to remove from autostart: {e}")
        return False


def _windows_is_in_autostart() -> bool:
    """Check if in Windows autostart."""
    winreg = _get_winreg()

    try:
        key = winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            WINDOWS_REGISTRY_PATH,
            0,
            winreg.KEY_READ
        )
        try:
            value, _ = winreg.QueryValueEx(key, APP_NAME)
            return bool(value)
        except FileNotFoundError:
            return False
        finally:
            winreg.CloseKey(key)
    except Exception:
        return False


def _windows_get_autostart_path() -> Optional[str]:
    """Get Windows autostart executable path."""
    winreg = _get_winreg()

    try:
        key = winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            WINDOWS_REGISTRY_PATH,
            0,
            winreg.KEY_READ
        )
        try:
            value, _ = winreg.QueryValueEx(key, APP_NAME)
            return value
        except FileNotFoundError:
            return None
        finally:
            winreg.CloseKey(key)
    except Exception:
        return None


# =============================================================================
# macOS Implementation
# =============================================================================

def _macos_create_plist(executable_path: str) -> dict:
    """
    Create a Launch Agent plist dictionary.

    Args:
        executable_path: Path to the executable or .app bundle

    Returns:
        Dictionary suitable for plistlib
    """
    # Determine program arguments
    if executable_path.endswith('.py'):
        # Running from source - use Python interpreter
        program_args = [sys.executable, executable_path]
    elif executable_path.endswith('.app'):
        # .app bundle - use open command
        program_args = ["/usr/bin/open", "-a", executable_path, "--args", "--background"]
    else:
        # Direct executable
        program_args = [executable_path]

    return {
        "Label": APP_IDENTIFIER,
        "ProgramArguments": program_args,
        "RunAtLoad": True,
        "KeepAlive": {
            "SuccessfulExit": False,  # Restart if it crashes
        },
        "StandardOutPath": str(Path.home() / ".otter" / "logs" / "probe-stdout.log"),
        "StandardErrorPath": str(Path.home() / ".otter" / "logs" / "probe-stderr.log"),
        "ProcessType": "Interactive",
        "LegacyTimers": True,
    }


def _macos_add_autostart(executable_path: Optional[str] = None) -> bool:
    """Add to macOS autostart via Launch Agent."""
    exe_path = executable_path or get_executable_path()

    try:
        # Ensure LaunchAgents directory exists
        MACOS_LAUNCH_AGENTS_DIR.mkdir(parents=True, exist_ok=True)

        # Ensure log directory exists
        log_dir = Path.home() / ".otter" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        # Create plist
        plist_data = _macos_create_plist(exe_path)

        # Write plist file
        with open(MACOS_PLIST_PATH, 'wb') as f:
            plistlib.dump(plist_data, f)

        print(f"Created Launch Agent: {MACOS_PLIST_PATH}")
        print(f"Executable: {exe_path}")

        # Load the Launch Agent
        result = os.system(f'launchctl load "{MACOS_PLIST_PATH}"')
        if result == 0:
            print("Launch Agent loaded successfully")
            return True
        else:
            print("Warning: Launch Agent created but not loaded (may require logout/login)")
            return True

    except PermissionError:
        print("Permission denied.")
        return False
    except Exception as e:
        print(f"Failed to add to autostart: {e}")
        return False


def _macos_remove_autostart() -> bool:
    """Remove from macOS autostart."""
    try:
        if MACOS_PLIST_PATH.exists():
            # Unload the Launch Agent first
            os.system(f'launchctl unload "{MACOS_PLIST_PATH}" 2>/dev/null')

            # Remove the plist file
            MACOS_PLIST_PATH.unlink()
            print(f"Removed Launch Agent: {MACOS_PLIST_PATH}")
        else:
            print(f"Not in autostart: {APP_NAME}")

        return True

    except PermissionError:
        print("Permission denied.")
        return False
    except Exception as e:
        print(f"Failed to remove from autostart: {e}")
        return False


def _macos_is_in_autostart() -> bool:
    """Check if in macOS autostart."""
    return MACOS_PLIST_PATH.exists()


def _macos_get_autostart_path() -> Optional[str]:
    """Get macOS autostart executable path from plist."""
    if not MACOS_PLIST_PATH.exists():
        return None

    try:
        with open(MACOS_PLIST_PATH, 'rb') as f:
            plist_data = plistlib.load(f)

        program_args = plist_data.get("ProgramArguments", [])
        if program_args:
            # Return the actual executable (skip 'open -a' wrapper if present)
            if program_args[0] == "/usr/bin/open" and "-a" in program_args:
                idx = program_args.index("-a") + 1
                if idx < len(program_args):
                    return program_args[idx]
            return program_args[-1]

        return None

    except Exception:
        return None


# =============================================================================
# Cross-Platform API
# =============================================================================

def add_to_autostart(executable_path: Optional[str] = None) -> bool:
    """
    Add Otter Probe to system autostart.

    Args:
        executable_path: Path to executable. Defaults to current executable.

    Returns:
        True if successful, False otherwise.
    """
    if is_windows():
        return _windows_add_autostart(executable_path)
    elif is_macos():
        return _macos_add_autostart(executable_path)
    else:
        print(f"Autostart not supported on {sys.platform}")
        return False


def remove_from_autostart() -> bool:
    """
    Remove Otter Probe from system autostart.

    Returns:
        True if successful (or already removed), False on error.
    """
    if is_windows():
        return _windows_remove_autostart()
    elif is_macos():
        return _macos_remove_autostart()
    else:
        print(f"Autostart not supported on {sys.platform}")
        return False


def is_in_autostart() -> bool:
    """
    Check if Otter Probe is configured for autostart.

    Returns:
        True if in autostart, False otherwise.
    """
    if is_windows():
        return _windows_is_in_autostart()
    elif is_macos():
        return _macos_is_in_autostart()
    else:
        return False


def get_autostart_path() -> Optional[str]:
    """
    Get the current autostart executable path.

    Returns:
        Executable path if in autostart, None otherwise.
    """
    if is_windows():
        return _windows_get_autostart_path()
    elif is_macos():
        return _macos_get_autostart_path()
    else:
        return None


def main():
    """Command-line interface for autostart management."""
    import argparse

    platform_name = "Windows" if is_windows() else "macOS" if is_macos() else sys.platform

    parser = argparse.ArgumentParser(
        description=f"Manage Otter Probe autostart ({platform_name})",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    subparsers = parser.add_subparsers(dest="command", help="Command")

    # Add command
    add_parser = subparsers.add_parser("add", help="Add to autostart")
    add_parser.add_argument("--path", help="Custom executable path")

    # Remove command
    subparsers.add_parser("remove", help="Remove from autostart")

    # Status command
    subparsers.add_parser("status", help="Check autostart status")

    args = parser.parse_args()

    if args.command == "add":
        success = add_to_autostart(args.path if hasattr(args, 'path') else None)
        sys.exit(0 if success else 1)

    elif args.command == "remove":
        success = remove_from_autostart()
        sys.exit(0 if success else 1)

    elif args.command == "status":
        if is_in_autostart():
            path = get_autostart_path()
            print(f"Autostart enabled ({platform_name}): {path}")
        else:
            print(f"Autostart not configured ({platform_name})")
        sys.exit(0)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
