#!/usr/bin/env python3
"""
macOS build script for Otter Probe.

Creates:
1. OtterProbe.app bundle (tray application)
2. OtterProbeAuth (CLI auth wizard)
3. OtterProbe.dmg installer

Usage:
    python installer/build_macos.py

Requirements:
    - PyInstaller
    - create-dmg (brew install create-dmg) - optional for DMG creation
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path


# Configuration
APP_NAME = "Otter Probe"
APP_VERSION = "1.0.0"
BUNDLE_ID = "com.otter.probe"

# Paths
SCRIPT_DIR = Path(__file__).parent
PROBE_DIR = SCRIPT_DIR.parent
DIST_DIR = PROBE_DIR / "dist"
BUILD_DIR = PROBE_DIR / "build"
DMG_DIR = DIST_DIR / "dmg_contents"


def run_command(cmd: list, cwd: Path = None) -> bool:
    """Run a command and return success status."""
    print(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, cwd=cwd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code {e.returncode}")
        return False


def clean_build():
    """Clean previous build artifacts."""
    print("\n=== Cleaning previous builds ===")

    for dir_path in [DIST_DIR, BUILD_DIR, DMG_DIR]:
        if dir_path.exists():
            print(f"Removing {dir_path}")
            shutil.rmtree(dir_path)


def build_tray_app() -> bool:
    """Build the tray application."""
    print("\n=== Building OtterProbe.app ===")

    return run_command(
        ["pyinstaller", "--clean", "--noconfirm", "probe_tray.spec"],
        cwd=PROBE_DIR
    )


def build_auth_wizard() -> bool:
    """Build the auth wizard."""
    print("\n=== Building OtterProbeAuth ===")

    return run_command(
        ["pyinstaller", "--clean", "--noconfirm", "probe_auth.spec"],
        cwd=PROBE_DIR
    )


def create_dmg_contents():
    """Prepare DMG contents directory."""
    print("\n=== Preparing DMG contents ===")

    DMG_DIR.mkdir(parents=True, exist_ok=True)

    # Copy .app bundle
    app_src = DIST_DIR / "OtterProbe.app"
    app_dst = DMG_DIR / "OtterProbe.app"
    if app_src.exists():
        print(f"Copying {app_src} -> {app_dst}")
        shutil.copytree(app_src, app_dst)
    else:
        print(f"Error: {app_src} not found")
        return False

    # Copy auth wizard into Resources folder of app bundle
    auth_src = DIST_DIR / "OtterProbeAuth"
    if auth_src.exists():
        resources_dir = app_dst / "Contents" / "Resources"
        resources_dir.mkdir(parents=True, exist_ok=True)
        auth_dst = resources_dir / "OtterProbeAuth"
        print(f"Copying {auth_src} -> {auth_dst}")
        shutil.copy2(auth_src, auth_dst)
        os.chmod(auth_dst, 0o755)

    # Create Applications symlink
    apps_link = DMG_DIR / "Applications"
    if not apps_link.exists():
        print("Creating Applications symlink")
        apps_link.symlink_to("/Applications")

    # Create README
    readme_path = DMG_DIR / "README.txt"
    with open(readme_path, 'w') as f:
        f.write(f"""{APP_NAME} v{APP_VERSION}
========================

Installation:
1. Drag OtterProbe.app to Applications folder
2. Open OtterProbe from Applications
3. Follow the authorization prompts in your browser
4. The app will run in your menu bar

First Launch:
- macOS may show a security warning
- Go to System Preferences > Security & Privacy
- Click "Open Anyway" to allow the app

The app will:
- Display a status icon in your menu bar
- Sync your point-of-sale data with Otter cloud
- Start automatically when you log in (optional)

Support: https://otter.com/support
""")

    return True


def create_dmg() -> bool:
    """Create DMG installer using create-dmg or hdiutil."""
    print("\n=== Creating DMG installer ===")

    dmg_path = DIST_DIR / f"OtterProbe-{APP_VERSION}.dmg"

    # Remove existing DMG
    if dmg_path.exists():
        dmg_path.unlink()

    # Try create-dmg first (prettier results)
    if shutil.which("create-dmg"):
        return run_command([
            "create-dmg",
            "--volname", APP_NAME,
            "--volicon", str(PROBE_DIR / "assets" / "icon.icns") if (PROBE_DIR / "assets" / "icon.icns").exists() else "",
            "--window-pos", "200", "120",
            "--window-size", "600", "400",
            "--icon-size", "100",
            "--icon", "OtterProbe.app", "150", "190",
            "--app-drop-link", "450", "190",
            "--hide-extension", "OtterProbe.app",
            str(dmg_path),
            str(DMG_DIR)
        ])
    else:
        # Fallback to hdiutil
        print("create-dmg not found, using hdiutil (install with: brew install create-dmg)")
        return run_command([
            "hdiutil", "create",
            "-volname", APP_NAME,
            "-srcfolder", str(DMG_DIR),
            "-ov",
            "-format", "UDZO",
            str(dmg_path)
        ])


def create_install_script():
    """Create a post-install script for the app."""
    print("\n=== Creating install helper script ===")

    script_path = DIST_DIR / "install.sh"
    with open(script_path, 'w') as f:
        f.write("""#!/bin/bash
# Otter Probe Install Helper

APP_PATH="/Applications/OtterProbe.app"
AUTH_PATH="$APP_PATH/Contents/Resources/OtterProbeAuth"

echo "Installing Otter Probe..."

# Run auth wizard
if [ -f "$AUTH_PATH" ]; then
    echo "Starting authorization..."
    "$AUTH_PATH"
    AUTH_RESULT=$?

    if [ $AUTH_RESULT -eq 0 ]; then
        echo "Authorization successful!"
    else
        echo "Authorization cancelled or failed."
        exit 1
    fi
fi

# Add to login items (optional - can be done from app)
# osascript -e 'tell application "System Events" to make login item at end with properties {path:"/Applications/OtterProbe.app", hidden:true}'

# Start the app
echo "Starting Otter Probe..."
open "$APP_PATH"

echo "Installation complete!"
echo "Look for the Otter Probe icon in your menu bar."
""")
    os.chmod(script_path, 0o755)


def main():
    """Main build process."""
    print(f"Building {APP_NAME} v{APP_VERSION} for macOS")
    print("=" * 50)

    if sys.platform != "darwin":
        print("Error: This script must be run on macOS")
        sys.exit(1)

    # Check for PyInstaller
    if not shutil.which("pyinstaller"):
        print("Error: PyInstaller not found. Install with: pip install pyinstaller")
        sys.exit(1)

    # Clean previous builds
    clean_build()

    # Build executables
    if not build_tray_app():
        print("Failed to build tray app")
        sys.exit(1)

    if not build_auth_wizard():
        print("Failed to build auth wizard")
        sys.exit(1)

    # Prepare DMG contents
    if not create_dmg_contents():
        print("Failed to prepare DMG contents")
        sys.exit(1)

    # Create DMG
    if not create_dmg():
        print("Warning: DMG creation failed, but .app bundle is available")

    # Create install script
    create_install_script()

    # Summary
    print("\n" + "=" * 50)
    print("Build complete!")
    print("=" * 50)
    print(f"\nOutputs:")
    print(f"  App bundle: {DIST_DIR / 'OtterProbe.app'}")
    print(f"  Auth wizard: {DIST_DIR / 'OtterProbeAuth'}")

    dmg_path = DIST_DIR / f"OtterProbe-{APP_VERSION}.dmg"
    if dmg_path.exists():
        print(f"  DMG installer: {dmg_path}")

    print(f"\nTo install manually:")
    print(f"  1. Copy OtterProbe.app to /Applications")
    print(f"  2. Run OtterProbeAuth for first-time setup")
    print(f"  3. Open OtterProbe.app")


if __name__ == "__main__":
    main()
