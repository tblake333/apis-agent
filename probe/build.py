#!/usr/bin/env python3
"""
Cross-platform build script for Otter Probe.

Automatically detects the platform and builds the appropriate installer:
- Windows: Creates OtterProbe.exe, OtterProbeAuth.exe, and Inno Setup installer
- macOS: Creates OtterProbe.app, OtterProbeAuth, and DMG installer

Usage:
    python build.py [--clean] [--no-installer]

Options:
    --clean         Clean build artifacts before building
    --no-installer  Skip installer creation (just build executables)
    --skip-auth     Skip building auth wizard
    --skip-tray     Skip building tray app
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


# Configuration
APP_NAME = "Otter Probe"
APP_VERSION = "1.0.0"

# Paths
SCRIPT_DIR = Path(__file__).parent
DIST_DIR = SCRIPT_DIR / "dist"
BUILD_DIR = SCRIPT_DIR / "build"

# Platform detection
IS_WINDOWS = sys.platform == "win32"
IS_MACOS = sys.platform == "darwin"
PLATFORM_NAME = "Windows" if IS_WINDOWS else "macOS" if IS_MACOS else sys.platform


def print_header(text: str):
    """Print a section header."""
    print(f"\n{'=' * 60}")
    print(f"  {text}")
    print(f"{'=' * 60}\n")


def run_command(cmd: list, cwd: Path = None, check: bool = True) -> subprocess.CompletedProcess:
    """Run a command and return the result."""
    print(f"  $ {' '.join(str(c) for c in cmd)}")
    return subprocess.run(cmd, cwd=cwd, check=check)


def check_requirements():
    """Check that required tools are installed."""
    print_header("Checking Requirements")

    # Check Python version
    print(f"  Python: {sys.version}")
    if sys.version_info < (3, 9):
        print("  ERROR: Python 3.9+ required")
        return False

    # Check PyInstaller
    if not shutil.which("pyinstaller"):
        print("  ERROR: PyInstaller not found")
        print("  Install with: pip install pyinstaller")
        return False
    print("  PyInstaller: Found")

    # Platform-specific checks
    if IS_WINDOWS:
        if shutil.which("iscc"):
            print("  Inno Setup: Found")
        else:
            print("  Inno Setup: Not found (optional, for installer)")
    elif IS_MACOS:
        if shutil.which("create-dmg"):
            print("  create-dmg: Found")
        else:
            print("  create-dmg: Not found (optional, will use hdiutil)")

    return True


def clean_build():
    """Clean previous build artifacts."""
    print_header("Cleaning Build Artifacts")

    for dir_path in [DIST_DIR, BUILD_DIR]:
        if dir_path.exists():
            print(f"  Removing: {dir_path}")
            shutil.rmtree(dir_path)

    print("  Clean complete")


def build_tray_app() -> bool:
    """Build the tray application."""
    print_header("Building Tray Application")

    try:
        run_command(
            ["pyinstaller", "--clean", "--noconfirm", "probe_tray.spec"],
            cwd=SCRIPT_DIR
        )
        print("\n  Tray app built successfully")
        return True
    except subprocess.CalledProcessError:
        print("\n  ERROR: Failed to build tray app")
        return False


def build_auth_wizard() -> bool:
    """Build the auth wizard."""
    print_header("Building Auth Wizard")

    try:
        run_command(
            ["pyinstaller", "--clean", "--noconfirm", "probe_auth.spec"],
            cwd=SCRIPT_DIR
        )
        print("\n  Auth wizard built successfully")
        return True
    except subprocess.CalledProcessError:
        print("\n  ERROR: Failed to build auth wizard")
        return False


def build_windows_installer() -> bool:
    """Build Windows installer using Inno Setup."""
    print_header("Building Windows Installer")

    iscc = shutil.which("iscc")
    if not iscc:
        print("  Inno Setup Compiler (iscc) not found")
        print("  Download from: https://jrsoftware.org/isinfo.php")
        print("  Skipping installer creation")
        return False

    iss_file = SCRIPT_DIR / "installer" / "probe_installer.iss"
    if not iss_file.exists():
        print(f"  ERROR: {iss_file} not found")
        return False

    try:
        run_command([iscc, str(iss_file)], cwd=SCRIPT_DIR)
        print("\n  Windows installer built successfully")
        return True
    except subprocess.CalledProcessError:
        print("\n  ERROR: Failed to build Windows installer")
        return False


def build_macos_installer() -> bool:
    """Build macOS DMG installer."""
    print_header("Building macOS Installer")

    # Import macOS build script
    build_script = SCRIPT_DIR / "installer" / "build_macos.py"
    if not build_script.exists():
        print(f"  ERROR: {build_script} not found")
        return False

    try:
        # Run the macOS build script's DMG creation
        from installer.build_macos import create_dmg_contents, create_dmg

        if not create_dmg_contents():
            print("  ERROR: Failed to prepare DMG contents")
            return False

        if not create_dmg():
            print("  Warning: DMG creation failed")
            return False

        print("\n  macOS installer built successfully")
        return True
    except Exception as e:
        print(f"\n  ERROR: Failed to build macOS installer: {e}")
        return False


def print_summary():
    """Print build summary."""
    print_header("Build Summary")

    print(f"  Platform: {PLATFORM_NAME}")
    print(f"  Version: {APP_VERSION}")
    print(f"  Output directory: {DIST_DIR}")
    print()

    # List built artifacts
    if DIST_DIR.exists():
        print("  Built artifacts:")
        for item in sorted(DIST_DIR.iterdir()):
            if item.is_file():
                size_mb = item.stat().st_size / (1024 * 1024)
                print(f"    - {item.name} ({size_mb:.1f} MB)")
            elif item.is_dir() and item.suffix in ('.app', ''):
                print(f"    - {item.name}/")

    # Platform-specific notes
    print()
    if IS_WINDOWS:
        installer = DIST_DIR / "installer" / "OtterProbeSetup.exe"
        if installer.exists():
            print(f"  Installer: {installer}")
        else:
            print("  Note: Run Inno Setup to create installer")
    elif IS_MACOS:
        dmg = DIST_DIR / f"OtterProbe-{APP_VERSION}.dmg"
        if dmg.exists():
            print(f"  DMG Installer: {dmg}")
        app = DIST_DIR / "OtterProbe.app"
        if app.exists():
            print(f"  App Bundle: {app}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description=f"Build {APP_NAME} for {PLATFORM_NAME}",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--clean", action="store_true", help="Clean build artifacts before building")
    parser.add_argument("--no-installer", action="store_true", help="Skip installer creation")
    parser.add_argument("--skip-auth", action="store_true", help="Skip building auth wizard")
    parser.add_argument("--skip-tray", action="store_true", help="Skip building tray app")

    args = parser.parse_args()

    print(f"\n{APP_NAME} Build Script")
    print(f"Platform: {PLATFORM_NAME}")
    print(f"Version: {APP_VERSION}")

    # Check requirements
    if not check_requirements():
        sys.exit(1)

    # Clean if requested
    if args.clean:
        clean_build()

    # Build executables
    success = True

    if not args.skip_tray:
        if not build_tray_app():
            success = False

    if not args.skip_auth:
        if not build_auth_wizard():
            success = False

    # Build installer
    if not args.no_installer and success:
        if IS_WINDOWS:
            build_windows_installer()
        elif IS_MACOS:
            build_macos_installer()

    # Print summary
    print_summary()

    if success:
        print("\nBuild completed successfully!")
    else:
        print("\nBuild completed with errors.")
        sys.exit(1)


if __name__ == "__main__":
    main()
