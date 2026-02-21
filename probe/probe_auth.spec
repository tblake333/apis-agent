# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec file for Otter Probe authentication wizard.

Cross-platform support for Windows and macOS.

Build with:
    pyinstaller probe_auth.spec

This creates a GUI application for the auth wizard.
"""

import sys
from pathlib import Path

block_cipher = None

# Get the directory containing this spec file
spec_dir = Path(SPECPATH)

# Platform-specific settings
is_windows = sys.platform == 'win32'
is_macos = sys.platform == 'darwin'

# Hidden imports
hidden_imports = [
    'auth',
    'auth.device_auth',
    'auth.credentials',
    'installer',
    'installer.auth_wizard_gui',
    'tkinter',
    'tkinter.ttk',
    'tkinter.messagebox',
]

# Excludes
excludes = [
    'matplotlib',
    'numpy',
    'scipy',
    'pandas',
    'PIL',
    'pystray',
    'fdb',
    'pytest',
    'IPython',
]

# Analysis phase
a = Analysis(
    ['main_auth.py'],
    pathex=[str(spec_dir)],
    binaries=[],
    datas=[],
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=excludes,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

# Package phase
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

if is_macos:
    # macOS: Create .app bundle for GUI
    exe = EXE(
        pyz,
        a.scripts,
        [],
        exclude_binaries=True,
        name='OtterProbeAuth',
        debug=False,
        bootloader_ignore_signals=False,
        strip=False,
        upx=True,
        console=False,  # GUI app, no console
        disable_windowed_traceback=False,
        argv_emulation=True,
        target_arch=None,
        codesign_identity=None,
        entitlements_file=None,
    )

    coll = COLLECT(
        exe,
        a.binaries,
        a.zipfiles,
        a.datas,
        strip=False,
        upx=True,
        upx_exclude=[],
        name='OtterProbeAuth',
    )

    app = BUNDLE(
        coll,
        name='OtterProbeAuth.app',
        icon=None,
        bundle_identifier='com.otter.probe.auth',
        info_plist={
            'CFBundleName': 'Otter Probe Setup',
            'CFBundleDisplayName': 'Otter Probe Setup',
            'CFBundleVersion': '1.0.0',
            'CFBundleShortVersionString': '1.0.0',
            'NSHighResolutionCapable': True,
        },
    )
else:
    # Windows: Create single-file GUI executable
    exe = EXE(
        pyz,
        a.scripts,
        a.binaries,
        a.zipfiles,
        a.datas,
        [],
        name='OtterProbeAuth',
        debug=False,
        bootloader_ignore_signals=False,
        strip=False,
        upx=True,
        upx_exclude=[],
        runtime_tmpdir=None,
        console=False,  # GUI app, no console window
        disable_windowed_traceback=False,
        argv_emulation=False,
        target_arch=None,
        codesign_identity=None,
        entitlements_file=None,
        icon=None,  # Can add custom .ico file here
    )
