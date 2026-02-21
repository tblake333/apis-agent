# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec file for Otter Probe tray application.

Cross-platform support for Windows and macOS.

Build with:
    pyinstaller probe_tray.spec

Windows: Creates OtterProbe.exe (windowed, no console)
macOS: Creates OtterProbe.app bundle
"""

import sys
from pathlib import Path

block_cipher = None

# Get the directory containing this spec file
spec_dir = Path(SPECPATH)

# Platform-specific settings
is_windows = sys.platform == 'win32'
is_macos = sys.platform == 'darwin'

# Hidden imports based on platform
hidden_imports = [
    # Core probe modules
    'gui',
    'gui.tray_app',
    'gui.icons',
    'app',
    'app.probe_application',
    'app.probe_service',
    'auth',
    'auth.device_auth',
    'auth.credentials',
    'config',
    'config.app_config',
    'sync',
    'sync.cloud_sync_client',
    'sync.local_buffer',
    'handlers',
    'handlers.changes_intake',
    'handlers.changes_processor',
    'handlers.base_table_handler',
    'database',
    'database.database_manager',
    'models',
    'models.change',
    'models.connection_info',
    'installer',
    'installer.autostart',
    # PIL for icon generation
    'PIL._tkinter_finder',
    # fdb for database
    'fdb',
]

# Platform-specific hidden imports
if is_windows:
    hidden_imports.extend([
        'pystray._win32',
        'win32api',
        'win32con',
        'win32gui',
    ])
elif is_macos:
    hidden_imports.extend([
        'pystray._darwin',
        'AppKit',
        'Foundation',
        'objc',
    ])

# Excludes
excludes = [
    'tkinter',
    'matplotlib',
    'numpy',
    'scipy',
    'pandas',
    'pytest',
    'IPython',
]

# Analysis phase
a = Analysis(
    ['main_tray.py'],
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
    # macOS: Create .app bundle
    exe = EXE(
        pyz,
        a.scripts,
        [],
        exclude_binaries=True,
        name='OtterProbe',
        debug=False,
        bootloader_ignore_signals=False,
        strip=False,
        upx=True,
        console=False,
        disable_windowed_traceback=False,
        argv_emulation=False,
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
        name='OtterProbe',
    )

    app = BUNDLE(
        coll,
        name='OtterProbe.app',
        icon=None,  # Can add custom .icns file here
        bundle_identifier='com.otter.probe',
        info_plist={
            'CFBundleName': 'Otter Probe',
            'CFBundleDisplayName': 'Otter Probe',
            'CFBundleVersion': '1.0.0',
            'CFBundleShortVersionString': '1.0.0',
            'LSUIElement': True,  # Hide from Dock (menu bar app)
            'LSBackgroundOnly': False,
            'NSHighResolutionCapable': True,
            'NSRequiresAquaSystemAppearance': False,  # Support dark mode
        },
    )
else:
    # Windows: Create single-file executable
    exe = EXE(
        pyz,
        a.scripts,
        a.binaries,
        a.zipfiles,
        a.datas,
        [],
        name='OtterProbe',
        debug=False,
        bootloader_ignore_signals=False,
        strip=False,
        upx=True,
        upx_exclude=[],
        runtime_tmpdir=None,
        console=False,  # Hide console window
        disable_windowed_traceback=False,
        argv_emulation=False,
        target_arch=None,
        codesign_identity=None,
        entitlements_file=None,
        icon=None,  # Can add custom .ico file here
    )
