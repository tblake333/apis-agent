#!/usr/bin/env python3
"""
Entry point for the Otter Probe tray application.

This is the main PyInstaller target for the Windows GUI application.
"""

import sys
import os

# Ensure the probe package directory is in the path
if getattr(sys, 'frozen', False):
    # Running as PyInstaller bundle
    bundle_dir = os.path.dirname(sys.executable)
else:
    # Running from source
    bundle_dir = os.path.dirname(os.path.abspath(__file__))

sys.path.insert(0, bundle_dir)

from gui.tray_app import main

if __name__ == "__main__":
    main()
