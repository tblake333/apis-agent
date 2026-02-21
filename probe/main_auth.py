#!/usr/bin/env python3
"""
Entry point for the Otter Probe authentication wizard.

This is the PyInstaller target for the GUI auth wizard
that runs during installation.
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

# Use GUI version
from installer.auth_wizard_gui import main

if __name__ == "__main__":
    main()
