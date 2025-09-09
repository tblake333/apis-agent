#!/bin/bash
"""
Installation script for the 't' test command.

This script creates a global symlink so you can use 't' from anywhere
instead of having to use './t' from the probe directory.
"""

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
T_COMMAND_PATH="$SCRIPT_DIR/t"

# Check if the t command exists
if [ ! -f "$T_COMMAND_PATH" ]; then
    echo "Error: t command not found at $T_COMMAND_PATH"
    exit 1
fi

# Make sure the t command is executable
chmod +x "$T_COMMAND_PATH"

# Create symlink in /usr/local/bin (requires sudo)
if command -v sudo >/dev/null 2>&1; then
    echo "Creating global symlink for 't' command..."
    sudo ln -sf "$T_COMMAND_PATH" /usr/local/bin/t
    echo "✅ 't' command installed successfully!"
    echo "You can now use 't' from anywhere instead of './t'"
else
    echo "Error: sudo command not found. Please run this script with appropriate permissions."
    echo "Alternatively, you can manually create the symlink:"
    echo "  ln -sf $T_COMMAND_PATH /usr/local/bin/t"
    exit 1
fi

# Test the installation
echo ""
echo "Testing the installation..."
if command -v t >/dev/null 2>&1; then
    echo "✅ 't' command is now available globally"
    echo ""
    echo "Try it out:"
    echo "  t --help"
    echo "  t test_default_values"
    echo "  t tests.test_config.TestDatabaseConfig"
else
    echo "❌ Installation failed - 't' command not found in PATH"
    echo "Make sure /usr/local/bin is in your PATH"
    exit 1
fi
