#!/usr/bin/env python3
"""
Test runner script for the probe application tests.

This script provides an easy way to run all tests with proper configuration.
"""
import sys
import os
import subprocess
from pathlib import Path

# Add the parent directory to the path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent))


def run_tests():
    """Run all tests using pytest."""
    # Change to the probe directory
    probe_dir = Path(__file__).parent.parent
    os.chdir(probe_dir)
    
    # Run pytest with verbose output
    cmd = [
        sys.executable, "-m", "pytest",
        "tests/",
        "-v",  # Verbose output
        "--tb=short",  # Short traceback format
        "--color=yes",  # Colored output
        "-x",  # Stop on first failure
    ]
    
    print("Running tests for the probe application...")
    print(f"Command: {' '.join(cmd)}")
    print("-" * 50)
    
    try:
        result = subprocess.run(cmd, check=True)
        print("\n" + "=" * 50)
        print("✅ All tests passed!")
        return 0
    except subprocess.CalledProcessError as e:
        print("\n" + "=" * 50)
        print(f"❌ Tests failed with exit code {e.returncode}")
        return e.returncode
    except FileNotFoundError:
        print("❌ pytest not found. Please install it with: pip install pytest")
        return 1


def run_specific_test(test_file):
    """Run a specific test file."""
    probe_dir = Path(__file__).parent.parent
    os.chdir(probe_dir)
    
    cmd = [
        sys.executable, "-m", "pytest",
        f"tests/{test_file}",
        "-v",
        "--tb=short",
        "--color=yes",
    ]
    
    print(f"Running specific test: {test_file}")
    print(f"Command: {' '.join(cmd)}")
    print("-" * 50)
    
    try:
        result = subprocess.run(cmd, check=True)
        print("\n" + "=" * 50)
        print(f"✅ Test {test_file} passed!")
        return 0
    except subprocess.CalledProcessError as e:
        print("\n" + "=" * 50)
        print(f"❌ Test {test_file} failed with exit code {e.returncode}")
        return e.returncode


def main():
    """Main function to handle command line arguments."""
    if len(sys.argv) > 1:
        test_file = sys.argv[1]
        return run_specific_test(test_file)
    else:
        return run_tests()


if __name__ == "__main__":
    sys.exit(main())
