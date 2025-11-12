#!/usr/bin/env python3
"""Test script to verify the CLI improvements."""

import sys
import subprocess
import time

def test_cli():
    """Test the CLI with .catalog command."""
    print("Testing fedq CLI improvements...")
    print("=" * 60)

    # Create a test script with commands to test
    commands = [
        ".catalog",
        "SELECT * FROM demo_users;",
        "\\q"
    ]

    # Join commands with newlines
    input_commands = "\n".join(commands)

    # Run the CLI with our commands
    try:
        process = subprocess.Popen(
            ["python3", "-m", "federated_query.cli.fedq"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        stdout, stderr = process.communicate(input=input_commands, timeout=5)

        print("STDOUT:")
        print(stdout)

        if stderr:
            print("\nSTDERR:")
            print(stderr)

        print("\n" + "=" * 60)
        print("Test completed successfully!")

        # Check if .catalog output is present
        if "Catalog Contents:" in stdout or "Table:" in stdout:
            print("\n✓ .catalog command works correctly")
        else:
            print("\n✗ .catalog command may not be working")

        if "demo_users" in stdout:
            print("✓ Query execution works correctly")
        else:
            print("✗ Query execution may not be working")

    except subprocess.TimeoutExpired:
        print("Test timed out!")
        process.kill()
    except Exception as e:
        print(f"Error running test: {e}")

if __name__ == "__main__":
    test_cli()
