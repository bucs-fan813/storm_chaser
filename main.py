#!/usr/bin/env python3
import os
from pathlib import Path
import subprocess
import sys


def main():
    # List all .py files in the current directory, excluding this script
    py_files = [f for f in os.listdir('.') if f.endswith('.py') and f != sys.argv[0]]

    if not py_files:
        print("No other Python files found to execute.")
        return

    for script in py_files:
        print(f"Executing {script}...")
        try:
            # Use the same Python interpreter to run the script
            result = subprocess.run([sys.executable, script], check=True, capture_output=False, text=True)
            print(f"Output of {script}:\n{result.stdout}")
            if result.stderr:
                print(f"Errors from {script}:\n{result.stderr}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to execute {script}. Return code: {e.returncode}")
            print(f"Error output: {e.stderr}")

def delete_files(patterns=('*.csv', '*.zip')):
    cwd = Path('.')
    deleted = 0
    for pattern in patterns:
        for file in cwd.glob(pattern, case_sensitive=False):
            try:
                file.unlink()
                print(f"Deleted {file.name}")
                deleted += 1
            except OSError as e:
                print(f"Failed to delete {file.name}: {e}")
    if deleted == 0:
        print("No .csv or .zip files found to delete.")

if __name__ == '__main__':

    if sys.argv[1] in ["--delete", "-d", "/?"]:
        delete_files()

    main()
