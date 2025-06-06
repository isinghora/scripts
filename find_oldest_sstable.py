#!/usr/bin/env python3

import os
from datetime import datetime

def find_oldest_sstable_globally():
    """
    Searches through all keyspaces and tables under /mnt/cassandra to find the
    single oldest SSTable (Data.db file) based on its modification time.
    """
    # Base path for Cassandra data directories
    base_cassandra_path = '/mnt/cassandra'

    # List of system keyspaces to potentially ignore. Add others if needed.
    system_keyspaces = ['system', 'system_schema', 'system_auth',
                        'system_distributed', 'system_traces', 'system_views']

    # Check if the base Cassandra data directory exists
    if not os.path.isdir(base_cassandra_path):
        print(f"Error: Cassandra data directory not found at '{base_cassandra_path}'")
        print("Please ensure you are running this script on the correct server.")
        return

    oldest_file_path = None
    # Initialize oldest_time with a value representing positive infinity
    oldest_file_time = float('inf')

    print(f"Scanning for the oldest Data.db file under: {base_cassandra_path}\n")

    try:
        # Iterate through all items in the base directory (these should be keyspaces)
        for keyspace in os.listdir(base_cassandra_path):
            keyspace_path = os.path.join(base_cassandra_path, keyspace)

            # Skip if it's not a directory or if it's a system keyspace
            if not os.path.isdir(keyspace_path) or keyspace in system_keyspaces:
                continue

            # Iterate through all items in the keyspace directory (these should be tables)
            for table_dir in os.listdir(keyspace_path):
                # Table directories often contain a unique ID, so we just check if it's a directory
                table_path = os.path.join(keyspace_path, table_dir)
                if not os.path.isdir(table_path):
                    continue

                # os.walk will traverse the directory tree for the current table
                for root, dirs, files in os.walk(table_path):
                    # Check if 'Data.db' is in the list of files
                    if 'Data.db' in files:
                        full_path = os.path.join(root, 'Data.db')
                        try:
                            # Get the modification time of the file (seconds since epoch)
                            mtime = os.path.getmtime(full_path)

                            # If this file's time is older than the oldest found so far...
                            if mtime < oldest_file_time:
                                # ...update our records.
                                oldest_file_time = mtime
                                oldest_file_path = full_path
                        except FileNotFoundError:
                            # Can happen in rare race conditions if a file is deleted
                            continue

    except PermissionError as e:
        print(f"Permission denied: Could not scan directory. Please run with appropriate permissions.")
        print(f"Details: {e}")
        return
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return

    # After scanning all keyspaces and tables, check if we found any Data.db files
    if oldest_file_path:
        # Convert the epoch time to a human-readable date and time string
        readable_time = datetime.fromtimestamp(oldest_file_time).strftime('%Y-%m-%d %H:%M:%S')
        print("--- Oldest SSTable Found Across All Keyspaces ---")
        print(f"Path: {oldest_file_path}")
        print(f"Modification Time: {readable_time}")
    else:
        print(f"No 'Data.db' files were found under '{base_cassandra_path}' (excluding system keyspaces).")


if __name__ == "__main__":
    # The script now runs without needing command-line arguments
    find_oldest_sstable_globally()
