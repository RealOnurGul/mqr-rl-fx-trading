#!/usr/bin/env python3
import os
import sys
import zipfile
import argparse
import pandas as pd
import mysql.connector
from io import BytesIO
from pathlib import Path
from datetime import datetime

"""
This script loads all the data in zip files into your local sql database
The script can be run like this:
python loadcsvs.py --host=localhost --user=myuser --password=mypassword --database=forex_db --data_dir=/Users/yourname/forex/data
You can also run python3 loadcsvs.py --help
data
├── Aug2024
│   ├── AUDJPY-2024-08.zip
│   ├── AUDJPY-2024-09.zip
│   ├── AUDNZD-2024-08.zip
│   ├── AUDNZD-2024-09.zip
│   ├── AUDUSD-2024-08.zip
│   ├── CADJPY-2024-08.zip
│   .
│   .
│   .
├── Dec2024
├── Jan2025
├── Nov2024
├── Oct2024
├── Sep2024
└── .gitignore

"""
def parse_arguments():
    """Parse command line arguments with defaults."""
    parser = argparse.ArgumentParser(description='Import forex data from zip files to MySQL')
    parser.add_argument('--host', default='localhost', help='MySQL host')
    parser.add_argument('--user', default='root', help='MySQL username')
    parser.add_argument('--password', default='', help='MySQL password')
    parser.add_argument('--database', default='forex_data', help='MySQL database name')
    parser.add_argument('--data_dir', default='./data', help='Directory containing the forex data')
    return parser.parse_args()

def create_database_if_not_exists(conn, cursor, db_name):
    """Create the database if it doesn't exist."""
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    cursor.execute(f"USE {db_name}")
    print(f"Using database: {db_name}")

def get_table_name(filename):
    """Extract forex pair, month and year from filename to create table name."""
    # Parse something like 'GBPUSD-2024-08.zip'
    parts = os.path.basename(filename).split('.')[0].split('-')
    forex_pair = parts[0]
    year_month = parts[1] + "_" + parts[2]
    return f"{forex_pair}_{year_month}"

def parse_timestamp(timestamp_str):
    """Parse timestamp accounting for potential milliseconds in the data."""
    try:
        # Try to parse with milliseconds (e.g., "20240801 00:00:00.110")
        return datetime.strptime(timestamp_str, '%Y%m%d %H:%M:%S.%f')
    except ValueError:
        # If that fails, try without milliseconds (e.g., "20240801 00:00:00")
        return datetime.strptime(timestamp_str, '%Y%m%d %H:%M:%S')

def format_datetime_with_ms(dt):
    """Format datetime object preserving milliseconds for MySQL."""
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # MySQL DATETIME(3) truncates to 3 decimal places

def process_zip_file(zip_path, cursor, conn):
    """Process a single zip file and import its data to MySQL."""
    table_name = get_table_name(zip_path)
    print(f"Processing {zip_path} into table {table_name}")
    
    # Create table if not exists with indexes for bid, ask, and timestamp
    # DATETIME(6) ensures microsecond precision (up to 6 decimal places)
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        timestamp DATETIME(6) NOT NULL,
        bid DECIMAL(10, 5) NOT NULL,
        ask DECIMAL(10, 5) NOT NULL,
        PRIMARY KEY (timestamp),
        INDEX idx_bid (bid),
        INDEX idx_ask (ask),
        INDEX idx_timestamp (timestamp)
    )
    """)
    
    # Read zip file without extracting to disk
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            if file_info.filename.endswith('.csv'):
                # Read CSV directly from zip
                with zip_ref.open(file_info) as csv_file:
                    # Read data in chunks to handle large files efficiently
                    chunk_size = 100000  # Adjust based on memory constraints
                    
                    for chunk in pd.read_csv(
                            BytesIO(csv_file.read()),
                            header=None,
                            names=['pair', 'timestamp', 'bid', 'ask'],
                            chunksize=chunk_size):
                        
                        # Drop the pair column and prepare the data
                        chunk.drop(columns=['pair'], inplace=True)
                        
                        # Convert timestamp format using the safe parsing function
                        chunk['timestamp'] = chunk['timestamp'].apply(parse_timestamp)
                        
                        # Prepare batch insert - format datetime with milliseconds preserved
                        values = [
                            (row['timestamp'], row['bid'], row['ask']) 
                            for _, row in chunk.iterrows()
                        ]
                        
                        # Batch insert
                        cursor.executemany(
                            f"INSERT IGNORE INTO {table_name} (timestamp, bid, ask) VALUES (%s, %s, %s)",
                            values
                        )
                        conn.commit()
                        print(f"Inserted {len(values)} records into {table_name}")
    
    print(f"Finished processing {table_name}")
    return table_name

def process_data_directory(data_dir, db_config):
    """Process all zip files in the data directory and subdirectories."""
    # Connect to MySQL with specific configuration to handle microseconds
    conn = mysql.connector.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password']
    )
    cursor = conn.cursor()
    
    # Create database if not exists
    create_database_if_not_exists(conn, cursor, db_config['database'])
    
    # Find all zip files
    processed_tables = []
    
    # Walk through the directory structure
    for root, _, files in os.walk(data_dir):
        for file in files:
            if file.endswith('.zip') and any(cur in file for cur in ['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF', 'NZD']):
                zip_path = os.path.join(root, file)
                table_name = process_zip_file(zip_path, cursor, conn)
                processed_tables.append(table_name)
    
    # Close connections
    cursor.close()
    conn.close()
    
    return processed_tables

def main():
    args = parse_arguments()
    
    db_config = {
        'host': args.host,
        'user': args.user,
        'password': args.password,
        'database': args.database
    }
    
    data_dir = args.data_dir
    
    # Verify data directory exists
    if not os.path.exists(data_dir):
        print(f"Error: Data directory {data_dir} does not exist.")
        sys.exit(1)
    
    print(f"Starting forex data import from {data_dir}")
    processed_tables = process_data_directory(data_dir, db_config)
    
    print(f"Import completed. Processed {len(processed_tables)} tables:")
    for table in processed_tables:
        print(f"- {table}")

if __name__ == "__main__":
    main()