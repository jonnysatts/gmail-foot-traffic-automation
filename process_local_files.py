#!/usr/bin/env python3
"""
Simple Local Processing Script
Processes Excel files that have been manually downloaded from Gmail.

Usage:
1. Download "Traffic By Hour - Mel Syd.xlsx" files from your Gmail
2. Put them all in a folder (e.g., ~/Downloads/traffic_files/)
3. Run: python3 process_local_files.py ~/Downloads/traffic_files/

The script will:
- Read all xlsx files in the folder
- Extract the date from the email's send date (you'll be prompted)
- Or use the file's modification date as a fallback
- Process all files and create/update the parquet file
"""

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os
import sys
import argparse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

# Configuration
PARQUET_FILENAME = 'hourly_foot_traffic.parquet'
FOOT_TRAFFIC_MULTIPLIER = 0.95
MELBOURNE_TZ = ZoneInfo('Australia/Melbourne')

# Venue operating hours
VENUE_OPERATING_HOURS = {
    "Melbourne": {
        0: (12, 23), 1: (12, 23), 2: (12, 23), 3: (12, 23),
        4: (12, 24), 5: (12, 25), 6: (12, 23)
    },
    "Sydney": {
        0: (12, 23), 1: (12, 23), 2: (12, 23), 3: (12, 23),
        4: (12, 24), 5: (12, 25), 6: (12, 23)
    }
}

def parse_traffic_file(file_path):
    """Parse Excel file and extract hourly traffic data."""
    print(f"\n📊 Processing: {os.path.basename(file_path)}")
    
    # Get file modification date as fallback for data date
    # The email is sent the day after the data, so file mod date - 1 day
    file_mtime = os.path.getmtime(file_path)
    file_date = datetime.fromtimestamp(file_mtime, tz=MELBOURNE_TZ).date()
    data_date = file_date - timedelta(days=1)
    
    print(f"📅 Using data date: {data_date} (file modified: {file_date})")
    
    traffic_data = []
    
    try:
        # Read Excel file
        df = pd.read_excel(file_path, sheet_name=0, header=None)
        
        # Find header row
        header_row = None
        for idx, row in df.iterrows():
            if pd.notna(row[0]) and 'Date' in str(row[0]) and 'time' in str(row[0]):
                header_row = idx
                break
        
        if header_row is None:
            print(f"⚠️  Could not find header row")
            return []
        
        # Process data rows
        df_data = df.iloc[header_row + 1:].copy()
        
        for idx, row in df_data.iterrows():
            try:
                time_val = row[0]
                
                if pd.isna(time_val):
                    continue
                
                # Skip summary rows
                if isinstance(time_val, str) and any(word in time_val.lower() for word in ['total', 'average', 'minimum', 'maximum']):
                    continue
                
                # Extract hour
                if isinstance(time_val, pd.Timestamp) or hasattr(time_val, 'hour'):
                    hour = time_val.hour
                elif isinstance(time_val, str):
                    try:
                        parsed_time = pd.to_datetime(time_val)
                        hour = parsed_time.hour
                    except:
                        continue
                else:
                    continue
                
                # Extract data for both venues
                melb_inside = float(row[1]) if pd.notna(row[1]) and str(row[1]).replace('-','').replace('.','').isdigit() else 0
                melb_entering = float(row[2]) if pd.notna(row[2]) and str(row[2]).replace('-','').replace('.','').isdigit() else 0
                syd_inside = float(row[3]) if pd.notna(row[3]) and str(row[3]).replace('-','').replace('.','').isdigit() else 0
                syd_entering = float(row[4]) if pd.notna(row[4]) and str(row[4]).replace('-','').replace('.','').isdigit() else 0
                
                if 0 <= hour <= 26:
                    traffic_data.append({
                        'Date': data_date,
                        'Hour': hour,
                        'Venue': 'Melbourne',
                        'Entering': melb_entering,
                        'Inside': melb_inside
                    })
                    traffic_data.append({
                        'Date': data_date,
                        'Hour': hour,
                        'Venue': 'Sydney',
                        'Entering': syd_entering,
                        'Inside': syd_inside
                    })
            
            except Exception as e:
                continue
        
        print(f"✅ Extracted {len(traffic_data)} records")
        return traffic_data
    
    except Exception as e:
        print(f"❌ Failed to parse file: {e}")
        import traceback
        traceback.print_exc()
        return []

def is_venue_open(date, hour, venue):
    """Check if venue is open at given date/hour."""
    dow = pd.Timestamp(date).dayofweek
    
    if venue not in VENUE_OPERATING_HOURS:
        return False
    
    hours = VENUE_OPERATING_HOURS[venue].get(dow, (0, 0))
    
    if hours[1] > 24:
        return hour >= hours[0] or hour < (hours[1] - 24)
    
    return hours[0] <= hour < hours[1]

def save_to_parquet(df, filename):
    """Save DataFrame to Parquet file."""
    schema = pa.schema([
        ('DateTime', pa.timestamp('ns')),
        ('Date', pa.date32()),
        ('Hour', pa.int64()),
        ('Venue', pa.string()),
        ('Entering', pa.float64()),
        ('Inside', pa.float64()),
        ('IsOpen', pa.bool_())
    ])
    
    table = pa.Table.from_pandas(df, schema=schema)
    pq.write_table(table, filename)
    print(f"💾 Saved to {filename}")

def merge_with_existing(new_df, filename):
    """Merge new data with existing Parquet file."""
    if not os.path.exists(filename):
        return new_df
    
    print(f"📂 Loading existing data from {filename}...")
    existing_df = pd.read_parquet(filename)
    existing_df['Date'] = pd.to_datetime(existing_df['Date']).dt.date
    
    new_dates = set(new_df['Date'].unique())
    existing_df = existing_df[~existing_df['Date'].isin(new_dates)]
    
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    combined_df = combined_df.sort_values(['DateTime', 'Venue']).reset_index(drop=True)
    
    print(f"📊 Combined: {len(existing_df)} existing + {len(new_df)} new = {len(combined_df)} total records")
    
    return combined_df

def main():
    parser = argparse.ArgumentParser(description='Process local traffic Excel files')
    parser.add_argument('folder', help='Folder containing Excel files')
    args = parser.parse_args()
    
    folder_path = Path(args.folder)
    
    if not folder_path.exists():
        print(f"❌ Folder not found: {folder_path}")
        return
    
    print("=" * 60)
    print("LOCAL FILE PROCESSING - FOOT TRAFFIC DATA")
    print(f"Started at: {datetime.now(MELBOURNE_TZ).strftime('%Y-%m-%d %H:%M:%S')} AEDT")
    print(f"📁 Scanning folder: {folder_path}")
    print("=" * 60)
    
    # Find all Excel files
    excel_files = list(folder_path.glob("*.xlsx")) + list(folder_path.glob("*.xls"))
    
    if not excel_files:
        print(f"❌ No Excel files found in {folder_path}")
        return
    
    print(f"✅ Found {len(excel_files)} Excel file(s)")
    
    # Process all files
    all_data = []
    
    for file_path in sorted(excel_files):
        data = parse_traffic_file(file_path)
        all_data.extend(data)
    
    if not all_data:
        print("❌ No data extracted from files")
        return
    
    # Create DataFrame
    df = pd.DataFrame(all_data)
    
    # Apply multiplier
    df['Entering'] = df['Entering'] * FOOT_TRAFFIC_MULTIPLIER
    
    # Create DateTime column
    df['DateTime'] = pd.to_datetime(df['Date']) + pd.to_timedelta(df['Hour'], unit='h')
    
    # Add IsOpen flag
    df['IsOpen'] = df.apply(lambda row: is_venue_open(row['Date'], row['Hour'], row['Venue']), axis=1)
    
    # Sort
    df = df.sort_values(['DateTime', 'Venue']).reset_index(drop=True)
    
    print(f"\n✅ Processed {len(df)} total records")
    
    # Merge with existing
    final_df = merge_with_existing(df, PARQUET_FILENAME)
    
    # Save
    save_to_parquet(final_df, PARQUET_FILENAME)
    
    # Summary
    date_range = final_df['Date'].agg(['min', 'max'])
    print("\n" + "=" * 60)
    print("✅ PROCESSING COMPLETED SUCCESSFULLY")
    print(f"📅 Date range: {date_range['min']} to {date_range['max']}")
    print(f"📊 Total records: {len(final_df):,}")
    print(f"🏢 Venues: {', '.join(final_df['Venue'].unique())}")
    print(f"Finished at: {datetime.now(MELBOURNE_TZ).strftime('%Y-%m-%d %H:%M:%S')} AEDT")
    print("=" * 60)

if __name__ == "__main__":
    main()
