#!/usr/bin/env python3
"""
Gmail Foot Traffic Data Pipeline - Using Gmail API via Claude
Extracts hourly foot traffic data from VemCount emails and stores in Parquet format.
"""

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import io
import os
import argparse
import json
import base64
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Configuration
TRAFFIC_SENDER = 'no-reply@vemcount.com'
PARQUET_FILENAME = 'hourly_foot_traffic.parquet'
FOOT_TRAFFIC_MULTIPLIER = 0.95
MELBOURNE_TZ = ZoneInfo('Australia/Melbourne')

# Venue operating hours (day_of_week: (open_hour, close_hour))
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

def get_data_date_from_email_date(email_date_str):
    """
    Parse email date and determine the date the data represents.
    Email received on June 3rd Melbourne time = data for June 2nd.
    """
    # Parse the date string (format: "Wed, 08 Oct 2025 22:01:46 +0000")
    from email.utils import parsedate_to_datetime
    email_date = parsedate_to_datetime(email_date_str)
    email_date_melbourne = email_date.astimezone(MELBOURNE_TZ)
    data_date = (email_date_melbourne - timedelta(days=1)).date()
    return data_date

def parse_traffic_file(file_content, data_date):
    """
    Parse the Excel file and extract foot traffic data.
    Returns list of dicts with hourly data.
    """
    traffic_data = []
    
    try:
        # Read Excel file - the file has multiple columns side by side
        df = pd.read_excel(io.BytesIO(file_content), sheet_name=0, header=None)
        
        # Based on the PDF you shared, the structure is:
        # Column 0: Date/time for Melbourne
        # Column 1: Melbourne Visitors inside
        # Column 2: Melbourne Visitors entering
        # Column 3: Sydney Visitors inside  
        # Column 4: Sydney Visitors entering
        
        # Find the header row (contains "Date / time")
        header_row = None
        for idx, row in df.iterrows():
            if pd.notna(row[0]) and 'Date' in str(row[0]) and 'time' in str(row[0]):
                header_row = idx
                break
        
        if header_row is None:
            print(f"⚠️  Could not find header row in file for {data_date}")
            return []
        
        # Set proper column names based on header row
        df_data = df.iloc[header_row + 1:].copy()
        
        # Extract hourly data for both venues
        for idx, row in df_data.iterrows():
            try:
                time_val = row[0]
                
                if pd.isna(time_val):
                    continue
                
                # Skip summary rows (Total, Average, etc.)
                if isinstance(time_val, str) and any(word in time_val.lower() for word in ['total', 'average', 'minimum', 'maximum']):
                    continue
                
                # Extract hour from datetime
                if isinstance(time_val, pd.Timestamp) or hasattr(time_val, 'hour'):
                    hour = time_val.hour
                elif isinstance(time_val, str):
                    # Try parsing the datetime string
                    try:
                        parsed_time = pd.to_datetime(time_val)
                        hour = parsed_time.hour
                    except:
                        continue
                else:
                    continue
                
                # Extract Melbourne data (columns 1 and 2)
                melb_inside = float(row[1]) if pd.notna(row[1]) and str(row[1]).replace('-','').replace('.','').isdigit() else 0
                melb_entering = float(row[2]) if pd.notna(row[2]) and str(row[2]).replace('-','').replace('.','').isdigit() else 0
                
                # Extract Sydney data (columns 3 and 4)
                syd_inside = float(row[3]) if pd.notna(row[3]) and str(row[3]).replace('-','').replace('.','').isdigit() else 0
                syd_entering = float(row[4]) if pd.notna(row[4]) and str(row[4]).replace('-','').replace('.','').isdigit() else 0
                
                if 0 <= hour <= 26:  # Allow extended hours
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
        
        return traffic_data
    
    except Exception as e:
        print(f"❌ Failed to parse file for {data_date}: {e}")
        import traceback
        traceback.print_exc()
        return []

def is_venue_open(date, hour, venue):
    """Check if venue is open at given date/hour."""
    dow = pd.Timestamp(date).dayofweek
    
    if venue not in VENUE_OPERATING_HOURS:
        return False
    
    hours = VENUE_OPERATING_HOURS[venue].get(dow, (0, 0))
    
    # Handle venues open past midnight
    if hours[1] > 24:
        return hour >= hours[0] or hour < (hours[1] - 24)
    
    return hours[0] <= hour < hours[1]

def save_to_parquet(df, filename):
    """Save DataFrame to Parquet file with proper schema."""
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
    """Merge new data with existing Parquet file if it exists."""
    if not os.path.exists(filename):
        return new_df
    
    print(f"📂 Loading existing data from {filename}...")
    existing_df = pd.read_parquet(filename)
    existing_df['Date'] = pd.to_datetime(existing_df['Date']).dt.date
    
    # Get dates in new data
    new_dates = set(new_df['Date'].unique())
    
    # Remove existing records for these dates (we'll replace with new data)
    existing_df = existing_df[~existing_df['Date'].isin(new_dates)]
    
    # Combine and sort
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    combined_df = combined_df.sort_values(['DateTime', 'Venue']).reset_index(drop=True)
    
    print(f"📊 Combined: {len(existing_df)} existing + {len(new_df)} new = {len(combined_df)} total records")
    
    return combined_df

def main():
    """Main pipeline execution using Gmail search results from stdin."""
    print("=" * 60)
    print("GMAIL FOOT TRAFFIC PIPELINE (Gmail API Version)")
    print(f"Started at: {datetime.now(MELBOURNE_TZ).strftime('%Y-%m-%d %H:%M:%S')} AEDT")
    print("=" * 60)
    
    # Read Gmail search results from stdin (passed from wrapper script)
    print("📧 Reading Gmail search results from stdin...")
    gmail_data = json.load(open('/tmp/gmail_search_results.json'))
    
    print(f"✅ Found {len(gmail_data['messages'])} emails")
    
    # Process each email
    files_found = {}
    
    for msg in gmail_data['messages']:
        try:
            email_date_str = None
            attachment_data = None
            attachment_filename = None
            
            # Extract date from headers
            for header in msg['payload']['headers']:
                if header['name'] == 'Date':
                    email_date_str = header['value']
                    break
            
            if not email_date_str:
                continue
            
            data_date = get_data_date_from_email_date(email_date_str)
            
            # Skip if we already have data for this date
            if data_date in files_found:
                continue
            
            # Find attachment
            if 'parts' in msg['payload']:
                for part in msg['payload']['parts']:
                    if part.get('filename') and ('Traffic' in part['filename'] or 'traffic' in part['filename']):
                        if part['filename'].endswith('.xlsx'):
                            # Get attachment data
                            if 'data' in part['body']:
                                attachment_data = base64.urlsafe_b64decode(part['body']['data'])
                                attachment_filename = part['filename']
                                break
            
            if attachment_data:
                files_found[data_date] = {
                    'content': attachment_data,
                    'filename': attachment_filename
                }
                print(f"  ✅ Found file for {data_date}: {attachment_filename}")
        
        except Exception as e:
            print(f"⚠️  Error processing message: {e}")
            continue
    
    if not files_found:
        print("❌ No files with attachments found. Exiting.")
        return
    
    # Process all found files
    all_data = []
    
    for data_date, file_info in sorted(files_found.items()):
        print(f"📊 Processing {file_info['filename']} for {data_date}...")
        data = parse_traffic_file(file_info['content'], data_date)
        all_data.extend(data)
    
    if not all_data:
        print("⚠️  No data extracted from files")
        return
    
    # Create DataFrame
    df = pd.DataFrame(all_data)
    
    # Apply multiplier to Entering traffic
    df['Entering'] = df['Entering'] * FOOT_TRAFFIC_MULTIPLIER
    
    # Create DateTime column
    df['DateTime'] = pd.to_datetime(df['Date']) + pd.to_timedelta(df['Hour'], unit='h')
    
    # Add IsOpen flag
    df['IsOpen'] = df.apply(lambda row: is_venue_open(row['Date'], row['Hour'], row['Venue']), axis=1)
    
    # Sort by DateTime and Venue
    df = df.sort_values(['DateTime', 'Venue']).reset_index(drop=True)
    
    print(f"✅ Processed {len(df)} records for {len(files_found)} days")
    
    # Merge with existing data
    final_df = merge_with_existing(df, PARQUET_FILENAME)
    
    # Save to Parquet
    save_to_parquet(final_df, PARQUET_FILENAME)
    
    # Print summary
    date_range = final_df['Date'].agg(['min', 'max'])
    print("\n" + "=" * 60)
    print("✅ PIPELINE COMPLETED SUCCESSFULLY")
    print(f"📅 Date range: {date_range['min']} to {date_range['max']}")
    print(f"📊 Total records: {len(final_df):,}")
    print(f"🏢 Venues: {', '.join(final_df['Venue'].unique())}")
    print(f"Finished at: {datetime.now(MELBOURNE_TZ).strftime('%Y-%m-%d %H:%M:%S')} AEDT")
    print("=" * 60)

if __name__ == "__main__":
    main()
