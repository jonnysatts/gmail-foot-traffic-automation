#!/usr/bin/env python3
"""
Gmail Foot Traffic Data Pipeline
Extracts hourly foot traffic data from VemCount emails and stores in Parquet format.
"""

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import imaplib
import email
import io
import os
import argparse
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
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

def connect_to_gmail():
    """Connect to Gmail using environment variables."""
    gmail_user = os.environ.get('GMAIL_USER')
    gmail_password = os.environ.get('GMAIL_APP_PASSWORD')
    
    if not gmail_user or not gmail_password:
        raise ValueError("GMAIL_USER and GMAIL_APP_PASSWORD environment variables must be set")
    
    mail = imaplib.IMAP4_SSL('imap.gmail.com')
    mail.login(gmail_user, gmail_password)
    return mail

def get_data_date(email_date):
    """
    Determine the date the data represents.
    Email received on June 3rd Melbourne time = data for June 2nd.
    """
    email_date_melbourne = email_date.astimezone(MELBOURNE_TZ)
    data_date = (email_date_melbourne - timedelta(days=1)).date()
    return data_date

def find_traffic_emails(mail, days_back=30):
    """
    Search for traffic emails from the last N days.
    Returns dict of {data_date: file_content}
    """
    print(f"🔍 Searching for emails from {TRAFFIC_SENDER} (last {days_back} days)...")
    
    # Select inbox (we'll search all emails, not a specific label)
    mail.select('INBOX')
    
    # Search for emails from sender in last N days
    since_date = (datetime.now() - timedelta(days=days_back)).strftime("%d-%b-%Y")
    search_criteria = f'(FROM "{TRAFFIC_SENDER}" SINCE "{since_date}")'
    
    status, messages = mail.search(None, search_criteria)
    
    if status != 'OK' or not messages[0]:
        print(f"❌ No emails found from {TRAFFIC_SENDER}")
        return {}
    
    email_ids = messages[0].split()
    print(f"✅ Found {len(email_ids)} emails from {TRAFFIC_SENDER}")
    
    files_found = {}
    
    for email_id in reversed(email_ids):  # Process newest first
        try:
            status, msg_data = mail.fetch(email_id, '(RFC822)')
            if status != 'OK':
                continue
            
            msg = email.message_from_bytes(msg_data[0][1])
            email_date = parsedate_to_datetime(msg.get('Date'))
            data_date = get_data_date(email_date)
            
            # Skip if we already have data for this date
            if data_date in files_found:
                continue
            
            # Look for Excel attachment
            for part in msg.walk():
                if part.get_content_disposition() == 'attachment':
                    filename = part.get_filename()
                    if filename and ('traffic' in filename.lower() or 'mel' in filename.lower()):
                        if filename.lower().endswith('.xlsx'):
                            content = part.get_payload(decode=True)
                            files_found[data_date] = {
                                'content': content,
                                'filename': filename
                            }
                            print(f"  ✅ Found file for {data_date}: {filename}")
                            break
        
        except Exception as e:
            print(f"⚠️  Error processing email {email_id.decode()}: {e}")
            continue
    
    return files_found

def parse_traffic_file(file_content, data_date):
    """
    Parse the Excel file and extract foot traffic data.
    Returns list of dicts with hourly data.
    """
    traffic_data = []
    
    try:
        # Read Excel file
        excel_file = pd.ExcelFile(io.BytesIO(file_content))
        sheet_name = 'Yesterday' if 'Yesterday' in excel_file.sheet_names else excel_file.sheet_names[0]
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        
        # Find time column
        time_col = None
        for col in df.columns:
            if any(word in col.lower() for word in ['time', 'hour', 'date']):
                time_col = col
                break
        
        if not time_col:
            time_col = df.columns[0]
        
        # Parse each venue
        for venue in ['Melbourne', 'Sydney']:
            entering_col = None
            inside_col = None
            
            # Find columns for this venue
            for col in df.columns:
                col_lower = col.lower()
                if venue.lower() in col_lower:
                    if 'entering' in col_lower:
                        entering_col = col
                    elif 'inside' in col_lower:
                        inside_col = col
            
            if not entering_col and not inside_col:
                print(f"⚠️  Could not find columns for {venue}")
                continue
            
            # Extract hourly data
            for idx, row in df.iterrows():
                try:
                    time_val = row[time_col]
                    
                    if pd.isna(time_val):
                        continue
                    
                    # Extract hour from time value
                    if isinstance(time_val, pd.Timestamp) or hasattr(time_val, 'hour'):
                        hour = time_val.hour
                    elif isinstance(time_val, str) and ':' in time_val:
                        hour = int(time_val.split(':')[0])
                    elif isinstance(time_val, (int, float)):
                        hour = int(time_val)
                    else:
                        continue
                    
                    if 0 <= hour <= 26:  # Allow extended hours
                        traffic_data.append({
                            'Date': data_date,
                            'Hour': hour,
                            'Venue': venue,
                            'Entering': float(row[entering_col]) if entering_col and pd.notna(row.get(entering_col)) else 0,
                            'Inside': float(row[inside_col]) if inside_col and pd.notna(row.get(inside_col)) else 0
                        })
                
                except Exception as e:
                    continue
        
        return traffic_data
    
    except Exception as e:
        print(f"❌ Failed to parse file for {data_date}: {e}")
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

def process_data(files_found):
    """Process all found files and create final DataFrame."""
    all_data = []
    
    for data_date, file_info in files_found.items():
        print(f"📊 Processing {file_info['filename']} for {data_date}...")
        data = parse_traffic_file(file_info['content'], data_date)
        all_data.extend(data)
    
    if not all_data:
        print("⚠️  No data extracted from files")
        return None
    
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
    
    return df

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
    """Main pipeline execution."""
    parser = argparse.ArgumentParser(description='Process Gmail foot traffic data')
    parser.add_argument('--backfill', type=int, default=30, 
                       help='Number of days to look back for emails (default: 30, use 365+ for historical backfill)')
    args = parser.parse_args()
    
    print("=" * 60)
    print("GMAIL FOOT TRAFFIC PIPELINE")
    print(f"Started at: {datetime.now(MELBOURNE_TZ).strftime('%Y-%m-%d %H:%M:%S')} AEDT")
    if args.backfill > 30:
        print(f"🔄 HISTORICAL BACKFILL MODE: Looking back {args.backfill} days")
    print("=" * 60)
    
    try:
        # Connect to Gmail
        mail = connect_to_gmail()
        print("✅ Connected to Gmail")
        
        # Find traffic emails
        files_found = find_traffic_emails(mail, days_back=args.backfill)
        mail.logout()
        
        if not files_found:
            print("❌ No files found. Exiting.")
            return
        
        # Process data
        new_df = process_data(files_found)
        
        if new_df is None or len(new_df) == 0:
            print("❌ No data to save. Exiting.")
            return
        
        # Merge with existing data
        final_df = merge_with_existing(new_df, PARQUET_FILENAME)
        
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
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
