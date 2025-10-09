#!/usr/bin/env python3
"""
Gmail Foot Traffic Automation - Using Gmail API with OAuth
This is the proper solution that will work with forwarded emails.
"""

import os
import base64
import pickle
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from email.utils import parsedate_to_datetime
import io
import argparse

# Configuration
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
PARQUET_FILENAME = 'hourly_foot_traffic.parquet'
FOOT_TRAFFIC_MULTIPLIER = 0.95
MELBOURNE_TZ = ZoneInfo('Australia/Melbourne')

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

def get_gmail_service():
    """Authenticate and return Gmail API service."""
    creds = None
    
    # Token file stores the user's access and refresh tokens
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    # If no valid credentials, let user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Check for credentials from environment (for GitHub Actions)
            if os.path.exists('credentials.json'):
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            else:
                raise FileNotFoundError("credentials.json not found. Please set up OAuth credentials.")
        
        # Save credentials for next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return build('gmail', 'v1', credentials=creds)

def get_data_date_from_email_date(email_date_str):
    """Determine data date from email date."""
    email_date = parsedate_to_datetime(email_date_str)
    email_date_melbourne = email_date.astimezone(MELBOURNE_TZ)
    data_date = (email_date_melbourne - timedelta(days=1)).date()
    return data_date

def search_traffic_emails(service, days_back=30):
    """Search for traffic emails using Gmail API with pagination."""
    print(f"🔍 Searching for VemCount emails (last {days_back} days)...")
    
    # Calculate date for search
    after_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y/%m/%d')
    
    # Search query
    query = f'from:no-reply@vemcount.com subject:"Hourly Foot Traffic" after:{after_date}'
    
    all_messages = []
    page_token = None
    
    # Paginate through all results
    while True:
        if page_token:
            results = service.users().messages().list(
                userId='me', 
                q=query, 
                maxResults=100,
                pageToken=page_token
            ).execute()
        else:
            results = service.users().messages().list(
                userId='me', 
                q=query, 
                maxResults=100
            ).execute()
        
        messages = results.get('messages', [])
        all_messages.extend(messages)
        
        page_token = results.get('nextPageToken')
        if not page_token:
            break
        
        print(f"  📄 Retrieved {len(all_messages)} emails so far...")
    
    print(f"✅ Found {len(all_messages)} total emails")
    
    return all_messages

def get_attachment(service, msg_id, attachment_id):
    """Download attachment from Gmail."""
    attachment = service.users().messages().attachments().get(
        userId='me',
        messageId=msg_id,
        id=attachment_id
    ).execute()
    
    data = attachment['data']
    file_data = base64.urlsafe_b64decode(data)
    return file_data

def process_gmail_messages(service, messages):
    """Process Gmail messages and extract traffic data."""
    files_found = {}
    
    for msg_info in messages:
        try:
            msg = service.users().messages().get(
                userId='me',
                id=msg_info['id'],
                format='full'
            ).execute()
            
            # Get email date
            headers = msg['payload']['headers']
            email_date_str = next(h['value'] for h in headers if h['name'] == 'Date')
            data_date = get_data_date_from_email_date(email_date_str)
            
            # Skip if already have this date
            if data_date in files_found:
                continue
            
            # Look for attachments
            if 'parts' in msg['payload']:
                for part in msg['payload']['parts']:
                    if part.get('filename') and 'traffic' in part['filename'].lower():
                        if part['filename'].endswith('.xlsx'):
                            attachment_id = part['body'].get('attachmentId')
                            if attachment_id:
                                file_data = get_attachment(service, msg_info['id'], attachment_id)
                                files_found[data_date] = {
                                    'content': file_data,
                                    'filename': part['filename']
                                }
                                print(f"  ✅ Found file for {data_date}: {part['filename']}")
        
        except Exception as e:
            print(f"⚠️  Error processing message: {e}")
            continue
    
    return files_found

def parse_traffic_file(file_content, data_date):
    """Parse Excel file and extract traffic data."""
    traffic_data = []
    
    try:
        df = pd.read_excel(io.BytesIO(file_content), sheet_name=0, header=None)
        
        # Find header row
        header_row = None
        for idx, row in df.iterrows():
            if pd.notna(row[0]) and 'Date' in str(row[0]) and 'time' in str(row[0]):
                header_row = idx
                break
        
        if header_row is None:
            return []
        
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
                
                # Extract data
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
            except:
                continue
        
        return traffic_data
    except Exception as e:
        print(f"❌ Failed to parse file for {data_date}: {e}")
        return []

def is_venue_open(date, hour, venue):
    """Check if venue is open."""
    dow = pd.Timestamp(date).dayofweek
    if venue not in VENUE_OPERATING_HOURS:
        return False
    hours = VENUE_OPERATING_HOURS[venue].get(dow, (0, 0))
    if hours[1] > 24:
        return hour >= hours[0] or hour < (hours[1] - 24)
    return hours[0] <= hour < hours[1]

def save_to_parquet(df, filename):
    """Save to parquet file."""
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
    """Merge with existing parquet file."""
    if not os.path.exists(filename):
        return new_df
    
    print(f"📂 Loading existing data...")
    existing_df = pd.read_parquet(filename)
    existing_df['Date'] = pd.to_datetime(existing_df['Date']).dt.date
    
    new_dates = set(new_df['Date'].unique())
    existing_df = existing_df[~existing_df['Date'].isin(new_dates)]
    
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    combined_df = combined_df.sort_values(['DateTime', 'Venue']).reset_index(drop=True)
    
    print(f"📊 Combined: {len(existing_df)} existing + {len(new_df)} new = {len(combined_df)} total records")
    return combined_df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--backfill', type=int, default=30, help='Days to look back')
    args = parser.parse_args()
    
    print("=" * 60)
    print("GMAIL FOOT TRAFFIC PIPELINE - OAuth Edition")
    print(f"Started at: {datetime.now(MELBOURNE_TZ).strftime('%Y-%m-%d %H:%M:%S')} AEDT")
    if args.backfill > 30:
        print(f"🔄 HISTORICAL BACKFILL MODE: Looking back {args.backfill} days")
    print("=" * 60)
    
    # Get Gmail service
    service = get_gmail_service()
    print("✅ Connected to Gmail via OAuth")
    
    # Search for emails
    messages = search_traffic_emails(service, days_back=args.backfill)
    
    if not messages:
        print("❌ No emails found")
        return
    
    # Process messages
    files_found = process_gmail_messages(service, messages)
    
    if not files_found:
        print("❌ No files with attachments found")
        return
    
    # Process all files
    all_data = []
    for data_date, file_info in sorted(files_found.items()):
        print(f"📊 Processing {file_info['filename']} for {data_date}...")
        data = parse_traffic_file(file_info['content'], data_date)
        all_data.extend(data)
    
    if not all_data:
        print("❌ No data extracted")
        return
    
    # Create DataFrame
    df = pd.DataFrame(all_data)
    df['Entering'] = df['Entering'] * FOOT_TRAFFIC_MULTIPLIER
    df['DateTime'] = pd.to_datetime(df['Date']) + pd.to_timedelta(df['Hour'], unit='h')
    df['IsOpen'] = df.apply(lambda row: is_venue_open(row['Date'], row['Hour'], row['Venue']), axis=1)
    df = df.sort_values(['DateTime', 'Venue']).reset_index(drop=True)
    
    print(f"✅ Processed {len(df)} records for {len(files_found)} days")
    
    # Merge and save
    final_df = merge_with_existing(df, PARQUET_FILENAME)
    save_to_parquet(final_df, PARQUET_FILENAME)
    
    # Summary
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
