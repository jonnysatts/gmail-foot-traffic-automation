#!/usr/bin/env python3
"""
Generate JSON file from parquet for dashboard consumption
"""

import pandas as pd
import json

# Read parquet file
df = pd.read_parquet('hourly_foot_traffic.parquet')

# Add day of week if not already there
if 'DayOfWeek' not in df.columns:
    df['DayOfWeek'] = pd.to_datetime(df['Date']).dt.day_name()

# Aggregate by Venue, DayOfWeek, and Hour (average across all dates)
agg_data = df.groupby(['Venue', 'DayOfWeek', 'Hour']).agg({
    'Entering': 'mean',
    'Inside': 'mean',
    'IsOpen': 'first'
}).reset_index()

# Convert to list of dicts
output = []
for _, row in agg_data.iterrows():
    output.append({
        'Venue': row['Venue'],
        'DayOfWeek': row['DayOfWeek'],
        'Hour': int(row['Hour']),
        'Entering': round(float(row['Entering']), 2),
        'Inside': round(float(row['Inside']), 2),
        'IsOpen': bool(row['IsOpen'])
    })

# Save to JSON
with open('traffic_data_compact.json', 'w') as f:
    json.dump(output, f, indent=2)

print(f"✅ Generated traffic_data_compact.json with {len(output)} records")
