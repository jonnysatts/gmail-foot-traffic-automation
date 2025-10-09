# Gmail Foot Traffic Automation

**Fully automated** pipeline that extracts hourly foot traffic data from VemCount emails and stores it in a Parquet file with a live dashboard.

## 🎯 What This Does

- ✅ **Automatically pulls** foot traffic data from Gmail every day at 7 AM Melbourne time
- ✅ **Processes Excel attachments** from VemCount emails
- ✅ **Stores in Parquet format** for easy analysis
- ✅ **Generates JSON for dashboard** - powers a real-time interactive visualization
- ✅ **Commits to GitHub** - keeps everything synced and backed up
- ✅ **5% discount applied** - automatically applies 0.95 multiplier to "Entering" traffic

## 📊 Live Dashboard

View your traffic data with an interactive React dashboard that:
- Fetches live data from GitHub
- Toggles between Melbourne and Sydney
- Compares days of week
- Shows "Entering" vs "Inside" metrics
- Works anywhere - portable to any React environment

**Dashboard URL**: The dashboard fetches from:
`https://raw.githubusercontent.com/jonnysatts/gmail-foot-traffic-automation/main/traffic_data_compact.json`

This means you can copy the dashboard code to Cursor, v0, or any other coding environment and it will automatically work with your latest data!

## ✅ Fully Automated Setup

This solution uses Google's Gmail API with OAuth, which works perfectly with forwarded emails and runs automatically via GitHub Actions.

### One-Time Setup (15 minutes)

#### 1. Enable Gmail API & Get Credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or select existing)
3. Enable the Gmail API:
   - Go to "APIs & Services" → "Library"
   - Search for "Gmail API"
   - Click "Enable"
4. Create OAuth credentials:
   - Go to "APIs & Services" → "Credentials"
   - Click "Create Credentials" → "OAuth client ID"
   - Choose "Desktop app"
   - Download the credentials JSON file
   - Rename it to `credentials.json`

#### 2. Initial Authorization (Run Locally Once)

```bash
# Clone your repo
cd ~/gmail-foot-traffic-automation

# Install dependencies
pip3 install -r requirements.txt

# Run initial setup (opens browser for authorization)
python3 process_traffic_oauth.py

# This creates token.pickle file with your authorization
```

When you run this the first time, it will:
- Open your browser
- Ask you to sign in to your Gmail account
- Ask you to authorize the app
- Save the authorization to `token.pickle`

#### 3. Historical Backfill

```bash
# Get 12 months of historical data
python3 process_traffic_oauth.py --backfill 365
```

#### 4. Generate Dashboard JSON

```bash
# Create the JSON file for the dashboard
python3 generate_json.py
```

#### 5. Push to GitHub

```bash
# Add everything
git add token.pickle credentials.json hourly_foot_traffic.parquet traffic_data_compact.json

# Commit
git commit -m "Add OAuth credentials and historical data"

# Push
git push
```

**Security Note:** The `token.pickle` and `credentials.json` files contain sensitive data. Make sure your repository is **PRIVATE**. Never make it public with these files.

#### 6. GitHub Actions - Already Configured!

The workflow is already set up in `.github/workflows/daily-traffic.yml`. It will:
- Run daily at 7 AM Melbourne time
- Use the committed OAuth token
- Download new emails automatically
- Update the parquet file
- **Generate fresh JSON for dashboard**
- Commit and push changes

That's it! Now it runs automatically forever.

## 🔄 How It Works

### Daily Automation Flow
1. **7 AM Melbourne time**: GitHub Actions triggers
2. **Searches Gmail**: Looks for new VemCount emails
3. **Downloads attachments**: Gets Excel files
4. **Processes data**: Extracts foot traffic numbers (applies 5% discount)
5. **Updates parquet**: Merges with existing data
6. **Generates JSON**: Creates compact dashboard data
7. **Commits to GitHub**: Pushes updated files
8. **Dashboard auto-updates**: Fetches latest data automatically

### Data Schema

**Parquet file** (`hourly_foot_traffic.parquet`):
- `DateTime`: Timestamp of the hour
- `Date`: Date (without time)
- `Hour`: Hour of day (0-26)
- `Venue`: "Melbourne" or "Sydney"
- `Entering`: People entering (0.95 multiplier applied)
- `Inside`: People inside
- `IsOpen`: Boolean for operating hours

**Dashboard JSON** (`traffic_data_compact.json`):
- Aggregated averages by Venue, Day of Week, and Hour
- 336 records (2 venues × 7 days × 24 hours)
- Compact format (~36KB) for fast loading

### Manual Trigger

You can manually run the automation anytime:
1. Go to Actions tab in GitHub
2. Select "Daily Foot Traffic Update"
3. Click "Run workflow"

## 📥 Accessing the Data

### Download from GitHub
- **Parquet file**: `hourly_foot_traffic.parquet`
- **Dashboard JSON**: `traffic_data_compact.json`

### Read in Python
```python
import pandas as pd

# Read the full dataset
df = pd.read_parquet('hourly_foot_traffic.parquet')
print(df.head())
print(f"Date range: {df['Date'].min()} to {df['Date'].max()}")

# Or read the dashboard JSON
import json
with open('traffic_data_compact.json', 'r') as f:
    data = json.load(f)
```

### Read in R
```r
library(arrow)
df <- read_parquet('hourly_foot_traffic.parquet')
```

### Use in Dashboard
The React dashboard automatically fetches from:
```javascript
fetch('https://raw.githubusercontent.com/jonnysatts/gmail-foot-traffic-automation/main/traffic_data_compact.json')
```

## 📁 Files

- `process_traffic_oauth.py` - Main OAuth-based script
- `generate_json.py` - Converts parquet to JSON for dashboard
- `process_local_files.py` - Alternative: process manually downloaded files
- `requirements.txt` - Python dependencies
- `credentials.json` - OAuth app credentials (commit to private repo)
- `token.pickle` - OAuth authorization token (commit to private repo)
- `hourly_foot_traffic.parquet` - Full historical data
- `traffic_data_compact.json` - Aggregated data for dashboard
- `.github/workflows/daily-traffic.yml` - GitHub Actions automation

## 🐛 Troubleshooting

**"Token expired" errors:**
- Run `python3 process_traffic_oauth.py` locally once to refresh
- Commit the updated `token.pickle`
- Push to GitHub

**No new data:**
- Check if VemCount emails are still arriving
- Verify emails contain "Hourly Foot Traffic" in subject
- Check GitHub Actions logs for errors

**Dashboard not updating:**
- Check that `traffic_data_compact.json` is in your repo
- Verify GitHub Actions is running successfully
- Dashboard fetches from GitHub raw URL - might be cached (wait a few minutes)

**Authorization issues:**
- Delete `token.pickle`
- Run `python3 process_traffic_oauth.py` to re-authorize
- Commit new token

## 🎯 Why This Works (vs Other Solutions)

✅ **OAuth instead of App Passwords** - Works with G Suite/Workspace  
✅ **Gmail API instead of IMAP** - Handles forwarded emails properly  
✅ **Token-based** - No passwords in GitHub secrets  
✅ **Auto-refresh** - Token renews automatically  
✅ **Fully automated** - Zero manual work after setup
✅ **Dashboard-ready** - JSON auto-generated for visualization
✅ **Portable** - Dashboard works in any environment

## 🔧 Maintenance

**None required!** Once set up, this runs automatically forever. The OAuth token automatically refreshes, so you never need to re-authorize.

## 🚀 Next Steps

The current dashboard shows foot traffic patterns. Future enhancements planned:
- **Labor cost overlay** - Compare staffing levels to traffic
- **Efficiency metrics** - Staff-to-customer ratios by hour
- **Cost optimization** - Identify overstaffing/understaffing periods
- **Scheduling recommendations** - Data-driven staff scheduling
