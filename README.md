# Gmail Foot Traffic Automation

**Fully automated** pipeline that extracts hourly foot traffic data from VemCount emails and stores it in a Parquet file.

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

#### 4. Push to GitHub

```bash
# Add the token (IMPORTANT: token.pickle must be committed for Actions to work)
git add token.pickle credentials.json hourly_foot_traffic.parquet

# Commit
git commit -m "Add OAuth credentials and historical data"

# Push
git push
```

**Security Note:** The `token.pickle` and `credentials.json` files contain sensitive data. Make sure your repository is **PRIVATE**. Never make it public with these files.

#### 5. Set Up GitHub Actions

The workflow is already configured in `.github/workflows/daily-traffic.yml`. It will:
- Run daily at 7 AM Melbourne time
- Use the committed OAuth token
- Download new emails automatically
- Update the parquet file
- Commit and push changes

That's it! Now it runs automatically forever.

## How It Works

### Daily Automation
- **7 AM Melbourne time**: GitHub Actions triggers
- **Searches Gmail**: Looks for new VemCount emails
- **Downloads attachments**: Gets Excel files
- **Processes data**: Extracts foot traffic numbers
- **Updates parquet**: Merges with existing data
- **Commits to GitHub**: Pushes updated file

### Data Schema

The Parquet file contains:
- `DateTime`: Timestamp of the hour
- `Date`: Date (without time)
- `Hour`: Hour of day (0-26)
- `Venue`: "Melbourne" or "Sydney"
- `Entering`: People entering (0.95 multiplier applied)
- `Inside`: People inside
- `IsOpen`: Boolean for operating hours

### Manual Trigger

You can manually run the automation anytime:
1. Go to Actions tab in GitHub
2. Select "Daily Foot Traffic Update"
3. Click "Run workflow"

## Accessing the Data

### Download from GitHub
The file is always at: `hourly_foot_traffic.parquet`

### Read in Python
```python
import pandas as pd

df = pd.read_parquet('hourly_foot_traffic.parquet')
print(df.head())
print(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
```

### Read in R
```r
library(arrow)
df <- read_parquet('hourly_foot_traffic.parquet')
```

## Files

- `process_traffic_oauth.py` - Main OAuth-based script
- `process_local_files.py` - Alternative: process manually downloaded files
- `requirements.txt` - Python dependencies
- `credentials.json` - OAuth app credentials (commit to private repo)
- `token.pickle` - OAuth authorization token (commit to private repo)
- `hourly_foot_traffic.parquet` - Output data file
- `.github/workflows/daily-traffic.yml` - GitHub Actions automation

## Troubleshooting

**"Token expired" errors:**
- Run `python3 process_traffic_oauth.py` locally once to refresh
- Commit the updated `token.pickle`
- Push to GitHub

**No new data:**
- Check if VemCount emails are still arriving
- Verify emails contain "Hourly Foot Traffic" in subject
- Check GitHub Actions logs for errors

**Authorization issues:**
- Delete `token.pickle`
- Run `python3 process_traffic_oauth.py` to re-authorize
- Commit new token

## Why This Works (vs Other Solutions)

✅ **OAuth instead of App Passwords** - Works with G Suite/Workspace  
✅ **Gmail API instead of IMAP** - Handles forwarded emails properly  
✅ **Token-based** - No passwords in GitHub secrets  
✅ **Auto-refresh** - Token renews automatically  
✅ **Fully automated** - Zero manual work after setup  

## Maintenance

**None required!** Once set up, this runs automatically forever. The OAuth token automatically refreshes, so you never need to re-authorize.
