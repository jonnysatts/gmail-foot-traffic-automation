# Gmail Foot Traffic Automation

Automated pipeline that extracts hourly foot traffic data from VemCount emails and stores it in a Parquet file.

## Features

- 🔄 **Daily automated updates** via GitHub Actions (runs at 7 AM Melbourne time)
- 📊 **Historical backfill** capability (12+ months of data)
- 💾 **Parquet storage** - efficient columnar format
- 🏢 **Dual venue tracking** - Melbourne and Sydney
- 📈 **Two metrics** - "Entering" and "Inside" counts
- ⏰ **Operating hours tracking** - marks which hours venues are open

## Data Schema

The Parquet file contains the following columns:

- `DateTime`: Timestamp of the hour
- `Date`: Date (without time)
- `Hour`: Hour of day (0-26, allowing for late hours)
- `Venue`: "Melbourne" or "Sydney"
- `Entering`: Number of people entering (with 0.95 multiplier applied)
- `Inside`: Number of people inside
- `IsOpen`: Boolean indicating if venue was open during this hour

## Setup

### 1. Gmail App Password

1. Visit https://myaccount.google.com/apppasswords
2. Enable 2-factor authentication if not already enabled
3. Create app password named "GitHub Actions Traffic Bot"
4. Save the 16-character password

### 2. GitHub Secrets

Add these secrets to your repository (Settings → Secrets → Actions):

- `GMAIL_USER`: Your Gmail address
- `GMAIL_APP_PASSWORD`: The 16-character app password from step 1

### 3. Run Historical Backfill

To populate historical data (12 months):

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export GMAIL_USER="your-email@gmail.com"
export GMAIL_APP_PASSWORD="your-app-password"

# Run backfill (looks back 365 days)
python process_traffic.py --backfill 365
```

### 4. Commit Initial Data

```bash
git add hourly_foot_traffic.parquet
git commit -m "Initial historical data backfill"
git push
```

## Daily Automation

After setup, GitHub Actions will:
1. Run daily at 7 AM Melbourne time
2. Check Gmail for new traffic emails
3. Process any new data
4. Update the Parquet file
5. Commit and push changes automatically

You can also manually trigger a run from the "Actions" tab in GitHub.

## Manual Usage

```bash
# Daily update (searches last 30 days)
python process_traffic.py

# Custom backfill period
python process_traffic.py --backfill 90

# Historical full backfill
python process_traffic.py --backfill 365
```

## File Structure

```
.
├── process_traffic.py           # Main processing script
├── requirements.txt              # Python dependencies
├── hourly_foot_traffic.parquet  # Output data file
├── .github/
│   └── workflows/
│       └── daily-traffic.yml    # GitHub Actions workflow
└── README.md                     # This file
```

## Venue Operating Hours

**Melbourne & Sydney:**
- Mon-Thu: 12:00 PM - 11:00 PM
- Fri: 12:00 PM - 12:00 AM (midnight)
- Sat: 12:00 PM - 1:00 AM
- Sun: 12:00 PM - 11:00 PM

## Accessing the Data

### Download from GitHub
The Parquet file is available directly in the repository at:
`hourly_foot_traffic.parquet`

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
head(df)
```

## Troubleshooting

**No data found:**
- Check that emails from `no-reply@vemcount.com` are in your inbox
- Verify the email contains an Excel attachment with "Traffic By Hour" in the filename

**Authentication failed:**
- Ensure Gmail app password is correct
- Check that 2-factor authentication is enabled
- Try regenerating the app password

**Workflow not running:**
- Check GitHub Actions is enabled for your repository
- Verify secrets are set correctly
- Check the Actions tab for error logs
