# USC Crime Dashboard

A web dashboard and data collection system for tracking USC Department of Public Safety (DPS) crime logs.

## Overview

This project:
- Scrapes daily PDF crime logs from USC DPS website
- Parses and stores incident data in CSV and JSON formats
- Generates year-over-year trend charts
- Provides an interactive web dashboard with visualizations and searchable incident logs
- **Automatically updates Datawrapper chart titles** when data changes

## Project Structure

```
usc-crime/
├── scripts/
│   ├── daily-log.py              # Main script to fetch and parse PDFs
│   ├── makeYoyTrendChart.py      # Generates year-over-year trend data (legacy)
│   ├── generateChartData.py      # Generates all chart CSV files + headlines
│   └── updateDatawrapper.py      # Updates Datawrapper chart titles via API
├── web/
│   ├── index.html                # Web dashboard
│   ├── styles.css                # Dashboard styles
│   ├── script.js                 # Dashboard JavaScript
│   └── favicon.png               # Site icon
├── data/                          # Generated data files (gitignored)
│   ├── usc_crime_logs.csv        # Full incident data (CSV)
│   ├── usc_crime_logs.json      # Full incident data (JSON)
│   ├── yoyTrendChart.csv         # Weekly aggregated trend data (property vs violent)
│   ├── mostCommonCrimes.csv      # Most common crimes (last 30 days)
│   ├── bikeThefts.csv            # Bike/scooter thefts (monthly, last 12 months)
│   ├── partyIncidents.csv         # Party-related incidents (last 30 days)
│   └── headlines.json            # HTML-formatted headlines for Datawrapper
├── requirements.txt              # Python dependencies
├── datawrapper_config.json.example  # Example config for Datawrapper chart IDs
└── README.md                     # This file
```

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the data collection script:
```bash
python scripts/daily-log.py
```

This will:
- Check for existing data in `data/usc_crime_logs.csv`
- Fetch PDFs from the latest date in your data through today
- Parse incidents and append new ones (deduplicated by Event #)
- Output both CSV and JSON formats to the `data/` folder

3. Generate all chart data files:
```bash
python scripts/generateChartData.py
```

This generates CSV files for all dashboard charts:
- `data/yoyTrendChart.csv` - Weekly property vs violent crimes
- `data/mostCommonCrimes.csv` - Most common crimes (last 30 days)
- `data/bikeThefts.csv` - Monthly bike/scooter thefts (last 12 months)
- `data/partyIncidents.csv` - Party-related incidents (last 30 days)
- `data/headlines.json` - HTML-formatted headlines for Datawrapper

4. Open `web/index.html` in a web browser to view the dashboard.

**Note:** These CSV files are designed to be used with Datawrapper or other charting tools that can pull from GitHub repositories.

## Datawrapper Integration

The project can automatically update Datawrapper chart titles when data changes. This requires:

1. **Datawrapper API Token**: Get one from [Datawrapper Account Settings](https://app.datawrapper.de/account/api-tokens)
2. **Chart IDs**: Find these in your Datawrapper chart URLs (e.g., `https://datawrapper.dwcdn.net/abc123/` → chart ID is `abc123`)

### Setup Options

**Option 1: Local config file** (for local development)
```bash
cp datawrapper_config.json.example datawrapper_config.json
# Edit datawrapper_config.json with your chart IDs
export DATAWRAPPER_API_TOKEN="your_token_here"
python scripts/generateChartData.py
```

**Option 2: Environment variables** (for GitHub Actions)
Add these secrets to your GitHub repository:
- `DATAWRAPPER_API_TOKEN` - Your API token
- `DATAWRAPPER_CHART_MOSTCOMMON` - Chart ID for most common crimes
- `DATAWRAPPER_CHART_BIKETHEFTS` - Chart ID for bike thefts
- `DATAWRAPPER_CHART_PARTIES` - Chart ID for party incidents
- `DATAWRAPPER_CHART_YOYTREND` - Chart ID for year-over-year trend

The GitHub Actions workflow will automatically update chart titles when data changes.

### CSV URLs for Datawrapper

Use these GitHub raw URLs when importing CSV data in Datawrapper:

- **Most Common Crimes**: `https://raw.githubusercontent.com/tomo-chien/usc-crime/main/data/mostCommonCrimes.csv`
- **Bike Thefts**: `https://raw.githubusercontent.com/tomo-chien/usc-crime/main/data/bikeThefts.csv`
- **Party Incidents**: `https://raw.githubusercontent.com/tomo-chien/usc-crime/main/data/partyIncidents.csv`
- **Year-over-Year Trend**: `https://raw.githubusercontent.com/tomo-chien/usc-crime/main/data/yoyTrendChart.csv`

## Features

### Dashboard
- **Year-over-year trends**: Weekly comparison of property vs violent crimes
- **Most common crimes**: Bar chart of top crime types (last 30 days)
- **Bike/scooter thefts**: Monthly trends over the past year
- **Party-related incidents**: Noise complaints, party shutdowns, overdoses

### All Incidents Tab
- Searchable, filterable table of all incidents
- Advanced search with column filters, date ranges, and boolean logic
- Mobile-responsive design

### Advanced Resources
- Download full datasets (CSV/JSON)
- Search syntax documentation

## Data Collection

The `scripts/daily-log.py` script:
- Fetches PDFs from: `https://dps.usc.edu/wp-content/uploads/{year}/{month}/{mmddyy}.pdf`
- Uses parallel processing (12 workers) for faster fetching
- Deduplicates by Event # to avoid duplicates
- Preserves existing data and only adds new incidents
- Automatically creates the `data/` directory if it doesn't exist

## Automation

The project includes GitHub Actions that:
- Run every 10 minutes to fetch new crime logs
- Generate updated chart data and headlines
- Automatically update Datawrapper chart titles (if configured)
- Commit and push changes to the repository

## Notes

- Data files in `data/` are generated and should not be manually edited
- The script will start from 2023-12-04 if no existing data is found
- PDFs are fetched incrementally from the latest date in your data through today
- If you have existing data files in the root directory, move them to `data/` to continue using them
