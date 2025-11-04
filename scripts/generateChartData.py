#!/usr/bin/env python3
"""
Generate CSV files for all dashboard charts.
Replicates the keyword matching logic from the JavaScript dashboard.
"""

import pandas as pd
import re
from datetime import datetime, timedelta
from pathlib import Path

# --- CONFIG ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SOURCE_FILE = DATA_DIR / "usc_crime_logs.csv"

# Match JS logic exactly: space before hyphen
PROPERTY_PATTERNS = [
    "THEFT -",
    "BURGLARY -",
    "ARSON -",
    "VANDALISM -",
    "TRESPASS -"
]

VIOLENT_PATTERNS = [
    "ROBBERY -",
    "ASSAULT -",
    "BATTERY -",
    "RAPE",         # covers "SEX OFFENSE - Rape"
    "MURDER",
    "HOMICIDE",
    "KIDNAPPING -",
    "CARJACKING"
]

# Most common crime queries (from JS)
MOST_COMMON_QUERIES = {
    "Assault": ["ASSAULT -", "ASSAULT-"],
    "Battery": ["BATTERY -", "BATTERY-"],
    "Rape": ["SEX OFFENSE - Rape", "SEX OFFENSE-Rape"],
    "Robbery": ["ROBBERY -", "ROBBERY-"],
    "Kidnapping": ["KIDNAPPING -"],
    "Theft": ["THEFT-PETTY -", "THEFT-MOTOR VEHICLE -", "THEFT-TRICK -", "THEFT-GRAND -"],
    "Motor vehicle theft": ["MOTOR VEHICLE THEFT -"],
    "Burglary": ["BURGLARY -", "BURGLARY-"],
    "Arson": ["ARSON -"],
    "Vandalism": ["VANDALISM -"],
    "Trespassing": ["TRESPASS -"]
}

# Bike/scooter theft patterns
BIKE_PATTERNS = [
    "MOTOR VEHICLE THEFT - Theft of Motorized Bicycle/Scooter",
    "THEFT-PETTY - Theft Bicycle"
]

# Party-related queries
PARTY_QUERIES = {
    "Party shut down": ["Party/Event Shut Down"],
    "Noise complaint": ["Loud and Raucous Noise"],
    "Alcohol Overdose": ["Alcohol Overdose"],
    "Drug Overdose": ["Drug Overdose"]
}


def parse_date(date_str):
    """Parse MM/DD/YY or MM/DD/YYYY format, matching JS logic."""
    if pd.isna(date_str):
        return None
    s = str(date_str).strip()
    # Extract date part (MM/DD/YY or MM/DD/YYYY)
    match = re.match(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", s)
    if not match:
        return None
    mm, dd, yy = match.groups()
    if len(yy) == 2:
        year = 2000 + int(yy) if int(yy) < 50 else 1900 + int(yy)
    else:
        year = int(yy)
    try:
        return datetime(year, int(mm), int(dd)).date()
    except:
        return None


def matches_pattern(text, patterns):
    """Check if text contains any of the patterns (case-insensitive, matching JS logic)."""
    if pd.isna(text):
        return False
    text_upper = str(text).upper()
    return any(pattern.upper() in text_upper for pattern in patterns)


def matches_offense_pattern(row, patterns):
    """Check if Offense column matches any pattern."""
    offense = row.get("Offense", "")
    return matches_pattern(offense, patterns)


def matches_final_incident_pattern(row, patterns):
    """Check if Final Incident column matches any pattern."""
    final_incident = row.get("Final Incident", "")
    return matches_pattern(final_incident, patterns)


def generate_most_common_chart(df):
    """Generate CSV for most common crimes (last 30 days)."""
    # Get latest date
    df["Date From Parsed"] = df["Date From"].apply(parse_date)
    df = df.dropna(subset=["Date From Parsed"])
    
    if df.empty:
        print("⚠️  No valid dates for most common chart")
        return
    
    latest_date = df["Date From Parsed"].max()
    start_date = latest_date - timedelta(days=29)
    
    # Filter to last 30 days
    df_30d = df[(df["Date From Parsed"] >= start_date) & (df["Date From Parsed"] <= latest_date)]
    
    # Count each crime type
    results = []
    for label, patterns in MOST_COMMON_QUERIES.items():
        count = df_30d[df_30d.apply(lambda row: matches_offense_pattern(row, patterns), axis=1)].shape[0]
        results.append({
            "Crime Type": label,
            "Count": count
        })
    
    result_df = pd.DataFrame(results)
    result_df = result_df.sort_values("Count", ascending=False)
    
    output_file = DATA_DIR / "mostCommonCrimes.csv"
    result_df.to_csv(output_file, index=False)
    print(f"✅ Generated {output_file} ({len(result_df)} categories)")


def generate_bike_theft_chart(df):
    """Generate CSV for bike/scooter thefts (monthly, last 12 months)."""
    df["Date From Parsed"] = df["Date From"].apply(parse_date)
    df = df.dropna(subset=["Date From Parsed"])
    
    if df.empty:
        print("⚠️  No valid dates for bike theft chart")
        return
    
    latest_date = df["Date From Parsed"].max()
    
    # Filter to bike thefts
    bike_df = df[df.apply(lambda row: matches_offense_pattern(row, BIKE_PATTERNS), axis=1)]
    
    # Generate 12 months of data
    results = []
    # Start from 11 months ago
    first_month = latest_date.replace(day=1)
    for i in range(11, -1, -1):
        # Calculate month by subtracting months
        target_month = first_month.month - i
        target_year = first_month.year
        while target_month <= 0:
            target_month += 12
            target_year -= 1
        month_start = datetime(target_year, target_month, 1).date()
        if month_start.month == 12:
            month_end = (datetime(month_start.year + 1, 1, 1) - timedelta(days=1)).date()
        else:
            month_end = (datetime(month_start.year, month_start.month + 1, 1) - timedelta(days=1)).date()
        
        month_df = bike_df[
            (bike_df["Date From Parsed"] >= month_start) &
            (bike_df["Date From Parsed"] <= month_end)
        ]
        
        month_name = month_start.strftime("%b")
        # Mark current month as MTD if incomplete
        if month_start.year == latest_date.year and month_start.month == latest_date.month:
            if latest_date < month_end:
                month_name += " (MTD)"
        
        results.append({
            "Month": month_name,
            "Thefts": len(month_df)
        })
    
    result_df = pd.DataFrame(results)
    output_file = DATA_DIR / "bikeThefts.csv"
    result_df.to_csv(output_file, index=False)
    print(f"✅ Generated {output_file} ({len(result_df)} months)")


def generate_parties_chart(df):
    """Generate CSV for party-related incidents (last 30 days)."""
    df["Date From Parsed"] = df["Date From"].apply(parse_date)
    df = df.dropna(subset=["Date From Parsed"])
    
    if df.empty:
        print("⚠️  No valid dates for parties chart")
        return
    
    latest_date = df["Date From Parsed"].max()
    start_date = latest_date - timedelta(days=29)
    
    # Filter to last 30 days
    df_30d = df[(df["Date From Parsed"] >= start_date) & (df["Date From Parsed"] <= latest_date)]
    
    # Count each party-related type
    results = []
    for label, patterns in PARTY_QUERIES.items():
        count = df_30d[df_30d.apply(lambda row: matches_final_incident_pattern(row, patterns), axis=1)].shape[0]
        results.append({
            "Incident Type": label,
            "Count": count
        })
    
    result_df = pd.DataFrame(results)
    output_file = DATA_DIR / "partyIncidents.csv"
    result_df.to_csv(output_file, index=False)
    print(f"✅ Generated {output_file} ({len(result_df)} categories)")


def generate_yoy_trend_chart(df):
    """Generate CSV for year-over-year trend (weekly, property vs violent)."""
    df["Date From Parsed"] = df["Date From"].apply(parse_date)
    df = df.dropna(subset=["Date From Parsed"])
    
    if df.empty:
        print("⚠️  No valid dates for YoY trend chart")
        return
    
    latest_date = df["Date From Parsed"].max()
    
    # Categorize offenses
    def categorize(row):
        offense = str(row.get("Offense", "")).upper()
        if any(p in offense for p in PROPERTY_PATTERNS):
            return "Property"
        elif any(p in offense for p in VIOLENT_PATTERNS):
            return "Violent"
        return None
    
    df["Category"] = df.apply(categorize, axis=1)
    df = df[df["Category"].notna()].copy()
    
    # Get 16 months of data (for ~16 weeks visualization)
    first_month = latest_date.replace(day=1) - timedelta(days=30 * 15)
    
    # Generate weekly data
    results = []
    current_week_start = first_month
    
    # Find first Monday
    while current_week_start.weekday() != 0:  # 0 = Monday
        current_week_start += timedelta(days=1)
    
    # Generate weeks up to latest date
    while current_week_start <= latest_date:
        week_end = current_week_start + timedelta(days=6)
        
        week_df = df[
            (df["Date From Parsed"] >= current_week_start) &
            (df["Date From Parsed"] <= min(week_end, latest_date))
        ]
        
        property_count = len(week_df[week_df["Category"] == "Property"])
        violent_count = len(week_df[week_df["Category"] == "Violent"])
        
        # Format week label
        week_label = current_week_start.strftime("%Y-%m-%d")
        
        results.append({
            "Week Start": week_label,
            "Property": property_count,
            "Violent": violent_count
        })
        
        current_week_start += timedelta(days=7)
    
    result_df = pd.DataFrame(results)
    output_file = DATA_DIR / "yoyTrendChart.csv"
    result_df.to_csv(output_file, index=False)
    print(f"✅ Generated {output_file} ({len(result_df)} weeks)")


def main():
    """Generate all chart CSV files."""
    DATA_DIR.mkdir(exist_ok=True)
    
    if not SOURCE_FILE.exists():
        print(f"❌ Source file not found: {SOURCE_FILE}")
        return
    
    print(f"📊 Loading data from {SOURCE_FILE}...")
    df = pd.read_csv(SOURCE_FILE)
    print(f"   Loaded {len(df)} rows")
    
    print("\n🔨 Generating chart data...")
    generate_most_common_chart(df.copy())
    generate_bike_theft_chart(df.copy())
    generate_parties_chart(df.copy())
    generate_yoy_trend_chart(df.copy())
    
    print("\n✨ All chart CSVs generated successfully!")


if __name__ == "__main__":
    main()

