import pandas as pd
from datetime import datetime, timedelta
import re
from pathlib import Path

# --- CONFIG ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SOURCE_FILE = DATA_DIR / "usc_crime_logs.csv"
OUTPUT_FILE = DATA_DIR / "yoyTrendChart.csv"

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

def clean_date_text(date_str):
    """Remove weekday suffix and keep only MM/DD/YY or MM/DD/YYYY."""
    if pd.isna(date_str):
        return None
    s = str(date_str).strip()
    match = re.match(r"(\d{1,2}/\d{1,2}/\d{2,4})", s)
    return match.group(1) if match else None

def parse_date(date_str):
    """Convert cleaned string to datetime."""
    s = clean_date_text(date_str)
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None

def categorize_offense(offense_text):
    """Return 'Property' or 'Violent' using JS-style pattern matching."""
    if pd.isna(offense_text):
        return None
    text = str(offense_text).upper().strip()

    if any(p in text for p in PROPERTY_PATTERNS):
        return "Property"
    elif any(p in text for p in VIOLENT_PATTERNS):
        return "Violent"
    return None

def main():
    # Ensure data directory exists
    DATA_DIR.mkdir(exist_ok=True)
    
    # Load CSV
    df = pd.read_csv(SOURCE_FILE)

    # Parse and clean dates
    df["Date From Parsed"] = df["Date From"].apply(parse_date)
    df = df.dropna(subset=["Date From Parsed"])
    df["Date From Parsed"] = pd.to_datetime(df["Date From Parsed"])

    if df["Date From Parsed"].empty:
        print("❌ No valid dates found.")
        return

    # Define the 13-month window
    latest_date = df["Date From Parsed"].max()
    start_date = latest_date - timedelta(days=30 * 13)

    # Filter to last 13 months
    df = df[(df["Date From Parsed"] >= start_date) & (df["Date From Parsed"] <= latest_date)]

    # Categorize offenses
    df["Category"] = df["Offense"].apply(categorize_offense)
    df = df[df["Category"].notna()].copy()

    # Determine week start (Monday)
    df["Week"] = df["Date From Parsed"] - pd.to_timedelta(df["Date From Parsed"].dt.weekday, unit="d")
    df["Week"] = df["Week"].dt.strftime("%Y-%m-%d")

    # Aggregate weekly totals
    counts = df.groupby(["Week", "Category"]).size().unstack(fill_value=0)

    # Ensure both columns exist
    for col in ["Property", "Violent"]:
        if col not in counts.columns:
            counts[col] = 0

    # Prepare final DataFrame
    result = counts.reset_index()[["Week", "Property", "Violent"]]
    result.columns = ["Week", "Property crimes", "Violent crimes"]
    result = result.sort_values("Week")

    # Save as CSV (overwrite each run)
    result.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Wrote {len(result)} weekly rows to {OUTPUT_FILE}")
    print(result.tail())

if __name__ == "__main__":
    main()

