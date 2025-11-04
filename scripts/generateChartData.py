#!/usr/bin/env python3
"""
Generate CSV files for all dashboard charts.
Replicates the keyword matching logic from the JavaScript dashboard.
"""

import pandas as pd
import re
import json
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


# Column name mapping (matching JS colMap)
COL_MAP = {
    "date reported": "Date Reported",
    "event #": "Event #",
    "case #": "Case #",
    "offense": "Offense",
    "initial incident": "Initial Incident",
    "final incident": "Final Incident",
    "date from": "Date From",
    "date to": "Date To",
    "location": "Location",
    "disposition": "Disposition",
    "url": "URL"
}

# Most common queries (matching JS exactly)
MOST_COMMON_QUERIES_JS = {
    "Assault": 'in:(Offense) "ASSAULT -" or "ASSAULT-"',
    "Battery": 'in:(Offense) "BATTERY -" or "BATTERY-"',
    "Rape": 'in:(Offense) "SEX OFFENSE - Rape" or "SEX OFFENSE-Rape"',
    "Robbery": 'in:(Offense) "ROBBERY -" or "ROBBERY-"',
    "Kidnapping": 'in:(Offense) "KIDNAPPING -"',
    "Theft": 'in:(Offense) "THEFT-PETTY -" or "THEFT-MOTOR VEHICLE -" or "THEFT-TRICK -" or "THEFT-GRAND -"',
    "Motor vehicle theft": 'in:(Offense) "MOTOR VEHICLE THEFT -"',
    "Burglary": 'in:(Offense) "BURGLARY -" or "BURGLARY-"',
    "Arson": 'in:(Offense) "ARSON -"',
    "Vandalism": 'in:(Offense) "VANDALISM -"',
    "Trespassing": 'in:(Offense) "TRESPASS -"'
}

# Bike query (matching JS exactly)
BIKE_QUERY_JS = 'in:(Offense) "MOTOR VEHICLE THEFT - Theft of Motorized Bicycle/Scooter" or "THEFT-PETTY - Theft Bicycle"'

# Party queries (matching JS exactly)
PARTY_QUERIES_JS = {
    "Party shut down": 'in:(Final Incident) "Party/Event Shut Down"',
    "Noise complaint": 'in:(Final Incident) "Loud and Raucous Noise"',
    "Alcohol Overdose": 'in:(Final Incident) "Alcohol Overdose"',
    "Drug Overdose": 'in:(Final Incident) "Drug Overdose"'
}


def match_value(cell, val_string):
    """Match value in cell (matching JS matchValue function)."""
    cell_val = (str(cell) if cell else "").lower()
    if val_string.startswith('"') and val_string.endswith('"'):
        val = val_string[1:-1].lower()
        return val in cell_val
    else:
        words = val_string.split()
        return any(w.lower() in cell_val for w in words)


def evaluate_row_js(row, tokens):
    """Evaluate row against query tokens (matching JS evaluateRow function exactly)."""
    result = None
    current_op = "AND"
    last_column = None
    
    for t in tokens:
        token = t.lower()
        if token == "and":
            current_op = "AND"
            continue
        if token == "or":
            current_op = "OR"
            continue
        
        condition = False
        
        if token.startswith("in:("):
            # Parse in:(Column) "value"
            import re
            m = re.match(r'in:\(([^)]+)\)\s*(.+)', token)
            if m:
                col_name = m.group(1).strip().lower()
                last_column = COL_MAP.get(col_name)
                val_str = m.group(2).strip()
                if last_column:
                    condition = match_value(row.get(last_column, ""), val_str)
        elif last_column:
            condition = match_value(row.get(last_column, ""), token)
        else:
            # Match across all columns
            if token.startswith('"') and token.endswith('"'):
                val = token[1:-1].lower()
                condition = any(val in str(v).lower() for v in row.values() if v)
            else:
                words = token.split()
                condition = any(
                    any(w.lower() in str(v).lower() for v in row.values() if v)
                    for w in words
                )
        
        if result is None:
            result = condition
        elif current_op == "AND":
            result = result and condition
        elif current_op == "OR":
            result = result or condition
    
    return result if result is not None else False


def parse_query_js(query):
    """Parse query string into tokens (matching JS parseQuery function)."""
    import re
    # Match: from:(...), after:..., before:..., in:(...) "...", in:(...) ..., "...", and, or, or words
    pattern = r'from:\(\d{1,2}/\d{1,2}/\d{4}\s+to\s+\d{1,2}/\d{1,2}/\d{4}\)|after:\d{1,2}/\d{1,2}/\d{4}|before:\d{1,2}/\d{1,2}/\d{4}|in:\([^)]+\)\s+"[^"]+"|in:\([^)]+\)\s+[^\s]+|"(.*?)"|and|or|[^\s]+'
    matches = re.findall(pattern, query, re.IGNORECASE)
    return [m if isinstance(m, str) else m[0] if m[0] else m for m in matches]


def generate_headlines(df):
    """Generate HTML-formatted headlines for Datawrapper charts."""
    df["Date From Parsed"] = df["Date From"].apply(parse_date)
    df = df.dropna(subset=["Date From Parsed"])
    
    if df.empty:
        return {}
    
    latest_date = df["Date From Parsed"].max()
    start_date_30d = latest_date - timedelta(days=29)
    
    # Convert to dict format for evaluation (matching JS structure)
    df_dict = df.to_dict('records')
    
    # Filter to last 30 days
    df_30d = [
        row for row in df_dict
        if row.get("Date From Parsed") and 
        start_date_30d <= row["Date From Parsed"] <= latest_date
    ]
    
    headlines = {}
    
    # 1. Most Common Crimes headline (using JS query evaluation)
    counts = {}
    for label, query in MOST_COMMON_QUERIES_JS.items():
        tokens = parse_query_js(query)
        count = sum(1 for row in df_30d if evaluate_row_js(row, tokens))
        counts[label] = count
    
    max_count = max(counts.values()) if counts else 0
    leaders = [label for label, count in counts.items() if count == max_count] if counts else []
    
    if leaders:
        # Lowercase first letter (matching JS lcFirst function)
        leader_text = leaders[0][0].lower() + leaders[0][1:] if len(leaders[0]) > 0 else leaders[0].lower()
        if len(leaders) > 1:
            leader_text += " (tie)"
        headlines["mostCommon"] = f'In the last 30 days, the most common crime was <span style="color: #ac2124; font-weight: 900;">{leader_text}</span>.'
    else:
        headlines["mostCommon"] = "No data available."
    
    # 2. Bike Thefts headline (using JS query evaluation)
    bike_tokens = parse_query_js(BIKE_QUERY_JS)
    bike_count = sum(1 for row in df_30d if evaluate_row_js(row, bike_tokens))
    headlines["bikeThefts"] = f'There were <span style="color: #ac2124; font-weight: 900;">{bike_count}</span> bikes, scooters, and skateboards reported stolen.'
    
    # 3. Party Incidents headline (using JS query evaluation)
    party_counts = {}
    for label, query in PARTY_QUERIES_JS.items():
        tokens = parse_query_js(query)
        count = sum(1 for row in df_30d if evaluate_row_js(row, tokens))
        party_counts[label] = count
    
    shut_down = party_counts.get("Party shut down", 0)
    noise = party_counts.get("Noise complaint", 0)
    party_plural = "party" if shut_down == 1 else "parties"
    noise_plural = "noise complaint" if noise == 1 else "noise complaints"
    headlines["parties"] = f'DPS shut down <span style="color: #ac2124; font-weight: 900;">{shut_down}</span> {party_plural} and logged <span style="color: #ac2124; font-weight: 900;">{noise}</span> {noise_plural}.'
    
    # 4. Year-over-year headline (using JS query evaluation with lowercase search)
    prop_tokens = parse_query_js('in:(Offense) "theft" or "burglary" or "arson" or "vandalism" or "trespass"')
    viol_tokens = parse_query_js('in:(Offense) "robbery" or "assault" or "battery" or "rape" or "murder" or "homicide" or "kidnapping" or "carjacking"')
    
    def is_reported(row):
        return evaluate_row_js(row, prop_tokens) or evaluate_row_js(row, viol_tokens)
    
    start_prev = start_date_30d - timedelta(days=365)
    end_prev = latest_date - timedelta(days=365)
    
    curr_total = sum(1 for row in df_30d if is_reported(row))
    prev_total = sum(1 for row in df_dict 
                    if row.get("Date From Parsed") and 
                    start_prev <= row["Date From Parsed"] <= end_prev and
                    is_reported(row))
    
    if prev_total > 0:
        pct = round(((curr_total - prev_total) / prev_total) * 100)
        word = "up" if pct >= 0 else "down"
        headlines["yoyTrend"] = f'Reported crimes are <span style="font-weight: 800;">{word}</span> <span style="color: #ac2124; font-weight: 900;">{abs(pct)}%</span> compared to this time last year.'
    else:
        headlines["yoyTrend"] = "Not enough data for a year-over-year comparison."
    
    return headlines


def save_headlines(headlines):
    """Save headlines to JSON file."""
    output_file = DATA_DIR / "headlines.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(headlines, f, indent=2)
    print(f"✅ Generated {output_file}")


def update_datawrapper_charts(headlines):
    """Update Datawrapper chart titles via API (optional)."""
    try:
        import sys
        from pathlib import Path
        script_dir = Path(__file__).parent
        sys.path.insert(0, str(script_dir))
        from updateDatawrapper import update_all_charts, get_api_token
        api_token = get_api_token()
        if api_token:
            update_all_charts(headlines, api_token)
    except ImportError:
        # Module not found, skip
        pass
    except Exception as e:
        print(f"⚠️  Datawrapper update failed: {e}")


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
    
    print("\n📝 Generating headlines...")
    headlines = generate_headlines(df.copy())
    save_headlines(headlines)
    
    # Optionally update Datawrapper charts via API
    update_datawrapper_charts(headlines)
    
    print("\n✨ All chart CSVs and headlines generated successfully!")


if __name__ == "__main__":
    main()

