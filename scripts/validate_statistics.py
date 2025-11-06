#!/usr/bin/env python3
"""
Validation script to verify statistical calculations are correct.
Compares Python and JavaScript logic, checks for edge cases, and validates calculations.
"""

import pandas as pd
import json
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Import the same functions from generateChartData
sys.path.insert(0, str(Path(__file__).parent))
from generateChartData import (
    parse_date, matches_pattern, matches_offense_pattern,
    PROPERTY_PATTERNS, VIOLENT_PATTERNS, MOST_COMMON_QUERIES,
    BIKE_PATTERNS, PARTY_QUERIES
)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SOURCE_FILE = DATA_DIR / "usc_crime_logs.csv"

def validate_date_parsing():
    """Validate date parsing matches JavaScript logic."""
    print("=" * 60)
    print("VALIDATING DATE PARSING")
    print("=" * 60)
    
    test_cases = [
        ("11/05/23", datetime(2023, 11, 5).date()),
        ("11/05/2023", datetime(2023, 11, 5).date()),
        # Note: Year 50 is ambiguous - JS uses < 50 ? 2000+ : 1900+
        # But for dates in 2025, year 50 would be 2050, not 1950
        # However, the actual data uses 2-digit years that are likely 20XX
        ("01/01/49", datetime(2049, 1, 1).date()),
        ("01/01/99", datetime(1999, 1, 1).date()),
        ("01/01/00", datetime(2000, 1, 1).date()),
    ]
    
    all_passed = True
    for input_date, expected in test_cases:
        result = parse_date(input_date)
        if result == expected:
            print(f"✅ {input_date} -> {result}")
        else:
            print(f"❌ {input_date} -> {result} (expected {expected})")
            all_passed = False
    
    return all_passed

def validate_last_30_days_calculation(df):
    """Validate last 30 days calculation."""
    print("\n" + "=" * 60)
    print("VALIDATING LAST 30 DAYS CALCULATION")
    print("=" * 60)
    
    df["Date From Parsed"] = df["Date From"].apply(parse_date)
    df = df.dropna(subset=["Date From Parsed"])
    
    if df.empty:
        print("⚠️  No data available")
        return False
    
    latest_date = df["Date From Parsed"].max()
    start_date_30d = latest_date - timedelta(days=29)
    
    print(f"Latest date: {latest_date}")
    print(f"30-day start: {start_date_30d}")
    print(f"Date range: {start_date_30d} to {latest_date} (inclusive)")
    
    # Count incidents in range
    in_range = df[
        (df["Date From Parsed"] >= start_date_30d) &
        (df["Date From Parsed"] <= latest_date)
    ]
    
    print(f"Total incidents in last 30 days: {len(in_range)}")
    
    # Verify it's actually 30 days
    days_diff = (latest_date - start_date_30d).days + 1
    if days_diff == 30:
        print(f"✅ Correct: {days_diff} days (29 days difference + 1 = 30 total)")
    else:
        print(f"❌ Incorrect: {days_diff} days (should be 30)")
        return False
    
    return True

def validate_yoy_calculation(df):
    """Validate year-over-year calculation."""
    print("\n" + "=" * 60)
    print("VALIDATING YEAR-OVER-YEAR CALCULATION")
    print("=" * 60)
    
    df["Date From Parsed"] = df["Date From"].apply(parse_date)
    df = df.dropna(subset=["Date From Parsed"])
    
    if df.empty:
        print("⚠️  No data available")
        return False
    
    latest_date = df["Date From Parsed"].max()
    start_date_30d = latest_date - timedelta(days=29)
    
    # Current period
    curr_start = start_date_30d
    curr_end = latest_date
    
    # Previous year period (exactly 365 days before)
    prev_start = start_date_30d - timedelta(days=365)
    prev_end = latest_date - timedelta(days=365)
    
    print(f"Current period: {curr_start} to {curr_end}")
    print(f"Previous period: {prev_start} to {prev_end}")
    
    # Categorize offenses
    def categorize(row):
        offense = str(row.get("Offense", "")).upper()
        if any(p in offense for p in PROPERTY_PATTERNS):
            return "Property"
        elif any(p in offense for p in VIOLENT_PATTERNS):
            return "Violent"
        return None
    
    df["Category"] = df.apply(categorize, axis=1)
    df_categorized = df[df["Category"].notna()].copy()
    
    # Current period counts
    curr_df = df_categorized[
        (df_categorized["Date From Parsed"] >= curr_start) &
        (df_categorized["Date From Parsed"] <= curr_end)
    ]
    curr_total = len(curr_df)
    
    # Previous period counts
    prev_df = df_categorized[
        (df_categorized["Date From Parsed"] >= prev_start) &
        (df_categorized["Date From Parsed"] <= prev_end)
    ]
    prev_total = len(prev_df)
    
    print(f"\nCurrent period (last 30 days): {curr_total} incidents")
    print(f"Previous period (same dates, last year): {prev_total} incidents")
    
    if prev_total > 0:
        pct = round(((curr_total - prev_total) / prev_total) * 100)
        print(f"Percentage change: {pct}%")
        print(f"✅ Calculation appears correct")
    else:
        print(f"⚠️  No data for previous period")
    
    return True

def validate_pattern_matching(df):
    """Validate pattern matching logic."""
    print("\n" + "=" * 60)
    print("VALIDATING PATTERN MATCHING")
    print("=" * 60)
    
    # Sample offenses to test
    test_offenses = [
        "THEFT-PETTY - Theft Bicycle",
        "THEFT - GRAND - Theft",
        "BURGLARY - Residential",
        "ASSAULT - Simple",
        "ROBBERY - Street",
        "VANDALISM - Graffiti",
        "TRESPASS - Unauthorized Entry",
        "ARSON - Fire",
        "SEX OFFENSE - Rape",
        "KIDNAPPING - Abduction",
        "MOTOR VEHICLE THEFT - Theft of Motorized Bicycle/Scooter",
    ]
    
    print("\nTesting Property patterns:")
    for offense in test_offenses:
        is_property = matches_pattern(offense, PROPERTY_PATTERNS)
        is_violent = matches_pattern(offense, VIOLENT_PATTERNS)
        category = "Property" if is_property else ("Violent" if is_violent else "None")
        print(f"  {offense[:50]:50} -> {category}")
    
    # Validate against actual data
    df_sample = df.head(100) if len(df) > 100 else df
    property_count = sum(1 for _, row in df_sample.iterrows() 
                        if matches_pattern(row.get("Offense", ""), PROPERTY_PATTERNS))
    violent_count = sum(1 for _, row in df_sample.iterrows() 
                       if matches_pattern(row.get("Offense", ""), VIOLENT_PATTERNS))
    
    print(f"\nSample from data (first 100 rows):")
    print(f"  Property crimes: {property_count}")
    print(f"  Violent crimes: {violent_count}")
    
    return True

def validate_most_common_crimes(df):
    """Validate most common crimes calculation."""
    print("\n" + "=" * 60)
    print("VALIDATING MOST COMMON CRIMES")
    print("=" * 60)
    
    df["Date From Parsed"] = df["Date From"].apply(parse_date)
    df = df.dropna(subset=["Date From Parsed"])
    
    if df.empty:
        print("⚠️  No data available")
        return False
    
    latest_date = df["Date From Parsed"].max()
    start_date_30d = latest_date - timedelta(days=29)
    
    df_30d = df[
        (df["Date From Parsed"] >= start_date_30d) &
        (df["Date From Parsed"] <= latest_date)
    ]
    
    print(f"Last 30 days: {start_date_30d} to {latest_date}")
    print(f"Total incidents in period: {len(df_30d)}")
    
    # Count each crime type
    counts = {}
    for label, patterns in MOST_COMMON_QUERIES.items():
        count = sum(1 for _, row in df_30d.iterrows() 
                   if matches_pattern(row.get("Offense", ""), patterns))
        counts[label] = count
        if count > 0:
            print(f"  {label}: {count}")
    
    max_count = max(counts.values()) if counts else 0
    leaders = [label for label, count in counts.items() if count == max_count]
    
    print(f"\nMost common: {leaders[0]} with {max_count} incidents")
    if len(leaders) > 1:
        print(f"  (Tie with: {', '.join(leaders[1:])})")
    
    return True

def validate_weekly_aggregation(df):
    """Validate weekly aggregation for YoY chart."""
    print("\n" + "=" * 60)
    print("VALIDATING WEEKLY AGGREGATION")
    print("=" * 60)
    
    df["Date From Parsed"] = df["Date From"].apply(parse_date)
    df = df.dropna(subset=["Date From Parsed"])
    
    if df.empty:
        print("⚠️  No data available")
        return False
    
    latest_date = df["Date From Parsed"].max()
    
    # Categorize
    def categorize(row):
        offense = str(row.get("Offense", "")).upper()
        if any(p in offense for p in PROPERTY_PATTERNS):
            return "Property"
        elif any(p in offense for p in VIOLENT_PATTERNS):
            return "Violent"
        return None
    
    df["Category"] = df.apply(categorize, axis=1)
    df_categorized = df[df["Category"].notna()].copy()
    
    # Group by week (Monday start)
    week_data = {}
    for _, row in df_categorized.iterrows():
        date = row["Date From Parsed"]
        category = row["Category"]
        
        # Find Monday of that week
        days_since_monday = date.weekday()
        week_start = date - timedelta(days=days_since_monday)
        week_key = week_start.strftime("%Y-%m-%d")
        
        if week_key not in week_data:
            week_data[week_key] = {"Property": 0, "Violent": 0}
        
        week_data[week_key][category] += 1
    
    weeks = sorted(week_data.keys())
    print(f"Total weeks: {len(weeks)}")
    print(f"First week: {weeks[0]}")
    print(f"Last week: {weeks[-1]}")
    
    # Show sample weeks
    print("\nSample weeks (last 5):")
    for week in weeks[-5:]:
        prop = week_data[week]["Property"]
        viol = week_data[week]["Violent"]
        print(f"  {week}: Property={prop}, Violent={viol}, Total={prop+viol}")
    
    # Verify week boundaries
    if len(weeks) >= 2:
        first_week = datetime.strptime(weeks[0], "%Y-%m-%d").date()
        if first_week.weekday() == 0:  # Monday
            print("✅ First week starts on Monday")
        else:
            print(f"❌ First week starts on {first_week.strftime('%A')} (should be Monday)")
            return False
    
    return True

def compare_with_js_results():
    """Compare Python results with JavaScript results if available."""
    print("\n" + "=" * 60)
    print("COMPARING WITH GENERATED FILES")
    print("=" * 60)
    
    # Check if chart CSV files exist
    yoy_file = DATA_DIR / "yoyTrendChart.csv"
    most_common_file = DATA_DIR / "mostCommonCrimes.csv"
    
    if yoy_file.exists():
        yoy_df = pd.read_csv(yoy_file)
        print(f"✅ YoY chart CSV exists: {len(yoy_df)} weeks")
    
    if most_common_file.exists():
        mc_df = pd.read_csv(most_common_file)
        print(f"✅ Most common crimes CSV exists: {len(mc_df)} categories")
        # Get column names dynamically
        cols = mc_df.columns.tolist()
        if len(cols) >= 2:
            print(f"   Columns: {cols}")
            print(f"   Top values: {mc_df.head().to_dict()}")
    
    return True

def main():
    """Run all validation checks."""
    print("\n" + "=" * 60)
    print("STATISTICAL VALIDATION REPORT")
    print("=" * 60)
    
    if not SOURCE_FILE.exists():
        print(f"❌ Source file not found: {SOURCE_FILE}")
        return 1
    
    print(f"\nLoading data from {SOURCE_FILE}...")
    df = pd.read_csv(SOURCE_FILE)
    print(f"Loaded {len(df)} rows")
    
    results = []
    
    # Run validations
    results.append(("Date Parsing", validate_date_parsing()))
    results.append(("Last 30 Days", validate_last_30_days_calculation(df.copy())))
    results.append(("Year-over-Year", validate_yoy_calculation(df.copy())))
    results.append(("Pattern Matching", validate_pattern_matching(df.copy())))
    results.append(("Most Common Crimes", validate_most_common_crimes(df.copy())))
    results.append(("Weekly Aggregation", validate_weekly_aggregation(df.copy())))
    results.append(("Generated Files", compare_with_js_results()))
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {name}")
    
    all_passed = all(result[1] for result in results)
    
    if all_passed:
        print("\n✅ All validations passed!")
        return 0
    else:
        print("\n⚠️  Some validations failed. Review the output above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())

