# Statistical Validation Guide

This document explains how to verify that the statistical calculations in the dashboard are correct.

## Quick Validation

Run the validation script:
```bash
python3 scripts/validate_statistics.py
```

This will check:
- ✅ Date parsing logic
- ✅ Last 30 days calculation
- ✅ Year-over-year comparison
- ✅ Pattern matching for crime categories
- ✅ Most common crimes calculation
- ✅ Weekly aggregation for YoY chart
- ✅ Generated CSV files

## Key Calculations to Verify

### 1. Last 30 Days Calculation

**Formula**: From `latest_date - 29 days` to `latest_date` (inclusive)

**JavaScript Logic** (`script.js`):
```javascript
function last30(){
  const end = latestDateFrom();
  const start = new Date(end); 
  start.setDate(start.getDate()-29);  // 29 days back = 30 days total
  return {start,end};
}
```

**Python Logic** (`generateChartData.py`):
```python
latest_date = df["Date From Parsed"].max()
start_date_30d = latest_date - timedelta(days=29)
```

**Verification**: 
- Should be exactly 30 days (29 days difference + 1 = 30 total days)
- Check that the start date is 29 days before the latest date
- All dates in range should be included (inclusive)

### 2. Year-over-Year Comparison

**Formula**: Compare last 30 days to the same 30-day period exactly 365 days prior

**JavaScript Logic**:
```javascript
const startCurr = addDays(latest, -29);  // Last 30 days start
const endCurr = latest;                   // Last 30 days end

const startPrev = new Date(startCurr);
startPrev.setFullYear(startPrev.getFullYear() - 1);  // Same dates, previous year

const endPrev = new Date(endCurr);
endPrev.setFullYear(endPrev.getFullYear() - 1);
```

**Verification**:
- Current period: `(latest - 29 days)` to `latest`
- Previous period: `(latest - 29 days - 365 days)` to `(latest - 365 days)`
- Both periods should be exactly 30 days
- Percentage: `((current - previous) / previous) * 100`

### 3. Pattern Matching

**Property Crimes**: 
- "THEFT -" (with space before hyphen)
- "BURGLARY -"
- "ARSON -"
- "VANDALISM -"
- "TRESPASS -"

**Violent Crimes**:
- "ROBBERY -"
- "ASSAULT -"
- "BATTERY -"
- "RAPE" (includes "SEX OFFENSE - Rape")
- "MURDER"
- "HOMICIDE"
- "KIDNAPPING -"
- "CARJACKING"

**Note**: The pattern matching is case-insensitive and uses substring matching (`.includes()`).

**Potential Issues**:
- "THEFT-PETTY" (no space) might not match "THEFT -" (with space)
- Check if data uses consistent formatting
- Some offenses might use hyphens vs spaces differently

### 4. Weekly Aggregation (YoY Chart)

**Week Definition**: Monday to Sunday (7 days)

**Logic**:
1. Find the Monday of the week for each incident date
2. Group incidents by week start date (Monday)
3. Count property vs violent crimes per week

**Verification**:
- All weeks should start on Monday
- Each week should span exactly 7 days
- Latest week might be incomplete (if current date < Sunday of that week)

### 5. Most Common Crimes

**Calculation**: Count incidents in last 30 days for each crime type

**Query Matching**: Uses the same `parseQuery` and `evaluateRow` logic as the search function

**Verification**:
- Should match the search results when using the same queries
- Manual spot-check: Search for a specific crime type and verify the count matches

## Manual Verification Steps

### Step 1: Verify Date Ranges
1. Open the dashboard
2. Check the "Data available from X to Y" note
3. Verify the latest date matches the most recent incident
4. Calculate: latest date - 29 days should match the start of the 30-day period

### Step 2: Spot-Check Crime Counts
1. Use the search function with a specific query (e.g., `in:(Offense) "THEFT -"`)
2. Apply a date filter for the last 30 days
3. Count the results manually
4. Compare with the chart value

### Step 3: Verify YoY Percentage
1. Calculate manually:
   - Current period: Count incidents from (latest - 29 days) to latest
   - Previous period: Count incidents from (latest - 29 days - 365) to (latest - 365)
   - Percentage: `((current - previous) / previous) * 100`
2. Compare with dashboard headline

### Step 4: Check Pattern Matching
1. Look at actual offense strings in the data
2. Verify they match the patterns (check for spaces, hyphens, case)
3. Test edge cases (e.g., "THEFT-PETTY" vs "THEFT -")

## Known Issues & Edge Cases

### 1. Date Format Ambiguity
- 2-digit years: `50` could be `1950` or `2050`
- Current logic: `< 50` → `20XX`, `>= 50` → `19XX`
- This is fine for current data (2023-2025), but might need adjustment for future dates

### 2. Pattern Matching Variations
- Some offenses might use "THEFT-PETTY" (no space) vs "THEFT -" (with space)
- Current patterns require the space before hyphen
- Check actual data format to ensure patterns match

### 3. Week Boundaries
- Weeks start on Monday
- If the latest date is before Sunday of that week, the week is incomplete
- The chart should show this week as transparent/partial

### 4. Time Zone
- Dates are parsed as-is (no timezone conversion)
- Ensure all dates are in the same timezone (likely Pacific/UTC)

## Comparing Python vs JavaScript

The Python script (`generateChartData.py`) should produce the same results as the JavaScript dashboard. To verify:

1. Run `python3 scripts/generateChartData.py`
2. Check the generated CSV files
3. Compare numbers with the dashboard
4. If they differ, check:
   - Date parsing logic
   - Pattern matching logic
   - Date range calculations

## Debugging Tips

1. **Add console logs** in JavaScript:
   ```javascript
   console.log('Latest date:', latest);
   console.log('30-day range:', start, 'to', end);
   console.log('Count in range:', count);
   ```

2. **Print intermediate values** in Python:
   ```python
   print(f"Latest date: {latest_date}")
   print(f"30-day start: {start_date_30d}")
   print(f"Count: {len(df_30d)}")
   ```

3. **Manual spot-check**: Pick a few specific incidents and verify they're included/excluded correctly

4. **Compare totals**: The sum of all crime categories should equal (or be less than) the total incidents in the period (some incidents might not match any category)

## Next Steps

If you find discrepancies:
1. Check the validation script output
2. Compare Python vs JavaScript results
3. Verify date parsing for edge cases
4. Check pattern matching for actual offense strings
5. Review date range calculations

