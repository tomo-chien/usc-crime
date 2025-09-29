# backfill.py — incremental updater for USC DPS daily PDFs
# - Reads usc_crime_logs.csv to find the latest date
# - Checks one PDF per day from the NEXT day through TODAY
# - Adds ONLY new rows (by unique Event #) to CSV and JSON, newest-first
# - Prints:
#     • whether the CSV was found (and its path)
#     • the latest date detected in the CSV
#
# Requirements:
#   pip install requests pdfplumber

import requests
import pdfplumber
import csv
import io
import json
import re
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple, Optional

# --------------------------------------------------------------------
# Configuration (point explicitly to root-level files)
# --------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
CSV_FILE = BASE_DIR / "usc_crime_logs.csv"
JSON_FILE = BASE_DIR / "usc_crime_logs.json"

# If no CSV exists yet, earliest date to backfill from:
EARLIEST_DATE = date(2023, 12, 4)

BASE_URL = "https://dps.usc.edu/wp-content/uploads/{year}/{month:02d}/{mmddyy}.pdf"

HEADERS = [
    "Date Reported", "Event #", "Case #",
    "Offense", "Initial Incident", "Final Incident",
    "Date From", "Date To", "Location", "Disposition",
    "URL"
]

# Column indexes for convenience
IDX_DATE_REPORTED = 0
IDX_EVENT = 1
IDX_DATE_FROM = 6
IDX_DATE_TO = 7

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------

DATE_RE = re.compile(r'(\d{1,2}/\d{1,2}/\d{2,4})')

def parse_mmddyy_or_yyyy(s: str) -> Optional[date]:
    """Parse MM/DD/YY or MM/DD/YYYY; return None on failure."""
    s = s.strip()
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    # Lenient fallback via regex capture
    m = DATE_RE.search(s)
    if m:
        mm, dd, yyyy = m.group(1).split("/")
        if len(yyyy) == 2:
            yyyy = str(2000 + int(yyyy))
        try:
            return date(int(yyyy), int(mm), int(dd))
        except Exception:
            return None
    return None

def parse_any_date_field(s: str) -> Optional[date]:
    """Extract a date even if extra text/time is present (e.g., '09/05/2024 00:00')."""
    if not s:
        return None
    m = DATE_RE.search(s)
    if not m:
        return None
    return parse_mmddyy_or_yyyy(m.group(1))

def best_row_date(row: List[str]) -> Optional[date]:
    """
    Choose the best available date from a row:
    1) Date Reported
    2) Date From
    3) Date To
    """
    for idx in (IDX_DATE_REPORTED, IDX_DATE_FROM, IDX_DATE_TO):
        if idx < len(row):
            d = parse_any_date_field(row[idx])
            if d:
                return d
    return None

def load_existing_csv() -> Tuple[List[List[str]], set, Optional[date]]:
    """
    Load existing rows from CSV_FILE.
    Returns:
      - rows: list of row lists (without header)
      - event_ids: set of existing Event # values
      - latest_date: max date found among Date Reported / From / To
    """
    if not CSV_FILE.exists():
        return [], set(), None

    rows: List[List[str]] = []
    event_ids = set()
    latest: Optional[date] = None

    with CSV_FILE.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        _header = next(reader, None)  # skip header if present
        for row in reader:
            if not row:
                continue
            rows.append(row)
            if len(row) > IDX_EVENT and row[IDX_EVENT]:
                event_ids.add(row[IDX_EVENT])
            d = best_row_date(row)
            if d and (latest is None or d > latest):
                latest = d

    return rows, event_ids, latest

def daterange(start: date, end: date):
    """Inclusive generator from start to end (dates)."""
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def normalize_row(cells: List[str], url: str) -> List[str]:
    """
    Ensure a row matches HEADERS length and is stripped of whitespace.
    Assumes the PDF table columns correspond 1:1 with HEADERS[:-1] (before URL).
    """
    cleaned = [(c or "").strip() for c in cells]
    target = len(HEADERS) - 1
    if len(cleaned) > target:
        cleaned = cleaned[:target]
    while len(cleaned) < target:
        cleaned.append("")
    cleaned.append(url)
    return cleaned

def fetch_and_parse(d: date) -> Tuple[date, List[List[str]]]:
    """
    Fetch the daily PDF for date d and return parsed rows with URL appended.
    Uses line-based table extraction for stability.
    """
    url = BASE_URL.format(year=d.year, month=d.month, mmddyy=d.strftime("%m%d%y"))
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return d, []
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            out_rows: List[List[str]] = []
            for page in pdf.pages:
                table = page.extract_table({
                    "vertical_strategy": "lines",
                    "horizontal_strategy": "lines"
                })
                if not table:
                    continue
                # Skip header row if first row contains "Date Reported"
                start_idx = 0
                if table and table[0]:
                    if any("Date Reported" in (cell or "") for cell in table[0]):
                        start_idx = 1
                for raw in table[start_idx:]:
                    if not raw:
                        continue
                    out_rows.append(normalize_row(raw, url))
            return d, out_rows
    except Exception:
        return d, []

def write_csv(all_rows: List[List[str]]):
    with CSV_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)
        writer.writerows(all_rows)

def write_json(all_rows: List[List[str]]):
    data = [
        {HEADERS[i]: (row[i] if i < len(row) else "") for i in range(len(HEADERS))}
        for row in all_rows
    ]
    with JSON_FILE.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

# --------------------------------------------------------------------
# Main incremental updater
# --------------------------------------------------------------------

def backfill_incremental(workers: int = 12):
    # Print whether CSV exists, and its path
    csv_exists = CSV_FILE.exists()
    print(f"CSV {'found' if csv_exists else 'NOT found'} at: {CSV_FILE.resolve()}")

    # Ensure CSV exists with header if not present
    if not csv_exists:
        with CSV_FILE.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADERS)

    # Load archive and detect latest date
    existing_rows, existing_event_ids, latest_found = load_existing_csv()
    print(f"Latest date in CSV: {latest_found}")

    # Determine the first date to check (day after latest_found)
    if latest_found:
        start_date = latest_found + timedelta(days=1)
    else:
        start_date = EARLIEST_DATE

    today = datetime.today().date()
    if start_date > today:
        return  # up to date

    # Build list of days to check (inclusive)
    dates = list(daterange(start_date, today))

    # Fetch PDFs in parallel
    all_new_rows: List[List[str]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_and_parse, d): d for d in dates}
        for future in as_completed(futures):
            _d, rows = future.result()
            if rows:
                all_new_rows.extend(rows)

    if not all_new_rows:
        return  # nothing new found

    # --- CHANGED: include rows even if Event # is missing ---
    # Keep all rows whose Event # is empty OR not already seen.
    unique_new_rows: List[List[str]] = []
    seen = set(existing_event_ids)  # existing non-empty Event #s
    for row in all_new_rows:
        ev = row[IDX_EVENT] if len(row) > IDX_EVENT else ""
        if ev and ev in seen:
            continue  # duplicate with a real Event # — skip
        unique_new_rows.append(row)
        if ev:
            seen.add(ev)  # track only non-empty Event #s
    # --- end change ---

    if not unique_new_rows:
        return

    # Sort new rows newest-first using the best available date in each row
    today_dt = today
    def row_dt(r: List[str]) -> date:
        return best_row_date(r) or today_dt

    unique_new_rows.sort(key=row_dt, reverse=True)

    # Prepend new rows to existing (assumes existing roughly newest-first)
    combined = unique_new_rows + existing_rows

    # Write outputs
    write_csv(combined)
    write_json(combined)
