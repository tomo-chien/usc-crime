import requests
import pdfplumber
import io
import datetime
import csv
import json
from pathlib import Path

# === CONFIG ===
OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(exist_ok=True)

CSV_FILE = OUTPUT_DIR / "usc_log.csv"
JSON_FILE = OUTPUT_DIR / "usc_log.json"

HEADERS = [
    "Date Reported", "Event #", "Case #",
    "Offense", "Initial Incident", "Final Incident",
    "Date From", "Date To", "Location", "Disposition",
    "URL"  # added URL column
]

def get_log_url():
    today = datetime.date.today()
    yyyy = today.year
    mm = f"{today.month:02d}"
    dd = f"{today.day:02d}"
    yy = str(today.year)[-2:]
    filename = f"{mm}{dd}{yy}.pdf"
    return f"https://dps.usc.edu/wp-content/uploads/{yyyy}/{mm}/{filename}"

def fetch_log_rows():
    url = get_log_url()
    print(f"Fetching {url}")
    response = requests.get(url)

    # Gracefully handle missing logs
    if response.status_code != 200:
        print(f"No log found for today (status {response.status_code}). Exiting.")
        return []

    pdf_file = io.BytesIO(response.content)
    rows = []

    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if table:
                for row in table:
                    if row[0] and row[0].startswith("Date Reported"):
                        continue
                    cleaned = [cell.strip() if cell else "" for cell in row]
                    cleaned.append(url)  # append URL column
                    rows.append(cleaned)
    return rows

def load_existing():
    existing = []
    if CSV_FILE.exists():
        with open(CSV_FILE, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header
            existing = [row for row in reader]
    return existing

def save_csv(all_rows):
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)
        writer.writerows(all_rows)

def save_json(all_rows):
    data = [
        {HEADERS[i]: row[i] if i < len(row) else "" for i in range(len(HEADERS))}
        for row in all_rows
    ]
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def main():
    new_rows = fetch_log_rows()
    if not new_rows:
        return  # exit gracefully, no file changes

    existing_rows = load_existing()
    existing_event_ids = {row[1] for row in existing_rows}  # Event # column

    # Only keep new unique rows
    unique_new_rows = [row for row in new_rows if row[1] not in existing_event_ids]

    if not unique_new_rows:
        print("No new rows to add.")
        return

    # Prepend new rows to the archive
    combined = unique_new_rows + existing_rows

    save_csv(combined)
    save_json(combined)

    print(f"Prepended {len(unique_new_rows)} new rows. Total now {len(combined)}.")

if __name__ == "__main__":
    main()
