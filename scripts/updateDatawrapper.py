#!/usr/bin/env python3
"""
Update Datawrapper chart titles via API.
Requires DATAWRAPPER_API_TOKEN environment variable and chart IDs in config.
"""

import json
import os
import sys
from pathlib import Path
import requests

# --- CONFIG ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
HEADLINES_FILE = DATA_DIR / "headlines.json"
CONFIG_FILE = BASE_DIR / "datawrapper_config.json"

# Datawrapper API endpoint
API_BASE = "https://api.datawrapper.de/v3/charts"

# Chart ID mapping (from config file)
CHART_IDS = {
    "mostCommon": None,
    "bikeThefts": None,
    "parties": None,
    "yoyTrend": None
}


def load_config():
    """Load chart IDs from config file or environment variables."""
    # Try environment variables first (for GitHub Actions)
    chart_ids_from_env = {}
    for key in CHART_IDS:
        env_key = f"DATAWRAPPER_CHART_{key.upper()}"
        if os.getenv(env_key):
            CHART_IDS[key] = os.getenv(env_key)
            chart_ids_from_env[key] = True
    
    if any(CHART_IDS.values()):
        return True
    
    # Fall back to config file
    if not CONFIG_FILE.exists():
        print(f"⚠️  Config file not found: {CONFIG_FILE}")
        print("   Create it with your chart IDs, or set environment variables:")
        print("   DATAWRAPPER_CHART_MOSTCOMMON, DATAWRAPPER_CHART_BIKETHEFTS, etc.")
        print("   Example config:")
        print('   {"mostCommon": "abc123", "bikeThefts": "def456", "parties": "ghi789", "yoyTrend": "jkl012"}')
        return False
    
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
        
        for key in CHART_IDS:
            if key in config and not CHART_IDS[key]:  # Don't override env vars
                CHART_IDS[key] = config[key]
        
        return True
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        return False


def get_api_token():
    """Get API token from environment variable."""
    token = os.getenv("DATAWRAPPER_API_TOKEN")
    if not token:
        print("⚠️  DATAWRAPPER_API_TOKEN environment variable not set")
        print("   Set it in your environment or GitHub Actions secrets")
        return None
    return token


def update_chart_title(chart_id, title, api_token):
    """Update a Datawrapper chart's title via API."""
    if not chart_id:
        return False
    
    url = f"{API_BASE}/{chart_id}"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    payload = {"title": title}
    
    try:
        response = requests.put(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Failed to update chart {chart_id}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"      Response: {e.response.text}")
        return False


def update_all_charts(headlines, api_token):
    """Update all Datawrapper chart titles."""
    if not load_config():
        return False
    
    if not api_token:
        return False
    
    print("\n📡 Updating Datawrapper charts...")
    success_count = 0
    
    for key, headline in headlines.items():
        chart_id = CHART_IDS.get(key)
        if not chart_id:
            print(f"   ⚠️  Skipping {key}: no chart ID configured")
            continue
        
        print(f"   Updating {key} ({chart_id})...")
        if update_chart_title(chart_id, headline, api_token):
            print(f"   ✅ Updated {key}")
            success_count += 1
        else:
            print(f"   ❌ Failed to update {key}")
    
    print(f"\n✅ Updated {success_count}/{len(headlines)} charts")
    return success_count > 0


def main():
    """Main function."""
    if not HEADLINES_FILE.exists():
        print(f"❌ Headlines file not found: {HEADLINES_FILE}")
        print("   Run generateChartData.py first to generate headlines")
        sys.exit(1)
    
    with open(HEADLINES_FILE, "r", encoding="utf-8") as f:
        headlines = json.load(f)
    
    api_token = get_api_token()
    if not api_token:
        print("\n⚠️  Skipping Datawrapper updates (no API token)")
        sys.exit(0)
    
    update_all_charts(headlines, api_token)


if __name__ == "__main__":
    main()

