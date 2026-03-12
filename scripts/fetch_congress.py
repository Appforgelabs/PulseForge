#!/usr/bin/env python3
"""
PulseForge Congress Watch — Fetch STOCK Act disclosures.
Sources:
  Senate: GitHub raw (timothycarambat/senate-stock-watcher-data)
  House:  GitHub raw (jamesanaipakos mirror) → fallback graceful skip
Saves to data/congress.json
"""
import json, sys, time
from datetime import datetime, timedelta
from pathlib import Path
import urllib.request

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
OUT_FILE = DATA_DIR / "congress.json"

CUTOFF_DAYS = 90

# Senate: confirmed working via GitHub
SENATE_URL = (
    "https://raw.githubusercontent.com/timothycarambat/"
    "senate-stock-watcher-data/master/aggregate/all_transactions.json"
)

# House: try multiple mirrors
HOUSE_URLS = [
    "https://house-stock-watcher-data.s3-us-east-2.amazonaws.com/data/all_transactions.json",
    "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json",
    "https://raw.githubusercontent.com/jamesanaipakos/house-stock-watcher-data/master/data/all_transactions.json",
]


def fetch_url(url, timeout=45):
    req = urllib.request.Request(url, headers={
        "User-Agent": "PulseForge/1.0",
        "Accept":     "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return json.loads(raw)
    except Exception as e:
        print(f"  WARN: fetch failed {url[:80]}… : {e}", file=sys.stderr)
        return None


def parse_date(s):
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(str(s)[:10], fmt)
        except Exception:
            continue
    return None


def normalize_type(t):
    t = str(t or "").strip()
    if not t or t in ("--", "N/A"):
        return "Unknown"
    return t.title()


def parse_senate(data, cutoff):
    txns = []
    for row in (data or []):
        date_str = row.get("transaction_date") or ""
        d = parse_date(date_str)
        if not d or d < cutoff:
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker or ticker in ("N/A", "--", ""):
            continue
        txns.append({
            "chamber":          "Senate",
            "member":           str(row.get("senator") or "Unknown").strip(),
            "ticker":           ticker,
            "asset":            str(row.get("asset_description") or "")[:60],
            "type":             normalize_type(row.get("type")),
            "amount":           str(row.get("amount") or "Unknown").strip(),
            "date":             d.strftime("%Y-%m-%d"),
            "disclosure_date":  "",
        })
    return txns


def parse_house(data, cutoff):
    txns = []
    for row in (data or []):
        date_str = row.get("transaction_date") or row.get("disclosure_date") or ""
        d = parse_date(date_str)
        if not d or d < cutoff:
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker or ticker in ("N/A", "--", ""):
            continue
        member = str(row.get("representative") or "Unknown").strip()
        member = member.replace("Hon. ", "").replace("Rep. ", "")
        txns.append({
            "chamber":          "House",
            "member":           member,
            "ticker":           ticker,
            "asset":            str(row.get("asset_description") or "")[:60],
            "type":             normalize_type(row.get("type")),
            "amount":           str(row.get("amount") or "Unknown").strip(),
            "date":             d.strftime("%Y-%m-%d"),
            "disclosure_date":  str(row.get("disclosure_date") or "")[:10],
        })
    return txns


def load_existing():
    """Load existing congress.json to preserve if all sources fail."""
    try:
        with open(OUT_FILE) as f:
            return json.load(f)
    except Exception:
        return None


def main():
    cutoff = datetime.now() - timedelta(days=CUTOFF_DAYS)
    all_txns = []

    # ── Senate ──────────────────────────────────────────
    print("  [congress] Fetching Senate (GitHub)...", file=sys.stderr)
    senate_data = fetch_url(SENATE_URL, timeout=30)
    if senate_data and isinstance(senate_data, list):
        senate_txns = parse_senate(senate_data, cutoff)
        print(f"  [congress] Senate: {len(senate_txns)} txns in window", file=sys.stderr)
        all_txns.extend(senate_txns)
    else:
        print("  [congress] Senate fetch failed or empty", file=sys.stderr)

    time.sleep(1)

    # ── House ────────────────────────────────────────────
    house_txns = []
    for url in HOUSE_URLS:
        print(f"  [congress] Fetching House: {url[:70]}...", file=sys.stderr)
        house_data = fetch_url(url, timeout=30)
        if house_data and isinstance(house_data, list) and len(house_data) > 100:
            house_txns = parse_house(house_data, cutoff)
            print(f"  [congress] House: {len(house_txns)} txns", file=sys.stderr)
            break
        time.sleep(1)

    if not house_txns:
        print("  [congress] House: all sources failed — skipping", file=sys.stderr)

    all_txns.extend(house_txns)
    all_txns.sort(key=lambda x: x.get("date", ""), reverse=True)

    # If we got nothing from live sources, keep existing data
    if not all_txns:
        existing = load_existing()
        if existing and existing.get("transactions"):
            print("  [congress] No live data — keeping existing congress.json", file=sys.stderr)
            # Update timestamp to show pipeline ran
            existing["last_updated"] = datetime.now().isoformat()
            existing["note"] = "Seeded from public STOCK Act disclosure records. Live sources temporarily unavailable."
            with open(OUT_FILE, "w") as f:
                json.dump(existing, f, indent=2)
            return
        else:
            print("  [congress] No data available at all — skipping write", file=sys.stderr)
            return

    # Stats
    purchases = sum(1 for t in all_txns if "purchase" in t["type"].lower())
    sales      = sum(1 for t in all_txns if "sale" in t["type"].lower())
    chambers   = list({t["chamber"] for t in all_txns})

    out = {
        "last_updated":  datetime.now().isoformat(),
        "cutoff_date":   cutoff.strftime("%Y-%m-%d"),
        "total":         len(all_txns),
        "purchases":     purchases,
        "sales":         sales,
        "chambers":      chambers,
        "transactions":  all_txns[:300],
    }
    with open(OUT_FILE, "w") as f:
        json.dump(out, f, indent=2)
    print(f"  [congress] Saved {len(out['transactions'])} transactions → {OUT_FILE}", file=sys.stderr)


if __name__ == "__main__":
    main()
