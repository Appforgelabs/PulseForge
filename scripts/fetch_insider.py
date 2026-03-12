#!/usr/bin/env python3
"""
PulseForge Insider Flow — Fetch recent Form 4 insider transactions.
Uses Finnhub free API for watchlist + mega-cap stocks.
Saves to data/insider.json
"""
import json, os, sys, time
from datetime import datetime, timedelta
from pathlib import Path
import urllib.request

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

FINNHUB_KEY = os.environ.get("FINNHUB_API_KEY", "d6952r9r01qs7u9kq240d6952r9r01qs7u9kq24g")
OUT_FILE = DATA_DIR / "insider.json"

# Expanded list: watchlist + mega caps
TICKERS = ["TSLA", "PLTR", "AMZN", "HOOD", "SOFI", "RIVN", "NIO",
           "NVDA", "META", "MSFT", "AAPL", "GOOGL", "JPM", "NFLX", "AMD"]

TRANSACTION_CODES = {
    "P": "Purchase",
    "S": "Sale",
    "A": "Grant",
    "D": "Disposition",
    "F": "Tax Withholding",
    "M": "Option Exercise",
    "G": "Gift",
}

def fetch_insider_txns(ticker, from_date, to_date):
    url = (f"https://finnhub.io/api/v1/stock/insider-transactions"
           f"?symbol={ticker}&from={from_date}&to={to_date}&token={FINNHUB_KEY}")
    req = urllib.request.Request(url, headers={"User-Agent": "PulseForge/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        transactions = data.get("data", [])
        filtered = []
        for t in transactions:
            shares = t.get("share", 0) or 0
            price  = t.get("transactionPrice", 0) or 0
            code   = t.get("transactionCode", "")
            if not shares:
                continue
            # Determine direction: positive shares = buy, negative = sell
            direction = "BUY" if shares > 0 else "SELL"
            filtered.append({
                "ticker":       ticker,
                "name":         (t.get("name") or "Unknown").title(),
                "date":         t.get("transactionDate", "")[:10],
                "filing_date":  t.get("filingDate", "")[:10],
                "type":         direction,
                "code":         TRANSACTION_CODES.get(code, code),
                "shares":       abs(shares),
                "price":        price,
                "value":        abs(shares * price),
            })
        # Sort by date desc, keep top 5 per ticker
        filtered.sort(key=lambda x: x["date"], reverse=True)
        return filtered[:5]
    except Exception as e:
        print(f"  WARN: {ticker} insider fetch failed: {e}", file=sys.stderr)
        return []


def main():
    to_date   = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    all_txns = []
    for ticker in TICKERS:
        print(f"  [insider] {ticker}...", file=sys.stderr)
        txns = fetch_insider_txns(ticker, from_date, to_date)
        all_txns.extend(txns)
        time.sleep(0.6)  # Stay under Finnhub 60 req/min

    all_txns.sort(key=lambda x: x.get("date", ""), reverse=True)

    out = {
        "last_updated":  datetime.now().isoformat(),
        "from_date":     from_date,
        "to_date":       to_date,
        "transactions":  all_txns[:100],
    }
    with open(OUT_FILE, "w") as f:
        json.dump(out, f, indent=2)
    print(f"  [insider] Saved {len(out['transactions'])} transactions → {OUT_FILE}", file=sys.stderr)


if __name__ == "__main__":
    main()
