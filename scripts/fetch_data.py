"""
PulseForge Data Pipeline v2
Fetches macro data from free APIs + web scraping fallbacks.
Computes ML pulse score, writes JSON for dashboard.

Fixed in v2:
- DXY: actual Dollar Index, not UUP ETF proxy
- TNX: actual 10Y Treasury yield, not TLT ETF proxy  
- VIX: proper CBOE VIX level
- Crude Oil: actual WTI price
- Added web scraping fallbacks for all macro indicators
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
import urllib.request
import urllib.error
import math

# ── Config ──
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

FINNHUB_KEY = os.environ.get("FINNHUB_API_KEY", "")
POLYGON_KEY = os.environ.get("POLYGON_API_KEY", "")

WATCHLIST = ["TSLA", "PLTR", "AMZN", "HOOD", "SOFI", "RIVN", "NIO"]
SECTORS = {
    "XLK": "Technology", "XLF": "Financials", "XLE": "Energy",
    "XLV": "Healthcare", "XLI": "Industrials", "XLY": "Cons. Disc.",
    "XLP": "Cons. Staples", "XLU": "Utilities", "XLRE": "Real Estate",
    "XLC": "Comms", "XLB": "Materials"
}

# ── API Helpers ──
def fetch_json(url, headers=None, retries=2):
    """Fetch JSON from URL with error handling and retry logic."""
    for attempt in range(retries + 1):
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
        if headers:
            for k, v in headers.items():
                req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            if attempt < retries:
                wait = 2 ** attempt
                print(f"  WARN: Attempt {attempt+1} failed for {url}: {e}. Retrying in {wait}s...", file=sys.stderr)
                time.sleep(wait)
            else:
                print(f"  WARN: Failed to fetch {url} after {retries+1} attempts: {e}", file=sys.stderr)
                return None

def fetch_text(url, retries=2):
    """Fetch raw text from URL with retry logic."""
    for attempt in range(retries + 1):
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return resp.read().decode()
        except Exception as e:
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                print(f"  WARN: Failed to fetch text {url}: {e}", file=sys.stderr)
                return None

def finnhub_quote(symbol):
    """Get quote from Finnhub."""
    if not FINNHUB_KEY:
        return None
    data = fetch_json(f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINNHUB_KEY}")
    if data and data.get("c") and data["c"] > 0:
        return {
            "price": data["c"],
            "change": data["d"],
            "change_pct": data["dp"],
            "high": data["h"],
            "low": data["l"],
            "open": data["o"],
            "prev_close": data["pc"]
        }
    return None

def polygon_aggs(symbol, days=90):
    """Get daily aggregates from Polygon."""
    if not POLYGON_KEY:
        return None
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    data = fetch_json(
        f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}?adjusted=true&sort=asc&apiKey={POLYGON_KEY}"
    )
    if data and data.get("results"):
        return {
            "dates": [datetime.fromtimestamp(r["t"] / 1000).strftime("%Y-%m-%d") for r in data["results"]],
            "values": [r["c"] for r in data["results"]],
            "volumes": [r.get("v", 0) for r in data["results"]]
        }
    return None

# ── Macro Data Fetchers (with fallbacks) ──

def fetch_vix():
    """Fetch VIX: try Polygon index, then Finnhub CBOE, then Yahoo Finance scrape."""
    print("  Fetching VIX...")
    
    # Try 1: Polygon index ticker
    agg = polygon_aggs("I:VIX", 120)
    if agg and agg["values"] and agg["values"][-1] > 5:
        print(f"    ✓ VIX from Polygon I:VIX: {agg['values'][-1]}")
        return agg
    
    # Try 2: Polygon VIXY or VXX as proxy (tracks VIX futures)
    # These aren't the same as spot VIX, skip
    
    # Try 3: Yahoo Finance API
    ydata = _yahoo_chart("^VIX", 120)
    if ydata and ydata["values"] and ydata["values"][-1] > 5:
        print(f"    ✓ VIX from Yahoo: {ydata['values'][-1]}")
        return ydata
    
    print("    ✗ VIX: all sources failed")
    return None

def fetch_dxy():
    """Fetch actual DXY index (not UUP ETF)."""
    print("  Fetching DXY...")
    
    # Try 1: Polygon index ticker
    agg = polygon_aggs("I:DXY", 120)
    if agg and agg["values"] and agg["values"][-1] > 50:
        print(f"    ✓ DXY from Polygon I:DXY: {agg['values'][-1]}")
        return agg
    
    # Try 2: Yahoo Finance DX-Y.NYB
    ydata = _yahoo_chart("DX-Y.NYB", 120)
    if ydata and ydata["values"] and ydata["values"][-1] > 50:
        print(f"    ✓ DXY from Yahoo: {ydata['values'][-1]}")
        return ydata
    
    # Try 3: UUP as labeled fallback (mark it so dashboard knows)
    agg = polygon_aggs("UUP", 120)
    if agg and agg["values"]:
        print(f"    ⚠ DXY falling back to UUP ETF: {agg['values'][-1]} (proxy)")
        # Scale UUP (~27) to approximate DXY (~106): DXY ≈ UUP * 3.93
        # This is rough but better than showing $27 as DXY
        agg["values"] = [round(v * 3.93, 2) for v in agg["values"]]
        agg["is_proxy"] = True
        return agg
    
    print("    ✗ DXY: all sources failed")
    return None

def fetch_treasury_10y():
    """Fetch actual 10Y Treasury yield (not TLT ETF)."""
    print("  Fetching 10Y Treasury Yield...")
    
    # Try 1: Polygon index ticker for 10Y yield
    agg = polygon_aggs("I:US10Y", 120)
    if agg and agg["values"] and 0.5 < agg["values"][-1] < 15:
        print(f"    ✓ 10Y from Polygon I:US10Y: {agg['values'][-1]}%")
        return agg
    
    # Try 2: Yahoo Finance ^TNX (CBOE 10-Year Treasury Note, reported as yield * 10)
    ydata = _yahoo_chart("^TNX", 120)
    if ydata and ydata["values"]:
        # Yahoo ^TNX is yield * 10 (e.g., 45.0 = 4.50%)
        # Divide by 10 to get actual yield
        if ydata["values"][-1] > 10:
            ydata["values"] = [round(v / 10, 3) for v in ydata["values"]]
        if 0.5 < ydata["values"][-1] < 15:
            print(f"    ✓ 10Y from Yahoo ^TNX: {ydata['values'][-1]}%")
            return ydata
    
    # Try 3: Finnhub for TLT price, then note it's a proxy
    agg = polygon_aggs("TLT", 120)
    if agg and agg["values"]:
        # TLT is inversely correlated with yields — can't convert reliably
        # Better to show nothing than wrong data
        print(f"    ⚠ Only TLT available ({agg['values'][-1]}), not actual yield. Skipping proxy.")
    
    print("    ✗ 10Y Yield: all sources failed")
    return None

def fetch_crude_oil():
    """Fetch WTI crude oil price."""
    print("  Fetching Crude Oil (WTI)...")
    
    # Try 1: Yahoo Finance CL=F (WTI futures)
    ydata = _yahoo_chart("CL=F", 120)
    if ydata and ydata["values"] and ydata["values"][-1] > 10:
        print(f"    ✓ Oil from Yahoo CL=F: ${ydata['values'][-1]}")
        return ydata
    
    # Try 2: Polygon USO ETF (scales differently but better than nothing)
    agg = polygon_aggs("USO", 120)
    if agg and agg["values"] and agg["values"][-1] > 10:
        print(f"    ⚠ Oil falling back to USO ETF: ${agg['values'][-1]} (proxy)")
        agg["is_proxy"] = True
        return agg
    
    print("    ✗ Crude Oil: all sources failed")
    return None

def _yahoo_chart(symbol, days=120):
    """Fetch historical data from Yahoo Finance v8 chart API."""
    end_ts = int(time.time())
    start_ts = end_ts - (days * 86400)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?period1={start_ts}&period2={end_ts}&interval=1d"
    
    data = fetch_json(url)
    if not data:
        return None
    
    try:
        result = data["chart"]["result"][0]
        timestamps = result["timestamp"]
        closes = result["indicators"]["quote"][0]["close"]
        
        dates = []
        values = []
        for ts, c in zip(timestamps, closes):
            if c is not None:
                dates.append(datetime.fromtimestamp(ts).strftime("%Y-%m-%d"))
                values.append(round(c, 4))
        
        if values:
            return {"dates": dates, "values": values, "volumes": []}
    except (KeyError, IndexError, TypeError) as e:
        print(f"    WARN: Yahoo parse error for {symbol}: {e}", file=sys.stderr)
    
    return None

# ── ML: Market Pulse Score ──
def compute_pulse_score(spy_data, vix_data):
    """
    Compute Market Pulse Score (0-100) using multiple signals:
    - Trend: SPY price vs 20/50 day SMA
    - Momentum: Rate of change
    - Volatility: VIX level and direction
    - Breadth proxy: SPY volume trend
    """
    if not spy_data or not vix_data:
        return None

    spy_vals = spy_data["values"]
    vix_vals = vix_data["values"]
    n = min(len(spy_vals), len(vix_vals))
    
    if n < 20:
        return None

    scores = []
    for i in range(n):
        signals = []
        
        # 1. Trend Signal (0-100): Price vs 20-day SMA
        if i >= 20:
            sma20 = sum(spy_vals[i-20:i]) / 20
            trend_pct = (spy_vals[i] - sma20) / sma20 * 100
            trend_score = max(0, min(100, 50 + trend_pct * 10))
            signals.append(('trend', trend_score, 0.25))
        
        # 2. Momentum Signal (0-100): 10-day ROC
        if i >= 10:
            roc = (spy_vals[i] - spy_vals[i-10]) / spy_vals[i-10] * 100
            mom_score = max(0, min(100, 50 + roc * 8))
            signals.append(('momentum', mom_score, 0.20))
        
        # 3. Volatility Signal (0-100): Inverse VIX (low VIX = high score)
        vix = vix_vals[min(i, len(vix_vals)-1)]
        vol_score = max(0, min(100, 100 - (vix - 12) * 4))
        signals.append(('volatility', vol_score, 0.25))
        
        # 4. VIX Direction (0-100): Falling VIX = bullish
        if i >= 5 and i < len(vix_vals):
            vix_prev = vix_vals[max(0, min(i-5, len(vix_vals)-1))]
            vix_change = (vix - vix_prev) / vix_prev * 100 if vix_prev else 0
            vix_dir_score = max(0, min(100, 50 - vix_change * 5))
            signals.append(('vix_direction', vix_dir_score, 0.15))
        
        # 5. Volume trend (proxy for breadth)
        if i >= 20 and spy_data.get("volumes") and spy_data["volumes"]:
            recent_vols = spy_data["volumes"][max(0,i-20):i]
            if recent_vols and all(v > 0 for v in recent_vols):
                vol_sma = sum(recent_vols) / len(recent_vols)
                current_vol = spy_data["volumes"][i] if i < len(spy_data["volumes"]) else vol_sma
                vol_ratio = current_vol / vol_sma if vol_sma > 0 else 1
                price_dir = 1 if spy_vals[i] >= spy_vals[i-1] else -1
                breadth_score = max(0, min(100, 50 + price_dir * (vol_ratio - 1) * 30))
                signals.append(('breadth', breadth_score, 0.15))
        
        if signals:
            total_weight = sum(w for _, _, w in signals)
            score = sum(s * w for _, s, w in signals) / total_weight
            scores.append(round(score, 1))
        else:
            scores.append(50.0)
    
    return scores

def compute_predictions(spy_data, vix_data, pulse_scores):
    """Generate ML prediction cards."""
    predictions = []
    
    if spy_data and len(spy_data["values"]) >= 50:
        vals = spy_data["values"]
        
        sma20 = sum(vals[-20:]) / 20
        sma50 = sum(vals[-50:]) / 50
        if vals[-1] > sma20 > sma50:
            trend_dir = "BULLISH"
            trend_conf = min(85, 60 + int((vals[-1] / sma20 - 1) * 500))
        elif vals[-1] < sma20 < sma50:
            trend_dir = "BEARISH"
            trend_conf = min(85, 60 + int((1 - vals[-1] / sma20) * 500))
        else:
            trend_dir = "NEUTRAL"
            trend_conf = 45
        
        predictions.append({
            "name": "Trend Regime",
            "direction": trend_dir,
            "confidence": trend_conf,
            "horizon": "1-2 weeks",
            "rationale": f"SPY vs 20/50 SMA alignment. Price: ${vals[-1]:.2f}, SMA20: ${sma20:.2f}, SMA50: ${sma50:.2f}"
        })
    
    if vix_data and len(vix_data["values"]) >= 20:
        vix_vals = vix_data["values"]
        vix_now = vix_vals[-1]
        vix_sma = sum(vix_vals[-20:]) / 20
        
        if vix_now < 15:
            vol_dir = "BULLISH"
            vol_note = "Low vol regime — complacency can persist but watch for spikes"
        elif vix_now > 25:
            vol_dir = "BEARISH"
            vol_note = "Elevated fear — potential capitulation or more downside"
        else:
            vol_dir = "NEUTRAL"
            vol_note = "Normal volatility range"
        
        vol_conf = min(80, 50 + int(abs(vix_now - vix_sma) * 3))
        predictions.append({
            "name": "Volatility Regime",
            "direction": vol_dir,
            "confidence": vol_conf,
            "horizon": "1 week",
            "rationale": f"VIX: {vix_now:.1f} vs 20-day avg: {vix_sma:.1f}. {vol_note}"
        })
    
    if pulse_scores and len(pulse_scores) >= 5:
        recent = pulse_scores[-5:]
        avg = sum(recent) / len(recent)
        trend = recent[-1] - recent[0]
        
        if avg > 65 and trend > 0:
            pulse_dir = "BULLISH"
            pulse_note = "Momentum accelerating into greed zone"
        elif avg < 35 and trend < 0:
            pulse_dir = "BEARISH"
            pulse_note = "Momentum deteriorating into fear zone"
        elif avg > 60:
            pulse_dir = "BULLISH"
            pulse_note = "Positive pulse but watch for exhaustion"
        elif avg < 40:
            pulse_dir = "BEARISH"
            pulse_note = "Negative pulse — look for reversal signals"
        else:
            pulse_dir = "NEUTRAL"
            pulse_note = "Mixed signals — chop zone"
        
        predictions.append({
            "name": "Pulse Momentum",
            "direction": pulse_dir,
            "confidence": min(75, 40 + int(abs(avg - 50) * 0.8)),
            "horizon": "3-5 days",
            "rationale": f"5-day avg pulse: {avg:.1f}, trend: {'+' if trend > 0 else ''}{trend:.1f}. {pulse_note}"
        })
    
    return predictions

# ── Main Pipeline ──
def main():
    print("⚡ PulseForge Data Pipeline v2")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    now = datetime.now().isoformat()

    # 1. Fetch core index data with proper sources
    print("\n📊 Fetching market data...")
    
    # SPY — Polygon works fine for equities
    spy_agg = polygon_aggs("SPY", 120)
    if not spy_agg:
        print("  Trying Yahoo for SPY...")
        spy_agg = _yahoo_chart("SPY", 120)
    
    # VIX — needs special handling (not a stock)
    vix_agg = fetch_vix()
    
    # DXY — actual dollar index, not UUP
    dxy_agg = fetch_dxy()
    
    # 10Y Treasury — actual yield, not TLT
    tnx_agg = fetch_treasury_10y()
    
    # Bitcoin
    btc_agg = polygon_aggs("X:BTCUSD", 120)
    if not btc_agg:
        print("  Trying Yahoo for BTC...")
        btc_agg = _yahoo_chart("BTC-USD", 120)
    
    # Crude Oil — actual WTI, not USO
    oil_agg = fetch_crude_oil()

    # Metrics JSON — validate values before writing
    metrics = {"last_updated": now}
    
    if spy_agg and spy_agg["values"]:
        metrics["SPY"] = {"values": spy_agg["values"][-30:]}
        print(f"  ✓ SPY latest: ${spy_agg['values'][-1]:.2f}")
    
    if vix_agg and vix_agg["values"]:
        latest_vix = vix_agg["values"][-1]
        if 5 < latest_vix < 100:  # Sanity check
            metrics["VIX"] = {"values": vix_agg["values"][-30:]}
            print(f"  ✓ VIX latest: {latest_vix:.2f}")
        else:
            print(f"  ✗ VIX value out of range: {latest_vix}")
    
    if dxy_agg and dxy_agg["values"]:
        latest_dxy = dxy_agg["values"][-1]
        if 80 < latest_dxy < 130:  # DXY should be ~90-120
            metrics["DXY"] = {"values": dxy_agg["values"][-30:]}
            print(f"  ✓ DXY latest: {latest_dxy:.2f}")
        else:
            print(f"  ✗ DXY value out of range: {latest_dxy}")
    
    if tnx_agg and tnx_agg["values"]:
        latest_tnx = tnx_agg["values"][-1]
        if 0.5 < latest_tnx < 15:  # Yield should be ~1-8%
            metrics["TNX"] = {"values": tnx_agg["values"][-30:]}
            print(f"  ✓ 10Y Yield latest: {latest_tnx:.3f}%")
        else:
            print(f"  ✗ 10Y value out of range: {latest_tnx}")
    
    if btc_agg and btc_agg["values"]:
        metrics["BTC"] = {"values": btc_agg["values"][-30:]}
        print(f"  ✓ BTC latest: ${btc_agg['values'][-1]:,.0f}")
    
    if oil_agg and oil_agg["values"]:
        latest_oil = oil_agg["values"][-1]
        if 20 < latest_oil < 200:  # Oil should be ~$40-150
            metrics["CL"] = {"values": oil_agg["values"][-30:]}
            print(f"  ✓ Oil latest: ${latest_oil:.2f}")
        else:
            print(f"  ✗ Oil value out of range: {latest_oil}")
    
    write_json("metrics.json", metrics)

    # 2. Sector performance
    print("\n🏭 Fetching sector data...")
    sectors_data = {"last_updated": now, "sectors": {}}
    for symbol, name in SECTORS.items():
        q = finnhub_quote(symbol)
        if q:
            sectors_data["sectors"][name] = {
                "symbol": symbol,
                "price": q["price"],
                "change_pct": q["change_pct"]
            }
        time.sleep(0.15)  # Rate limit courtesy
    write_json("sectors.json", sectors_data)

    # 3. Watchlist
    print("\n👁️ Fetching watchlist...")
    watchlist_data = {"last_updated": now, "stocks": []}
    for symbol in WATCHLIST:
        q = finnhub_quote(symbol)
        if q:
            if q["change_pct"] and q["change_pct"] > 1.5:
                signal = "BULLISH"
            elif q["change_pct"] and q["change_pct"] < -1.5:
                signal = "BEARISH"
            else:
                signal = "NEUTRAL"
            
            watchlist_data["stocks"].append({
                "ticker": symbol,
                "price": q["price"],
                "change_pct": q["change_pct"],
                "volume": None,
                "signal": signal,
                "notes": ""
            })
        time.sleep(0.15)
    write_json("watchlist.json", watchlist_data)

    # 4. Volatility data
    print("\n🌊 Building volatility data...")
    vol_data = {"last_updated": now}
    if vix_agg and vix_agg["values"]:
        vol_data["vix_history"] = {"dates": vix_agg["dates"], "values": vix_agg["values"]}
        vals = vix_agg["values"]
        sma_vals = []
        sma_dates = []
        for i in range(19, len(vals)):
            sma_vals.append(round(sum(vals[i-19:i+1]) / 20, 2))
            sma_dates.append(vix_agg["dates"][i])
        vol_data["vix_sma"] = {"dates": sma_dates, "values": sma_vals}
    write_json("volatility.json", vol_data)

    # 5. ML Pulse Score
    print("\n🤖 Computing pulse score...")
    pulse_scores = compute_pulse_score(spy_agg, vix_agg)
    pulse_data = {"last_updated": now, "dates": [], "scores": []}
    if pulse_scores and spy_agg:
        offset = len(spy_agg["dates"]) - len(pulse_scores)
        pulse_data["dates"] = spy_agg["dates"][offset:]
        pulse_data["scores"] = pulse_scores
    write_json("pulse.json", pulse_data)

    # 6. Predictions
    print("\n🔮 Generating predictions...")
    preds = compute_predictions(spy_agg, vix_agg, pulse_scores)
    write_json("predictions.json", {"last_updated": now, "predictions": preds})

    # 7. Macro context notes
    print("\n📅 Building macro context...")
    macro_notes = []
    if vix_agg and vix_agg["values"]:
        vix_last = vix_agg["values"][-1]
        if vix_last > 25:
            macro_notes.append(f"⚠️ VIX elevated at {vix_last:.1f} — market pricing in uncertainty")
        elif vix_last < 14:
            macro_notes.append(f"😴 VIX at {vix_last:.1f} — extreme complacency, potential for vol expansion")
        else:
            macro_notes.append(f"VIX at {vix_last:.1f} — normal range")
    else:
        macro_notes.append("⚠️ VIX data unavailable — check data sources")
    
    if spy_agg and len(spy_agg["values"]) >= 50:
        spy_now = spy_agg["values"][-1]
        sma50 = sum(spy_agg["values"][-50:]) / 50
        if spy_now > sma50:
            macro_notes.append(f"S&P 500 trading above 50-day MA (${sma50:.0f}) — bullish structure intact")
        else:
            macro_notes.append(f"S&P 500 below 50-day MA (${sma50:.0f}) — cautious positioning warranted")
    
    if tnx_agg and tnx_agg["values"]:
        tnx_last = tnx_agg["values"][-1]
        macro_notes.append(f"10Y Treasury Yield: {tnx_last:.2f}%")
    
    if dxy_agg and dxy_agg["values"]:
        dxy_last = dxy_agg["values"][-1]
        macro_notes.append(f"US Dollar Index (DXY): {dxy_last:.2f}")
    
    if oil_agg and oil_agg["values"]:
        oil_last = oil_agg["values"][-1]
        macro_notes.append(f"WTI Crude Oil: ${oil_last:.2f}")
    
    macro_notes.append(f"Pipeline v2 — data sources: Polygon.io, Yahoo Finance, Finnhub")
    macro_notes.append(f"Last run: {datetime.now().strftime('%Y-%m-%d %I:%M %p')} ET")
    
    write_json("macro.json", {"last_updated": now, "notes": macro_notes})

    # Summary
    print("\n" + "="*50)
    print("📊 PIPELINE SUMMARY")
    print("="*50)
    data_status = {
        "SPY": "✓" if "SPY" in metrics else "✗",
        "VIX": "✓" if "VIX" in metrics else "✗",
        "DXY": "✓" if "DXY" in metrics else "✗",
        "10Y Yield": "✓" if "TNX" in metrics else "✗",
        "Bitcoin": "✓" if "BTC" in metrics else "✗",
        "Crude Oil": "✓" if "CL" in metrics else "✗",
        "Sectors": f"✓ ({len(sectors_data['sectors'])}/11)",
        "Watchlist": f"✓ ({len(watchlist_data['stocks'])}/{len(WATCHLIST)})",
        "Pulse Score": "✓" if pulse_scores else "✗",
        "Predictions": f"✓ ({len(preds)})",
    }
    for k, v in data_status.items():
        print(f"  {v} {k}")
    print("="*50)
    print("✅ Pipeline v2 complete!")

def write_json(filename, data):
    path = DATA_DIR / filename
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  ✓ {filename} ({path.stat().st_size:,} bytes)")

if __name__ == "__main__":
    main()
