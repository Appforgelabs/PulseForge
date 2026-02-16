"""
PulseForge Data Pipeline
Fetches macro data from free APIs, computes ML pulse score, writes JSON for dashboard.
Run daily via GitHub Actions.
"""

import json
import os
import sys
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
def fetch_json(url, headers=None):
    """Fetch JSON from URL with error handling."""
    req = urllib.request.Request(url)
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"  WARN: Failed to fetch {url}: {e}", file=sys.stderr)
        return None

def finnhub_quote(symbol):
    """Get quote from Finnhub."""
    if not FINNHUB_KEY:
        return None
    data = fetch_json(f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINNHUB_KEY}")
    if data and data.get("c"):
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

# ── ML: Market Pulse Score ──
def compute_pulse_score(spy_data, vix_data):
    """
    Compute Market Pulse Score (0-100) using multiple signals:
    - Trend: SPY price vs 20/50 day SMA
    - Momentum: Rate of change
    - Volatility: VIX level and direction
    - Breadth proxy: SPY volume trend
    
    Returns list of daily scores aligned with spy_data dates.
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
        if i >= 20 and spy_data.get("volumes"):
            vol_sma = sum(spy_data["volumes"][i-20:i]) / 20 if spy_data["volumes"][i-20:i] else 1
            vol_ratio = spy_data["volumes"][i] / vol_sma if vol_sma > 0 else 1
            # High volume on up days = bullish, high volume on down days = bearish
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
        
        # Trend Regime
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
    print("⚡ PulseForge Data Pipeline")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    now = datetime.now().isoformat()

    # 1. Fetch core index data
    print("\n📊 Fetching market data...")
    spy_agg = polygon_aggs("SPY", 120)
    vix_agg = polygon_aggs("VIX", 120)  # CBOE VIX via Polygon uses ticker "VIX"
    dxy_agg = polygon_aggs("DX-Y.NYB", 120) or polygon_aggs("UUP", 120)
    tnx_agg = polygon_aggs("TLT", 120)  # Treasury proxy
    btc_agg = polygon_aggs("X:BTCUSD", 120)
    oil_agg = polygon_aggs("CL=F", 120) or polygon_aggs("USO", 120)

    # Metrics JSON
    metrics = {"last_updated": now}
    mapping = {
        "SPY": spy_agg, "VIX": vix_agg, "DXY": dxy_agg,
        "TNX": tnx_agg, "BTC": btc_agg, "CL": oil_agg
    }
    for key, agg in mapping.items():
        if agg:
            metrics[key] = {"values": agg["values"][-30:]}
    
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
    write_json("sectors.json", sectors_data)

    # 3. Watchlist
    print("\n👁️ Fetching watchlist...")
    watchlist_data = {"last_updated": now, "stocks": []}
    for symbol in WATCHLIST:
        q = finnhub_quote(symbol)
        if q:
            # Simple signal based on daily change
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
                "volume": None,  # Would need separate volume endpoint
                "signal": signal,
                "notes": ""
            })
    write_json("watchlist.json", watchlist_data)

    # 4. Volatility data
    print("\n🌊 Building volatility data...")
    vol_data = {"last_updated": now}
    if vix_agg:
        vol_data["vix_history"] = {"dates": vix_agg["dates"], "values": vix_agg["values"]}
        # Compute 20-day SMA
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
        # Align dates with scores (scores start from index 0 but may be shorter)
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
    
    if spy_agg and len(spy_agg["values"]) >= 50:
        spy_now = spy_agg["values"][-1]
        sma50 = sum(spy_agg["values"][-50:]) / 50
        if spy_now > sma50:
            macro_notes.append(f"S&P 500 trading above 50-day MA (${sma50:.0f}) — bullish structure intact")
        else:
            macro_notes.append(f"S&P 500 below 50-day MA (${sma50:.0f}) — cautious positioning warranted")
    
    macro_notes.append("Pipeline runs daily at 6 PM ET via GitHub Actions")
    macro_notes.append("Data sources: Polygon.io (price history), Finnhub (real-time quotes)")
    
    write_json("macro.json", {"last_updated": now, "notes": macro_notes})

    print("\n✅ Pipeline complete! All data written to /data/")

def write_json(filename, data):
    path = DATA_DIR / filename
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  ✓ {filename} ({path.stat().st_size:,} bytes)")

if __name__ == "__main__":
    main()
