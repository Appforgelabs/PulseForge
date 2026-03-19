"""
compute_sibt.py — Server-side SIBT (Should I Be Trading?) Score Computer
Reads PulseForge pipeline JSON and writes sibt.json for the ShouldIBeTrading dashboard.
Run after every pipeline fetch.
"""
import json
import math
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"

FOMC_DATES = [
    "2026-01-28","2026-01-29","2026-03-18","2026-03-19",
    "2026-05-06","2026-05-07","2026-06-17","2026-06-18",
    "2026-07-29","2026-07-30","2026-09-16","2026-09-17",
    "2026-11-04","2026-11-05","2026-12-15","2026-12-16"
]

SECTOR_NAMES = {
    "XLK":"Technology","XLF":"Financials","XLE":"Energy","XLV":"Healthcare",
    "XLI":"Industrials","XLY":"Cons. Disc.","XLP":"Cons. Staples","XLU":"Utilities",
    "XLB":"Materials","XLRE":"Real Estate","XLC":"Comm. Services"
}

def load(filename):
    p = DATA_DIR / filename
    if p.exists():
        with open(p) as f:
            return json.load(f)
    return {}

def calc_rsi(values, period=14):
    if len(values) < period + 1:
        return None
    gains, losses = 0, 0
    for i in range(len(values) - period, len(values)):
        diff = values[i] - values[i-1]
        if diff > 0: gains += diff
        else: losses -= diff
    if losses == 0: return 100
    rs = (gains/period) / (losses/period)
    return round(100 - (100 / (1 + rs)), 1)

def calc_sma(values, period):
    if len(values) < period:
        return None
    return round(sum(values[-period:]) / period, 2)

def check_fomc():
    today = datetime.now(timezone.utc)
    for date_str in FOMC_DATES:
        fomc = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        diff_days = (fomc - today).total_seconds() / 86400
        if -1 <= diff_days <= 3:
            days = math.ceil(diff_days)
            if days <= 0:
                text = "⚠️ FOMC MEETING TODAY — ELEVATED VOLATILITY — REDUCE POSITION SIZING"
            else:
                text = f"⚠️ FOMC MEETING IN {days} DAY{'S' if days > 1 else ''} — PRE-FOMC CAUTION"
            return {"active": True, "text": text, "days_away": max(0, days)}
    return {"active": False, "text": "", "days_away": None}

def score_volatility(vix_values):
    if not vix_values:
        return 50
    vix = vix_values[-1]
    if vix < 15: score = min(100, 90 + (15 - vix) * 2)
    elif vix < 20: score = 70 + (20 - vix) * 4
    elif vix < 25: score = 40 + (25 - vix) * 6
    elif vix < 30: score = 15 + (30 - vix) * 5
    else: score = max(0, 10 - (vix - 30) * 2)
    # 5d slope
    slope = 0
    if len(vix_values) >= 5:
        slope = round(vix_values[-1] - vix_values[-5], 2)
        if slope < 0: score = min(100, score + 5)
        elif slope > 3: score = max(0, score - 10)
    # percentile
    pct = None
    if len(vix_values) >= 20:
        sorted_v = sorted(vix_values)
        idx = next((i for i, x in enumerate(sorted_v) if x >= vix), len(sorted_v))
        pct = round(idx / len(sorted_v) * 100)
    status = get_status(score)
    return {"score": round(score), "status": status, "vix": vix, "vix_trend": slope, "vix_percentile": pct}

def score_trend(spy_values):
    if len(spy_values) < 5:
        return {"score": 50, "status": "NEUTRAL", "spy": None, "sma20": None, "sma50": None,
                "sma200": None, "rsi": None, "above_sma20": None, "above_sma50": None,
                "above_sma200": None, "regime": "UNKNOWN"}
    spy = spy_values[-1]
    sma20 = calc_sma(spy_values, 20)
    sma50 = calc_sma(spy_values, 50)
    sma200 = calc_sma(spy_values, 200)
    rsi = calc_rsi(spy_values)
    pts = 0
    if sma200 and spy > sma200: pts += 25
    if sma50 and spy > sma50: pts += 20
    if sma20 and spy > sma20: pts += 15
    # QQQ SMA50 not available — skip +15
    if rsi is not None:
        if 50 <= rsi <= 70: pts += 25
        elif 40 <= rsi < 50: pts += 15
        elif 30 <= rsi < 40: pts += 5
        elif rsi < 30: pts += 10
        elif 70 < rsi <= 80: pts += 15
        elif rsi > 80: pts += 5
    else:
        pts += 12
    score = min(100, max(0, pts))
    # Regime
    if sma50 and sma20 and spy > sma20 and spy > sma50: regime = "UPTREND"
    elif sma50 and spy < sma50: regime = "DOWNTREND"
    else: regime = "CHOP"
    return {
        "score": round(score), "status": get_status(score),
        "spy": round(spy, 2),
        "sma20": sma20, "sma50": sma50, "sma200": sma200,
        "rsi": rsi,
        "above_sma20": bool(sma20 and spy > sma20),
        "above_sma50": bool(sma50 and spy > sma50),
        "above_sma200": bool(sma200 and spy > sma200) if sma200 else None,
        "regime": regime
    }

def score_breadth(sectors_list):
    """Use change_pct > 0 as proxy for positive momentum / likely above 50d SMA.
    In a strong downtrend day, most sectors will be negative — this reflects reality."""
    above = sum(1 for s in sectors_list if s.get("change_pct", -99) > 0)
    total = len(sectors_list)
    if above >= 9: score = min(100, 90 + (above - 9) * 5)
    elif above >= 6: score = 60 + (above - 6) * 10
    elif above >= 3: score = 30 + (above - 3) * 8
    else: score = above * 8
    return {"score": round(score), "status": get_status(score), "above_50d": above, "total": total}

def score_momentum(sectors_list):
    perfs = [(s["sym"], s.get("change_pct", 0)) for s in sectors_list if s.get("change_pct") is not None]
    if len(perfs) < 3:
        return {"score": 50, "status": "NEUTRAL", "top_sector": None, "top_pct": None,
                "weak_sector": None, "weak_pct": None, "xlk_leading": False}
    perfs.sort(key=lambda x: x[1], reverse=True)
    top3 = perfs[:3]
    bot3 = perfs[-3:]
    avg_top = sum(p for _, p in top3) / 3
    avg_bot = sum(p for _, p in bot3) / 3
    spread = avg_top - avg_bot
    if spread > 5: score = min(100, 85 + (spread - 5) * 3)
    elif spread > 3: score = 65 + (spread - 3) * 10
    elif spread > 1: score = 40 + (spread - 1) * 12.5
    else: score = max(10, spread * 10)
    xlk_leading = any(s == "XLK" for s, _ in top3)
    if xlk_leading: score = min(100, score + 5)
    return {
        "score": round(score), "status": get_status(score),
        "top_sector": top3[0][0], "top_pct": round(top3[0][1], 2),
        "weak_sector": bot3[-1][0], "weak_pct": round(bot3[-1][1], 2),
        "xlk_leading": xlk_leading
    }

def score_macro(tnx, dxy, fomc_days):
    score = 0
    if tnx is not None:
        if tnx < 3.5: s = 70
        elif tnx < 4.5: s = 50
        elif tnx < 5: s = 30
        else: s = 15
        score += s * 0.5
    else:
        score += 25  # neutral fallback

    if dxy is not None:
        if dxy < 100: s = 60
        elif dxy < 104: s = 45
        else: s = 30
        score += s * 0.5
    else:
        score += 22  # neutral fallback

    if fomc_days is not None and fomc_days >= 0:
        score = max(0, score - 10)

    fed_stance = "NEUTRAL"
    if tnx:
        if tnx < 3.8: fed_stance = "DOVISH"
        elif tnx > 4.5: fed_stance = "HAWKISH"
    dxy_trend = "NEUTRAL →"
    if dxy:
        if dxy < 100: dxy_trend = "WEAK ↓"
        elif dxy > 104: dxy_trend = "STRONG ↑"

    return {
        "score": round(min(100, max(0, score))),
        "status": get_status(score),
        "tnx": tnx, "dxy": dxy,
        "fed_stance": fed_stance, "dxy_trend": dxy_trend,
        "fomc_days": fomc_days
    }

def score_ews(spy_values, vix_values, sectors_list):
    score = 0
    # SPY 5d momentum
    if len(spy_values) >= 6:
        m = (spy_values[-1] - spy_values[-6]) / spy_values[-6] * 100
        if m > 0: score += 30
        elif m > -1: score += 15
    else:
        score += 15
    # Sector dispersion
    perfs = [s.get("change_pct", 0) for s in sectors_list if s.get("change_pct") is not None]
    if len(perfs) >= 6:
        dispersion = max(perfs) - min(perfs)
        if dispersion > 8: score += 25
        elif dispersion > 4: score += 15
        else: score += 5
    else:
        score += 12
    # RSI
    rsi = calc_rsi(spy_values)
    if rsi:
        if 50 <= rsi <= 65: score += 25
        elif 45 <= rsi < 70: score += 15
        else: score += 5
    else:
        score += 12
    # VIX falling
    if len(vix_values) >= 5:
        slope = vix_values[-1] - vix_values[-5]
        if slope < 0: score += 20
        elif slope < 1: score += 10
    else:
        score += 10
    return min(100, score)

def get_status(score):
    if score >= 75: return "HEALTHY"
    if score >= 55: return "NEUTRAL"
    if score >= 35: return "CAUTION"
    return "RISK-OFF"

def generate_terminal(scores, mqs, ews, fomc_alert):
    ts = datetime.now().strftime("%H:%M")
    vix = scores["volatility"].get("vix", "?")
    breadth = scores["breadth"]
    regime = scores["trend"].get("regime", "UNKNOWN")
    rsi = scores["trend"].get("rsi")
    top = scores["momentum"].get("top_sector", "?")
    fomc_str = ""
    if fomc_alert.get("active"):
        fomc_str = f"FOMC in {fomc_alert['days_away']}d — reduced position sizing. "

    if mqs >= 80:
        line1 = f"Strong trend ({regime}). Breadth: {breadth['above_50d']}/{breadth['total']} above 50d. VIX {vix}."
        line2 = f"Tech leadership: {'YES' if scores['momentum']['xlk_leading'] else 'NO'}. RSI: {rsi or '--'}. {fomc_str}"
        line3 = "VERDICT: Full-size swing entries with defined risk. Press winners."
    elif mqs >= 60:
        line1 = f"Mixed conditions. {regime}. VIX {vix} — {scores['volatility']['status'].lower()} volatility."
        line2 = f"Breadth {breadth['above_50d']}/{breadth['total']} above 50d. Top sector: {top}. {fomc_str}"
        line3 = f"VERDICT: 50% position sizing. A+ setups only. EWS: {ews}%."
    else:
        line1 = f"Elevated risk. VIX {vix} — {scores['volatility']['status'].lower()}. {regime}."
        line2 = f"Breadth narrow: {breadth['above_50d']}/{breadth['total']} above 50d. {fomc_str}"
        line3 = "VERDICT: Capital preservation. No new swing entries. Wait for regime reset."

    sep = "─" * 45
    return (
        f"> TERMINAL ANALYSIS [{ts}]\n"
        f"> {sep}\n"
        f"> {line1}\n"
        f"> {line2}\n"
        f"> {line3}\n"
        f"> {sep}\n"
        f"> MQS: {mqs:.1f}% | EWS: {ews}%"
    )

def main():
    print("🎯 Computing SIBT scores...")

    # Load data
    metrics = load("metrics.json")
    volatility = load("volatility.json")
    sectors_raw = load("sectors.json")
    watchlist_raw = load("watchlist.json")
    macro_raw = load("macro.json")

    # SPY closes
    spy_values = metrics.get("SPY", {}).get("values", [])
    # VIX history
    vix_values = volatility.get("vix_history", {}).get("values", [])
    if not vix_values:
        vix_values = metrics.get("VIX", {}).get("values", [])
    # TNX, DXY
    tnx_list = metrics.get("TNX", {}).get("values", [])
    dxy_list = metrics.get("DXY", {}).get("values", [])
    tnx = tnx_list[-1] if tnx_list else None
    dxy = dxy_list[-1] if dxy_list else None

    # Build sectors list
    sectors_list = []
    for name, data in sectors_raw.get("sectors", {}).items():
        sym = data.get("symbol", name)
        sectors_list.append({
            "sym": sym,
            "name": name,
            "price": data.get("price"),
            "change_pct": data.get("change_pct"),
            "above_50d": (data.get("change_pct") or -99) > 0
        })
    # Sort by change_pct desc
    sectors_list.sort(key=lambda x: x.get("change_pct") or 0, reverse=True)

    # FOMC check
    fomc = check_fomc()
    fomc_days = fomc.get("days_away")

    # Score each component
    vol_score = score_volatility(vix_values)
    trend_score = score_trend(spy_values)
    breadth_score = score_breadth(sectors_list)
    mom_score = score_momentum(sectors_list)
    macro_score = score_macro(tnx, dxy, fomc_days)

    # MQS
    mqs = (
        vol_score["score"] * 0.25 +
        trend_score["score"] * 0.20 +
        breadth_score["score"] * 0.20 +
        mom_score["score"] * 0.25 +
        macro_score["score"] * 0.10
    )
    mqs = round(mqs, 1)
    if mqs >= 80: verdict = "YES"
    elif mqs >= 60: verdict = "CAUTION"
    else: verdict = "NO"

    # EWS
    ews = score_ews(spy_values, vix_values, sectors_list)

    scores = {
        "volatility": vol_score,
        "trend": trend_score,
        "breadth": breadth_score,
        "momentum": mom_score,
        "macro": macro_score
    }

    # Terminal text
    terminal = generate_terminal(scores, mqs, ews, fomc)

    # Ticker bar items
    spy_price = spy_values[-1] if spy_values else None
    spy_prev = spy_values[-2] if len(spy_values) >= 2 else None
    spy_chg = round((spy_price - spy_prev) / spy_prev * 100, 2) if spy_price and spy_prev else None
    vix_price = vix_values[-1] if vix_values else None
    ticker = [
        {"sym": "SPY", "price": round(spy_price, 2) if spy_price else None, "change_pct": spy_chg},
        {"sym": "QQQ", "price": None, "change_pct": None},
        {"sym": "VIX", "price": round(vix_price, 2) if vix_price else None, "change_pct": None},
        {"sym": "TNX", "price": tnx, "change_pct": None},
        {"sym": "DXY", "price": dxy, "change_pct": None},
    ]
    for s in sectors_list:
        ticker.append({"sym": s["sym"], "price": s.get("price"), "change_pct": s.get("change_pct")})

    # Watchlist
    watchlist_out = []
    for sym, data in watchlist_raw.get("watchlist", {}).items():
        if isinstance(data, dict):
            watchlist_out.append({
                "sym": sym,
                "price": data.get("price"),
                "change_pct": data.get("change_pct") or data.get("dp")
            })

    sibt = {
        "generated": datetime.now().isoformat(),
        "mqs": mqs,
        "mqs_verdict": verdict,
        "ews": ews,
        "fomc_alert": fomc,
        "scores": scores,
        "sectors": sectors_list,
        "watchlist": watchlist_out,
        "ticker": ticker,
        "terminal_text": terminal,
        "fomc_dates": FOMC_DATES
    }

    out_path = DATA_DIR / "sibt.json"
    with open(out_path, "w") as f:
        json.dump(sibt, f, indent=2)

    print(f"✅ sibt.json written — MQS: {mqs}% ({verdict}), EWS: {ews}%")
    print(f"   VIX: {vix_price}, Trend: {trend_score['regime']}, Breadth: {breadth_score['above_50d']}/{breadth_score['total']}")

if __name__ == "__main__":
    main()
