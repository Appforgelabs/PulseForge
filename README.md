# ⚡ PulseForge — Market Macro Intelligence

**Live Dashboard:** [appforgelabs.github.io/PulseForge](https://appforgelabs.github.io/PulseForge)

A real-time macro intelligence dashboard that captures market data daily, generates beautiful interactive charts, and uses ML algorithms to produce a predictive **Market Pulse Score** (0-100).

## Features

- 📊 **Key Metrics** — VIX, S&P 500, DXY, 10Y Treasury, Bitcoin, Crude Oil with sparklines
- ⚡ **Market Pulse Score** — ML-driven composite score combining trend, momentum, volatility, and breadth signals
- 🏭 **Sector Rotation** — Real-time sector performance heatmap
- 👁️ **Watchlist Tracker** — Custom stock watchlist with signals
- 🌊 **Volatility Regime** — VIX analysis with SMA overlay and regime detection
- 🤖 **ML Predictions** — Trend regime, volatility regime, and pulse momentum forecasts
- 📅 **Macro Context** — Automated market commentary

## Architecture

```
PulseForge/
├── index.html          # Dashboard (GitHub Pages)
├── assets/
│   ├── style.css       # Dark theme
│   └── app.js          # Chart rendering (Plotly.js)
├── data/               # JSON data (auto-updated daily)
│   ├── metrics.json
│   ├── pulse.json
│   ├── sectors.json
│   ├── watchlist.json
│   ├── volatility.json
│   ├── predictions.json
│   └── macro.json
├── scripts/
│   └── fetch_data.py   # Data pipeline + ML scoring
└── .github/workflows/
    └── daily-pipeline.yml  # Runs at 6 PM ET weekdays
```

## Data Sources

- **Polygon.io** — Historical price data (free tier)
- **Finnhub** — Real-time quotes (free tier)

## Setup

1. Fork this repo
2. Add repository secrets:
   - `POLYGON_API_KEY`
   - `FINNHUB_API_KEY`
3. Enable GitHub Pages (deploy from `main` branch, root `/`)
4. Run the workflow manually or wait for the daily schedule

## ML Pulse Score Methodology

The pulse score combines five weighted signals:

| Signal | Weight | Description |
|--------|--------|-------------|
| Trend | 25% | SPY price vs 20-day SMA |
| Momentum | 20% | 10-day rate of change |
| Volatility | 25% | Inverse VIX level (low VIX = bullish) |
| VIX Direction | 15% | 5-day VIX change (falling = bullish) |
| Breadth | 15% | Volume-weighted price direction |

**Score Ranges:** 0-30 = Extreme Fear · 30-45 = Fear · 45-55 = Neutral · 55-70 = Greed · 70-100 = Extreme Greed

## License

MIT — Built by [Appforgelabs](https://github.com/Appforgelabs)

---
*Not financial advice. For educational and personal use only.*
