# ETF Pulse 🌅

**Daily US ETF insights** — automated analysis + content generation right after market close.

---

## What's different? (Moat)

| Feature | Brokers | Webull | Seeking Alpha | **ETF Pulse** |
|---------|---------|--------|---------------|---------------|
| Category-based best recommendations | ✗ | ✗ | Partial | ✓ ★ |
| Volume spike + AI narrative | ✗ | ✗ | ✗ | ✓ ★ |
| Same-category alternatives auto-compare | ✗ | ✗ | ✗ | ✓ ★ |
| Auto pattern insights (theme rotation) | ✗ | ✗ | ✗ | ✓ ★ |
| Portfolio daily pulse | Partial | Partial | ✗ | ✓ ★ |
| Daily auto-publish (email/telegram) | ✗ | ✗ | ✗ | ✓ ★ |
| Intent-based best (long-hold/trader/dividend) | ✗ | ✗ | ✗ | ✓ ★ |
| Tracking error analysis | ✗ | ✗ | ✗ | ✓ ★ |

---

## Quick Start

```bash
# Install
pip install -r etf_pulse/requirements.txt

# Init
python etf_pulse/db_schema.py
python etf_pulse/backfill.py    # 30-day history (one time)

# Daily run
python etf_pulse/run_daily.py
```

---

## Modules (15+)

```
etf_pulse/
├── Data layer
│   ├── etf_universe.py     # 257 US ETFs (8 categories)
│   ├── db_schema.py        # SQLite 5 tables
│   ├── daily_fetch.py      # yfinance daily data
│   └── backfill.py         # Historical N-day backfill
├── Signal layer
│   ├── signals.py          # Volume spike, momentum, category strength
│   ├── advanced_signals.py # Tracking error, Sharpe, MDD, RSI
│   └── ranking_changes.py  # Weekly category best changes
├── Content layer
│   ├── narrative.py        # Korean Markdown content
│   ├── narrative_en.py     # English content
│   ├── category_best.py    # Michelin-style category rankings
│   ├── intent_best.py      # Long-hold / Trader / Dividend / Small / Momentum
│   ├── dual_market.py      # KR + US integrated daily
│   └── compare.py          # Same-category ETF comparison
├── Analysis layer
│   ├── portfolio.py        # User holdings tracker
│   ├── portfolio_analyzer.py # Sector overlap, diversification score
│   └── bridge_eps.py       # EPS Momentum system integration
├── Publishing layer
│   ├── publisher.py        # Telegram + Substack
│   ├── email_sender.py     # Gmail SMTP
│   └── charts.py           # matplotlib visualization
├── AI / API
│   ├── chatbot.py          # AI advisor (Claude/Gemini)
│   └── mcp_server.py       # Claude Desktop MCP server
├── Backtesting
│   ├── bt_signals.py       # Signal alpha validation
│   └── bt_advanced.py      # Sector rotation, AUM growth, dual signal
├── Other
│   ├── kr_etfs.py          # Korean ETFs (32 ETFs, prototype)
│   ├── utils.py            # Logging, retry decorator
│   ├── test_basic.py       # Unit tests (11/11 pass)
│   ├── run_daily.py        # Integrated cron entry point
│   ├── landing/index.html  # Landing page
│   └── content/            # Auto-generated content (md + charts)
```

---

## Sample Output

### Daily Pulse (Korean)
```markdown
# 🌅 ETF Pulse — 2026-05-29

## 🎯 Market Mood
Strong: Theme ETFs (+0.46% avg)
Weak: Commodity/Hedge (-0.42% avg)

## 📈 Top 5 Returns
- MSTU (MSTR 2X Leveraged): +9.66%
- CIBR (Cybersecurity): +6.41%
- HACK (Cybersecurity): +5.83% ← same theme dual ★
- GDXJ (Junior Gold Miners): +3.88%
- TAN (Solar): +2.78%

## 🧠 Auto Insights
- 2 Cybersecurity ETFs up simultaneously → theme rotation
- ARK series 4 ETFs at 30-day highs → growth strong
- 4 Commodity ETFs at 30-day lows → cycle weakness
```

### Intent-Based Best (S&P 500)
```
🏦 Long-hold (fees/stability):  VOO (score 70, $1600B AUM)
⚡ Short-trade (volume/liquidity): SPY (score 82, $735B AUM)
🐤 Small account (low price):    SPLG (score 81, $89B AUM)
🚀 Momentum tracking:            SPY (5d momentum strong)
```

### Portfolio Analyzer
```
Portfolio: VOO 40% / IVV 30% / SPY 30%

📊 Diversification Score: 50/100
  Categories: 1, Weight diversity 66%, Concentration 1.9

⚠️ VOO and IVV very similar — consider consolidation
  Common: NVDA, AAPL, MSFT, AMZN, GOOGL (all 38%+ overlap)
```

---

## BT Validation (30-day backfill)

| Signal | 1d hold | 3d hold | 5d hold |
|--------|---------|---------|---------|
| SPY baseline | +0.22% | +0.71% | +1.19% |
| Volume spike Top 5 | -0.18% | -0.16% | +0.01% |
| 5-day momentum | +0.68% | +0.54% | -0.27% |
| Category rotation | +0.24% | +0.69% | +0.88% |
| Mean reversion in uptrend | **+0.66%, win 73.7%** | +0.31% | +0.97% |

**Note**: 30-day sample is small. SPY very strong in this period (bull market).
Long-term validation (1 year+) needed for true alpha.

---

## Pricing Model (planned)

| Tier | Price | Features |
|------|-------|----------|
| Free | $0 | Daily newsletter (email/Substack) |
| **Pro** | **$10-15/mo** | Portfolio tracking + alerts + 10y BT + AI chatbot |
| Premium | $30/mo | Walk-forward BT + custom categories + priority support |

---

## Roadmap

### Phase 1 ✅ MVP Complete
- 257 ETF universe + daily data
- 15+ modules (signals, content, analysis, publishing)
- Korean + English content
- Unit tests + GitHub Actions cron

### Phase 2 (1-2 months)
- 1-week dogfood + signal accuracy verification
- Substack launch (audience building)
- X/Twitter daily previews
- Telegram bot stabilization

### Phase 3 (3-6 months)
- Paid tier launch (portfolio + alerts)
- AI advisor (Claude API natural language)
- Korean ETFs full integration (KRX)
- Mobile-friendly web app

### Phase 4 (6+ months)
- B2B (asset managers, RIAs)
- Japan/Europe ETF expansion
- Educational content / courses

---

## Tech Stack

- **Data**: yfinance (single source, 90% coverage), Yahoo Finance
- **DB**: SQLite (lightweight, portable)
- **Content**: Markdown + Python templates
- **AI**: Claude API / Gemini API (fallback to template)
- **Visualization**: matplotlib (PNG export)
- **Publishing**: Telegram API, Gmail SMTP, Substack (manual)
- **Cron**: GitHub Actions or local cron
- **Integration**: MCP server for Claude Desktop

---

## Differentiation Summary

What ETF Pulse does that nobody else does:
1. **Auto pattern detection** — "Cybersecurity ETF 2 up + ARK 4 high" auto-summarized
2. **Category × Intent matrix** — VOO for long-hold, SPY for trading
3. **Portfolio overlap warning** — VOO+IVV redundancy auto-detected
4. **KR + US unified** — both markets in one newsletter (huge for Korean retail)
5. **EPS Momentum bridge** — links to stock-picking system

---

## Contact

- GitHub: https://github.com/VolumeQuant/eps-momentum-us
- Email: hello@etfpulse.kr (planned)
- Substack: TBA
