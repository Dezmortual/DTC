# DTC Trading Bot

An automated trading robot converted from the **DTC v1.36** TradingView (Pine Script v5)
indicator. It trades the exact signal logic of the indicator — no TradingView required —
and runs 24/7 as a free web service on Render, with a live dashboard.

## Strategy (identical to the Pine Script)

| Pine Script | Bot |
|---|---|
| 6 stacked EMAs (30/35/40/45/50/60) | ✔ computed on every closed candle |
| Bullish stack = ema30>ema35>ema40>ema45>ema50>ema60 → BUY | ✔ |
| Bearish stack (exact reverse) → SELL | ✔ |
| Signals only on confirmed (closed) bars | ✔ in-progress candle is always discarded |
| No-repeat (signal_state != 1 / != -1) | ✔ same state machine |
| ATR(14) filter, min ATR value | ✔ |
| Stop-loss % (default 0.25%) | ✔ |
| TP1–TP4 at SL distance × [1, 2, 3, 4] | ✔ scale-out: 25% of the position at each TP |
| Multi-timeframe EMA(20)/EMA(50) dashboard (15m/30m/1h/4h/1D) | ✔ shown on the bot dashboard |

Note: the original script computed a "stop-loss lookback" (lowest low / highest high) but
never used it — the SL is purely percentage-based, so the bot does the same.

**Trading behavior added by the bot (the indicator obviously can't do this):**
- Position sizing: risk % per trade, capped at max notional % of balance and a hard
  `$` cap (`DTC_MAX_POSITION_USDT`, default 1000)
- Opposite signal while a position is open → close and reverse
- 0.1% taker fee simulated per fill
- **Daily loss brake**: stop opening new trades if the day's realized P&L ≤ -4% (UTC day)
- **Kill-switch**: if the win rate over the last 20 closed trades drops below 35%,
  new entries pause for 48 hours (the bot still manages the open position)

## Modes

- **PAPER (default)** — simulated trading from a virtual balance of 10,000 USDT. Zero risk. Start here.
- **LIVE** — real spot market orders on Binance (LONG signals only; spot cannot short).
  Requires `BINANCE_API_KEY` / `BINANCE_API_SECRET` environment variables and
  spot trading enabled on your Binance account. Only switch after the paper run proves out.

## Run locally

```bash
pip install -r requirements.txt
python app.py
# open http://localhost:5000
```

## Deploy to Render (same pattern as your other bots)

1. Push this repo to GitHub (all files are in the repo root — `app.py`,
   `requirements.txt`, `README.md`, `render.yaml`).
2. Render → New → Web Service → connect the repo.
   - Build command: `pip install -r requirements.txt`
   - Start command: `gunicorn --workers 1 --threads 4 --timeout 60 --bind 0.0.0.0:$PORT app:app`
3. Add a **keep-alive pinger** (UptimeRobot or cron-job.org, every 10 minutes) hitting
   `https://YOUR-APP.onrender.com/health`. On the free tier Render sleeps idle web
   services — the pinger plus the built-in watchdog keeps the engine trading.
   (The bot is hardened for this: cycles run in generations with hard-bounded data
   fetches, so a stalled cycle can never freeze the engine, dashboard, or /api/run-now.)

`render.yaml` is included, so Render auto-fills the build/start commands.

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `DTC_MODE` | `PAPER` | `PAPER` or `LIVE` |
| `DTC_SYMBOL` | `BTCUSDT` | any Binance spot symbol |
| `DTC_TIMEFRAME` | `15m` | signal timeframe (the chart TF you'd run the indicator on) |
| `DTC_SL_PCT` | `0.25` | stop-loss % |
| `DTC_ATR_MIN` | `0.5` | min ATR value for the ATR filter |
| `DTC_RISK_PCT` | `1.0` | % of balance risked per trade |
| `DTC_MAX_NOTIONAL_PCT` | `100` | max position size as % of balance |
| `DTC_MAX_POSITION_USDT` | `1000` | hard $ cap per position |
| `DTC_START_BALANCE` | `10000` | paper starting balance |
| `DTC_KILL_SWITCH` | `1` | win-rate kill-switch on/off |
| `DTC_DAILY_BRAKE_PCT` | `-4` | daily loss brake |
| `BINANCE_API_KEY` / `BINANCE_API_SECRET` | — | required only for LIVE mode |

## Endpoints

- `/` — dashboard (auto-refreshes every 10s)
- `/health` — keep-alive / status check
- `/api/status` — full bot state as JSON
- `/api/run-now` — force a trading cycle immediately

## Disclaimer

Educational software, not financial advice. Crypto trading is risky — always validate in
PAPER mode first, and never allocate more than you can afford to lose.
