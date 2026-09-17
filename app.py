#!/usr/bin/env python3
"""
DTC Trading Bot v1.0 — automated trading robot converted from the
'DTC - v1.36' TradingView Pine Script indicator.

STRATEGY (faithful to the Pine Script):
  * 6 stacked EMAs (30/35/40/45/50/60) on the signal timeframe
  * LONG  when the EMA stack becomes perfectly bullish (ema30>ema35>ema40>ema45>ema50>ema60)
  * SHORT when the EMA stack becomes perfectly bearish (exact reverse)
  * Signals fire only on CLOSED candles, never twice in the same direction (no-repeat state)
  * ATR(14) filter: signals ignored while ATR <= min ATR value
  * Stop-loss: 0.25% from entry (configurable)
  * Take-profits TP1..TP4 at SL distance x [1, 2, 3, 4] — scale out 25% at each
  * Multi-timeframe EMA(20)/EMA(50) trend dashboard: 15m / 30m / 1h / 4h / 1D (informational)
  * Note: the original script's "SL lookback" (Tiny/Small/Mid/Large) input was computed
    but never actually used — the SL is purely percentage-based, so that's what this bot does.

OPERATIONAL HARDENING (proven on Render with the Medallion bot v4):
  * Cycles run in generations — a hung cycle is superseded, it can never block the loop,
    the watchdog, or /api/run-now
  * Every external data fetch is hard-bounded (20s) via a thread pool — covers DNS stalls
  * Watchdog on every web request kicks a stale cycle (throttled to 120s)
  * /health endpoint for external keep-alive pingers (UptimeRobot / cron-job.org)
  * Binance fallback endpoints (api.binance.com -> api1 -> data-api.binance.vision)

MODES:
  * PAPER (default): simulated trading with a virtual balance — zero risk, full dashboard
  * LIVE: real spot market orders on Binance (LONG signals only — spot cannot short).
          Requires BINANCE_API_KEY / BINANCE_API_SECRET env vars. Capped by
          DTC_MAX_POSITION_USDT. Use only after paper-trading proves the strategy.
"""

import os
import json
import copy
import time
import math
import hmac
import hashlib
import logging
import threading
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Flask, jsonify, request

# ============================== CONFIG ==============================

MODE             = os.environ.get("DTC_MODE", "PAPER").upper()     # PAPER | LIVE
SYMBOL           = os.environ.get("DTC_SYMBOL", "BTCUSDT")
TIMEFRAME        = os.environ.get("DTC_TIMEFRAME", "15m")         # signal timeframe (chart TF of the indicator)

# --- DTC indicator settings (defaults mirror the Pine inputs) ---
EMA_LENS         = [30, 35, 40, 45, 50, 60]                        # EMA 1..EMA 6
USE_ATR          = os.environ.get("DTC_USE_ATR", "1") == "1"      # "Enable ATR Filter?"
ATR_PERIOD       = int(os.environ.get("DTC_ATR_PERIOD", "14"))
ATR_MIN          = float(os.environ.get("DTC_ATR_MIN", "0.5"))    # min ATR value (absolute price units)

STOP_LOSS_PCT    = float(os.environ.get("DTC_SL_PCT", "0.25"))    # Stop Loss %
TP_MULTIPLIERS   = [1.0, 2.0, 3.0, 4.0]                           # TP1..TP4 multipliers of SL distance
TP_FRACTIONS     = [0.25, 0.25, 0.25, 0.25]                      # scale out 25% per TP

# --- Risk management ---
RISK_PCT         = float(os.environ.get("DTC_RISK_PCT", "1.0"))   # % of balance risked per trade
MAX_NOTIONAL_PCT = float(os.environ.get("DTC_MAX_NOTIONAL_PCT", "100"))  # max position as % of balance (no leverage above 1x)
MAX_POSITION_USDT= float(os.environ.get("DTC_MAX_POSITION_USDT", "1000"))  # hard $ cap per position
START_BALANCE    = float(os.environ.get("DTC_START_BALANCE", "10000"))    # paper starting balance (USDT)
FEE_PCT          = float(os.environ.get("DTC_FEE_PCT", "0.1"))   # taker fee % per fill
MIN_NOTIONAL_USDT= 10.0                                          # skip entries below this notional

# --- Safety rails ---
DAILY_LOSS_BRAKE_PCT = float(os.environ.get("DTC_DAILY_BRAKE_PCT", "-4"))  # stop opening trades for the UTC day
KILL_SWITCH          = os.environ.get("DTC_KILL_SWITCH", "1") == "1"
KILL_WINRATE_MIN     = float(os.environ.get("DTC_KILL_WINRATE_MIN", "0.35"))  # min win rate over window
KILL_WINDOW          = int(os.environ.get("DTC_KILL_WINDOW", "20"))         # last N closed trades
KILL_PAUSE_HOURS     = int(os.environ.get("DTC_KILL_PAUSE_HOURS", "48"))

# --- Multi-timeframe dashboard (informational, like the Pine table) ---
MTF_TIMEFRAMES   = ["15m", "30m", "1h", "4h", "1d"]
MTF_EMA_FAST     = 20
MTF_EMA_SLOW     = 50

# --- Engine ---
POLL_SECONDS          = int(os.environ.get("DTC_POLL_SECONDS", "60"))
BOOT_DELAY            = int(os.environ.get("DTC_BOOT_DELAY", "30"))
FETCH_TIMEOUT_S       = 20          # hard bound on any single data fetch (covers DNS)
WATCHDOG_STALE_S      = 5 * 60      # kick a cycle if none finished in this long
WATCHDOG_THROTTLE_S   = 20          # how often a web request is allowed to trigger the check
CANDLE_HISTORY        = 300
MTF_REFRESH_S         = 600

# --- Binance ---
BINANCE_BASES = [
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://data-api.binance.vision",   # public data mirror, works even where trading endpoints are geo-fenced
]
API_KEY    = os.environ.get("BINANCE_API_KEY", "")
API_SECRET = os.environ.get("BINANCE_API_SECRET", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dtc")

# ============================== INDICATOR MATH ==============================

def ema_series(values, period):
    """EMA with SMA seed (matches ta.ema closely; converges identically after ~3x period)."""
    n = len(values)
    if n < period:
        return [None] * n
    out = [None] * n
    out[period - 1] = sum(values[:period]) / period
    k = 2.0 / (period + 1)
    for i in range(period, n):
        out[i] = values[i] * k + out[i - 1] * (1 - k)
    return out


def atr_series(highs, lows, closes, period):
    """Wilder ATR (matches ta.atr)."""
    n = len(closes)
    if n == 0:
        return []
    trs = [highs[0] - lows[0]]
    for i in range(1, n):
        trs.append(max(highs[i] - lows[i],
                       abs(highs[i] - closes[i - 1]),
                       abs(lows[i] - closes[i - 1])))
    out = [None] * n
    if n < period:
        return out
    out[period - 1] = sum(trs[:period]) / period
    for i in range(period, n):
        out[i] = (out[i - 1] * (period - 1) + trs[i]) / period
    return out


def iso(ts=None):
    dt = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


# ============================== DATA FEED ==============================

_FETCH_POOL = ThreadPoolExecutor(max_workers=4)


def _http_get_json(url, params):
    r = requests.get(url, params=params, timeout=(5, 5))
    r.raise_for_status()
    return r.json()


def fetch_klines(symbol, interval, limit):
    """Fetch klines from Binance with fallback hosts and a HARD 20s bound (covers DNS stalls)."""
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    path = "/api/v3/klines"
    last_err = None
    for base in BINANCE_BASES:
        try:
            fut = _FETCH_POOL.submit(_http_get_json, base + path, params)
            raw = fut.result(timeout=FETCH_TIMEOUT_S)
            candles = []
            for row in raw:
                candles.append({
                    "open_time": int(row[0]),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                })
            return candles
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"klines fetch failed for {symbol} {interval}: {last_err}")


# ============================== LIVE TRADING (Binance spot) ==============================

_EXCH_CACHE = {"ts": 0, "step": None, "min_notional": None}


def _get_filters():
    if MODE != "LIVE":
        return None
    now = time.time()
    if _EXCH_CACHE["step"] and now - _EXCH_CACHE["ts"] < 3600:
        return _EXCH_CACHE
    try:
        fut = _FETCH_POOL.submit(_http_get_json,
                                 BINANCE_BASES[0] + "/api/v3/exchangeInfo",
                                 {"symbol": SYMBOL})
        info = fut.result(timeout=FETCH_TIMEOUT_S)
        f = info["symbols"][0]["filters"]
        step = None
        min_notional = 10.0
        for flt in f:
            if flt["filterType"] == "LOT_SIZE":
                step = float(flt["stepSize"])
            elif flt["filterType"] in ("NOTIONAL", "MIN_NOTIONAL"):
                try:
                    min_notional = float(flt.get("minNotional") or flt.get("minNotional"))
                except Exception:
                    pass
        _EXCH_CACHE.update({"ts": now, "step": step, "min_notional": min_notional})
    except Exception as e:
        log.warning(f"exchangeInfo fetch failed, using defaults: {e}")
    return _EXCH_CACHE


def _round_step(qty, step):
    if not step or step <= 0:
        return qty
    return math.floor(qty / step) * step


def _signed_request(method, path, params):
    params = dict(params)
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 10000
    qs = urlencode(params)
    sig = hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    url = f"{BINANCE_BASES[0]}{path}?{qs}&signature={sig}"
    r = requests.request(method, url, headers={"X-MBX-APIKEY": API_KEY}, timeout=(5, 10))
    if r.status_code >= 400:
        raise RuntimeError(f"Binance {r.status_code}: {r.text[:200]}")
    return r.json()


def execute_entry(side, price, notional):
    """Buy (open) a position. PAPER simulates at `price`; LIVE places a real spot market order."""
    if MODE != "LIVE":
        return {"ok": True, "units": notional / price, "price": price,
                "fee": notional * FEE_PCT / 100.0}
    if side == -1:
        return {"ok": False, "reason": "LIVE spot mode takes LONG signals only (short logged)"}
    if not API_KEY or not API_SECRET:
        return {"ok": False, "reason": "BINANCE_API_KEY / BINANCE_API_SECRET not configured"}
    try:
        o = _signed_request("POST", "/api/v3/order", {
            "symbol": SYMBOL, "side": "BUY", "type": "MARKET",
            "quoteOrderQty": f"{notional:.2f}",
        })
        units = float(o.get("executedQty") or 0)
        quote = float(o.get("cummulativeQuoteQty") or 0)
        if units <= 0 or quote <= 0:
            return {"ok": False, "reason": f"bad fill: {json.dumps(o)[:200]}"}
        return {"ok": True, "units": units, "price": quote / units,
                "fee": quote * FEE_PCT / 100.0, "order_id": o.get("orderId")}
    except Exception as e:
        return {"ok": False, "reason": str(e)[:200]}


def execute_exit(units, price):
    """Sell (close part of) a position."""
    if MODE != "LIVE":
        return {"ok": True, "units": units, "price": price,
                "fee": units * price * FEE_PCT / 100.0}
    if not API_KEY or not API_SECRET:
        return {"ok": False, "reason": "API keys not configured"}
    filt = _get_filters() or {}
    qty = _round_step(units, filt.get("step") or 0.0)
    if qty <= 0:
        return {"ok": False, "reason": "qty rounds to zero"}
    try:
        o = _signed_request("POST", "/api/v3/order", {
            "symbol": SYMBOL, "side": "SELL", "type": "MARKET",
            "quantity": f"{qty:.8f}".rstrip("0").rstrip("."),
        })
        filled = float(o.get("executedQty") or 0)
        quote = float(o.get("cummulativeQuoteQty") or 0)
        if filled <= 0:
            return {"ok": False, "reason": f"bad fill: {json.dumps(o)[:200]}"}
        return {"ok": True, "units": filled, "price": quote / filled,
                "fee": quote * FEE_PCT / 100.0, "order_id": o.get("orderId")}
    except Exception as e:
        return {"ok": False, "reason": str(e)[:200]}


# ============================== STATE ==============================

def fresh_state():
    return {
        "mode": MODE,
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "balance": START_BALANCE,
        "equity": START_BALANCE,
        "position": None,
        "trades": [],            # closed trade records
        "signals": [],           # signal log (last 50)
        "signal_state": 0,       # 1=long, -1=short, 0=none (no-repeat, mirrors Pine)
        "last_candle_open_time": 0,
        "chart": None,           # last closed candle snapshot: EMAs, trend, ATR
        "mtf": {},               # multi-timeframe trends
        "mtf_ts": 0,
        "day": {"date": "", "start_equity": START_BALANCE, "realized": 0.0, "braked": False},
        "kill_until": None,
        "kill_reason": None,
        "data_feed_status": "starting",
        "last_error": None,
        "last_price": None,
        "equity_curve": [],
        "price_history": [],
        "last_cycle_ts": 0,
        "cycle_count": 0,
    }


STATE = fresh_state()

# ============================== TRADING LOGIC ==============================

def _entry_blocked(st):
    now = datetime.now(timezone.utc)
    if st["kill_until"]:
        try:
            if datetime.fromisoformat(st["kill_until"].replace(" UTC", "+00:00")) > now:
                return True, f"kill-switch active until {st['kill_until']}"
        except Exception:
            pass
    if st["day"]["braked"]:
        return True, f"daily loss brake hit ({DAILY_LOSS_BRAKE_PCT}% day) — resumes next UTC day"
    if MODE == "LIVE" and (not API_KEY or not API_SECRET):
        return True, "LIVE mode without API keys"
    return False, None


def _day_rollover(st):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if st["day"]["date"] != today:
        st["day"] = {"date": today, "start_equity": st["balance"], "realized": 0.0, "braked": False}


def _kill_check(st):
    if not KILL_SWITCH:
        return
    trades = st["trades"][-KILL_WINDOW:]
    if len(trades) < KILL_WINDOW:
        return
    wins = sum(1 for t in trades if t["pnl"] > 0)
    if wins / len(trades) < KILL_WINRATE_MIN:
        until = datetime.now(timezone.utc) + timedelta(hours=KILL_PAUSE_HOURS)
        st["kill_until"] = until.strftime("%Y-%m-%d %H:%M:%S UTC")
        st["kill_reason"] = (f"win rate {wins}/{len(trades)} below "
                             f"{KILL_WINRATE_MIN:.0%} over last {KILL_WINDOW} trades")
        log.warning(f"KILL-SWITCH engaged: {st['kill_reason']}")


def _open_position(st, side, candle):
    price = candle["close"]
    eq = st["balance"]
    sl_frac = STOP_LOSS_PCT / 100.0
    if sl_frac > 0:
        risk_notional = eq * (RISK_PCT / 100.0) / sl_frac
    else:
        risk_notional = eq
    notional = min(eq * MAX_NOTIONAL_PCT / 100.0, MAX_POSITION_USDT, risk_notional)
    notional = max(0.0, min(notional, eq))
    if notional < MIN_NOTIONAL_USDT:
        log.info(f"entry skipped: notional {notional:.2f} below minimum")
        return
    fill = execute_entry(side, price, notional)
    if not fill["ok"]:
        if st["signals"]:
            st["signals"][-1]["blocked"] = True
            st["signals"][-1]["reason"] = fill["reason"]
        log.warning(f"entry failed: {fill['reason']}")
        return
    entry_price = fill["price"]
    units = fill["units"]
    st["balance"] -= fill["fee"]
    if side == 1:
        sl = entry_price * (1 - sl_frac)
        tps = [entry_price * (1 + STOP_LOSS_PCT * m / 100.0) for m in TP_MULTIPLIERS]
    else:
        sl = entry_price * (1 + sl_frac)
        tps = [entry_price * (1 - STOP_LOSS_PCT * m / 100.0) for m in TP_MULTIPLIERS]
    st["position"] = {
        "side": side,
        "entry_price": entry_price,
        "entry_time": iso(candle["open_time"] / 1000),
        "units": units,
        "remaining": units,
        "entry_notional": units * entry_price,
        "sl": sl,
        "tps": tps,
        "tps_hit": [False] * len(tps),
        "realized": -fill["fee"],
        "fees": fill["fee"],
        "partial_fills": [],
    }
    log.info(f"OPEN {'LONG' if side==1 else 'SHORT'} {units:.6f} {SYMBOL} @ {entry_price:.2f} "
             f"SL {sl:.2f} TP1..4 {['%.2f' % t for t in tps]}")


def _exit_tranche(st, units, price, tag):
    pos = st["position"]
    if not pos or units <= 0:
        return 0.0
    units = min(units, pos["remaining"])
    # LIVE: if the tranche is below the exchange minimum, just close everything left
    if MODE == "LIVE":
        filt = _get_filters() or {}
        min_notional = filt.get("min_notional") or MIN_NOTIONAL_USDT
        if units * price < min_notional:
            units = pos["remaining"]
    fill = execute_exit(units, price)
    if not fill["ok"]:
        log.warning(f"exit failed ({tag}): {fill['reason']}")
        return 0.0
    px, u, fee = fill["price"], fill["units"], fill["fee"]
    pnl = (px - pos["entry_price"]) * u * pos["side"] - fee
    st["balance"] += pnl
    st["day"]["realized"] += pnl
    pos["realized"] += pnl
    pos["fees"] += fee
    pos["remaining"] -= u
    pos["partial_fills"].append({"tag": tag, "units": u, "price": px, "pnl": round(pnl, 4),
                                 "time": iso()})
    if st["day"]["realized"] <= st["day"]["start_equity"] * DAILY_LOSS_BRAKE_PCT / 100.0:
        if not st["day"]["braked"]:
            st["day"]["braked"] = True
            log.warning(f"DAILY LOSS BRAKE hit: {st['day']['realized']:.2f} USDT today")
    return pnl


def _finalize_position(st, reason):
    pos = st["position"]
    if not pos:
        return
    fills = pos["partial_fills"]
    avg_exit = (sum(f["price"] * f["units"] for f in fills) / sum(f["units"] for f in fills)) if fills else pos["entry_price"]
    record = {
        "side": "LONG" if pos["side"] == 1 else "SHORT",
        "entry_time": pos["entry_time"],
        "entry_price": pos["entry_price"],
        "exit_time": iso(),
        "avg_exit": round(avg_exit, 2),
        "units": pos["units"],
        "pnl": round(pos["realized"], 4),
        "fees": round(pos["fees"], 4),
        "tps_hit": sum(pos["tps_hit"]),
        "reason": reason,
    }
    st["trades"].append(record)
    st["position"] = None
    log.info(f"CLOSE {record['side']} pnl {record['pnl']:+.2f} USDT ({reason})")
    _kill_check(st)


def _manage_position(st, candle):
    """SL/TP management against a closed candle. Conservative: SL checked first."""
    pos = st["position"]
    if not pos:
        return
    # 1) Stop-loss (full exit)
    if (pos["side"] == 1 and candle["low"] <= pos["sl"]) or \
       (pos["side"] == -1 and candle["high"] >= pos["sl"]):
        _exit_tranche(st, pos["remaining"], pos["sl"], "SL")
        _finalize_position(st, "stop-loss")
        return
    # 2) Take-profits (scale out, in order)
    for i, tp in enumerate(pos["tps"]):
        if pos["tps_hit"][i]:
            continue
        hit = (pos["side"] == 1 and candle["high"] >= tp) or \
              (pos["side"] == -1 and candle["low"] <= tp)
        if not hit:
            break  # TPs are ordered; if this one wasn't hit, later ones weren't either
        pos["tps_hit"][i] = True
        _exit_tranche(st, pos["units"] * TP_FRACTIONS[i], tp, f"TP{i+1}")
        if pos and pos["remaining"] <= pos["units"] * 0.02:
            _exit_tranche(st, pos["remaining"], tp, f"TP{i+1}-dust")
            _finalize_position(st, "all TPs hit")
            return


def _on_signal(st, side, candle):
    """New confirmed signal: mirrors Pine state update, then trades it (unless blocked)."""
    st["signal_state"] = side
    blocked, reason = _entry_blocked(st)
    st["signals"].append({
        "time": iso(candle["open_time"] / 1000),
        "candle_close": candle["close"],
        "side": "LONG" if side == 1 else "SHORT",
        "entry": candle["close"],
        "blocked": blocked,
        "reason": reason,
    })
    st["signals"] = st["signals"][-50:]
    log.info(f"{'BLOCKED ' + reason if blocked else ''}"
             f"{'LONG' if side==1 else 'SHORT'} signal @ {candle['close']:.2f}")
    if blocked:
        return
    pos = st["position"]
    if pos and pos["side"] != side:
        _exit_tranche(st, pos["remaining"], candle["close"], "reverse")
        _finalize_position(st, "opposite signal (reverse)")
    if not st["position"]:
        _open_position(st, side, candle)


def _process_new_candles(st, closed):
    """Replay every newly closed candle in order: manage position, then evaluate signals."""
    closes = [c["close"] for c in closed]
    highs = [c["high"] for c in closed]
    lows = [c["low"] for c in closed]
    emas = [ema_series(closes, L) for L in EMA_LENS]
    atrs = atr_series(highs, lows, closes, ATR_PERIOD)
    n = len(closed)

    bull = [False] * n
    bear = [False] * n
    for i in range(n):
        e = [emas[k][i] for k in range(len(EMA_LENS))]
        if any(v is None for v in e):
            continue
        bull[i] = all(e[k] > e[k + 1] for k in range(len(e) - 1))
        bear[i] = all(e[k] < e[k + 1] for k in range(len(e) - 1))

    start = 0
    if st["last_candle_open_time"]:
        while start < n and closed[start]["open_time"] <= st["last_candle_open_time"]:
            start += 1

    for i in range(start, n):
        c = closed[i]
        _manage_position(st, c)
        atr_ok = (not USE_ATR) or (atrs[i] is not None and atrs[i] > ATR_MIN)
        prev_bull = bull[i - 1] if i > 0 else False
        prev_bear = bear[i - 1] if i > 0 else False
        if not prev_bull and bull[i] and st["signal_state"] != 1 and atr_ok:
            _on_signal(st, 1, c)
        if not prev_bear and bear[i] and st["signal_state"] != -1 and atr_ok:
            _on_signal(st, -1, c)
        st["last_candle_open_time"] = c["open_time"]

    last = n - 1
    if last >= 0:
        st["price_history"] = [
            {"t": closed[i]["open_time"] // 1000, "c": closed[i]["close"]}
            for i in range(max(0, n - 120), n)
        ]
        st["chart"] = {
            "candle_time": iso(closed[last]["open_time"] / 1000),
            "close": closes[last],
            "emas": {f"ema{L}": (round(emas[k][last], 2) if emas[k][last] else None)
                      for k, L in enumerate(EMA_LENS)},
            "trend": "bullish" if bull[last] else "bearish" if bear[last] else "none",
            "atr": round(atrs[last], 4) if atrs[last] else None,
            "atr_ok": (not USE_ATR) or (atrs[last] is not None and atrs[last] > ATR_MIN),
        }


def _update_mtf(st):
    mtf = dict(st["mtf"] or {})
    for tf in MTF_TIMEFRAMES:
        try:
            kl = fetch_klines(SYMBOL, tf, 150)
            closed = kl[:-1]
            closes = [c["close"] for c in closed]
            e_fast = ema_series(closes, MTF_EMA_FAST)
            e_slow = ema_series(closes, MTF_EMA_SLOW)
            if e_fast[-1] is not None and e_slow[-1] is not None:
                mtf[tf] = bool(e_fast[-1] > e_slow[-1])
        except Exception as e:
            log.warning(f"MTF {tf} fetch failed: {e}")
    st["mtf"] = mtf
    st["mtf_ts"] = time.time()


def _unrealized(st, price):
    pos = st["position"]
    if not pos or price is None:
        return 0.0
    gross = (price - pos["entry_price"]) * pos["remaining"] * pos["side"]
    fee = pos["remaining"] * price * FEE_PCT / 100.0
    return gross - fee


# ============================== CYCLE ENGINE (generations) ==============================

_GEN = {"n": 0}
_GEN_LOCK = threading.Lock()
_KICK_LOCK = threading.Lock()
_cycle_running = False
_cycle_started = 0
_LOOP_HB = 0.0
_WATCHDOG_LAST = 0.0


def _run_generation():
    """Work on a private deepcopy of state; commit only if still the newest generation.
    A hung/slow cycle is superseded and never blocks anything."""
    global STATE
    with _GEN_LOCK:
        _GEN["n"] += 1
        gen = _GEN["n"]
    st = copy.deepcopy(STATE)
    try:
        _day_rollover(st)
        kl = fetch_klines(SYMBOL, TIMEFRAME, CANDLE_HISTORY)
        closed = kl[:-1]  # drop the in-progress candle — signals need barstate.isconfirmed
        if len(closed) >= EMA_LENS[-1] + 2:
            st["data_feed_status"] = "ok"
            st["last_error"] = None
            _process_new_candles(st, closed)
            st["last_price"] = closed[-1]["close"]
            st["equity"] = st["balance"] + _unrealized(st, closed[-1]["close"])
            st["equity_curve"].append({"t": int(time.time()), "eq": round(st["equity"], 2)})
            st["equity_curve"] = st["equity_curve"][-500:]
        if not st["mtf"] or time.time() - st.get("mtf_ts", 0) > MTF_REFRESH_S:
            _update_mtf(st)
        st["last_cycle_ts"] = time.time()
        st["cycle_count"] += 1
    except Exception as e:
        st["data_feed_status"] = "error"
        st["last_error"] = str(e)[:300]
        st["last_cycle_ts"] = time.time()
        st["cycle_count"] += 1
        log.warning(f"cycle error: {e}")
    with _GEN_LOCK:
        if _GEN["n"] == gen:
            STATE = st


def _cycle_worker():
    global _cycle_running, _LOOP_HB
    try:
        _LOOP_HB = time.time()
        _run_generation()
    except Exception:
        logging.exception("cycle worker crashed")
    finally:
        _LOOP_HB = time.time()
        _cycle_running = False


def kick_cycle(force=False):
    """Start a cycle thread unless one is already running (and not hopelessly stale)."""
    global _cycle_running, _cycle_started, _WATCHDOG_LAST
    with _KICK_LOCK:
        fresh = time.time() - _cycle_started < 300
        if _cycle_running and fresh and not force:
            return False
        _cycle_running = True
        _cycle_started = time.time()
    threading.Thread(target=_cycle_worker, daemon=True).start()
    return True


def _main_loop():
    log.info(f"DTC bot engine: mode={MODE} symbol={SYMBOL} timeframe={TIMEFRAME} "
             f"(boot delay {BOOT_DELAY}s)")
    time.sleep(BOOT_DELAY)
    while True:
        try:
            kick_cycle()
        except Exception:
            logging.exception("main loop tick failed")
        time.sleep(POLL_SECONDS)


threading.Thread(target=_main_loop, daemon=True).start()

# ============================== FLASK APP ==============================

app = Flask(__name__)


@app.before_request
def _watchdog():
    """Any web request kicks a stale cycle back to life (throttled)."""
    global _WATCHDOG_LAST
    now = time.time()
    if now - _WATCHDOG_LAST < WATCHDOG_THROTTLE_S:
        return
    _WATCHDOG_LAST = now
    if now - STATE.get("last_cycle_ts", 0) > WATCHDOG_STALE_S:
        log.info("watchdog: kicking stale cycle")
        kick_cycle()


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "mode": MODE,
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "loop_alive": (time.time() - _LOOP_HB) < max(WATCHDOG_STALE_S + 60, POLL_SECONDS * 6),
        "cycle_running": _cycle_running,
        "data_feed": STATE.get("data_feed_status"),
        "last_cycle_age_s": int(time.time() - STATE.get("last_cycle_ts", 0)),
        "cycles": STATE.get("cycle_count", 0),
    })


@app.route("/api/status")
def api_status():
    st = STATE
    trades = st["trades"]
    wins = sum(1 for t in trades if t["pnl"] > 0)
    gross_win = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    gross_loss = abs(sum(t["pnl"] for t in trades if t["pnl"] <= 0))
    payload = {k: st[k] for k in (
        "mode", "symbol", "timeframe", "balance", "equity", "position",
        "signals", "chart", "mtf", "day", "kill_until", "kill_reason",
        "data_feed_status", "last_error", "last_price", "last_cycle_ts", "cycle_count",
        "equity_curve", "price_history",
    )}
    payload["start_balance"] = START_BALANCE
    payload["stats"] = {
        "closed_trades": len(trades),
        "wins": wins,
        "win_rate": round(wins / len(trades) * 100, 1) if trades else None,
        "total_pnl": round(sum(t["pnl"] for t in trades), 2),
        "profit_factor": round(gross_win / gross_loss, 2) if gross_loss > 0 else None,
        "unrealized": round(_unrealized(st, st["last_price"]), 2),
        "config": {
            "ema_lens": EMA_LENS, "atr_period": ATR_PERIOD, "atr_min": ATR_MIN,
            "use_atr": USE_ATR, "sl_pct": STOP_LOSS_PCT, "tp_mults": TP_MULTIPLIERS,
            "risk_pct": RISK_PCT, "max_notional_pct": MAX_NOTIONAL_PCT,
            "max_position_usdt": MAX_POSITION_USDT, "kill_switch": KILL_SWITCH,
            "daily_brake_pct": DAILY_LOSS_BRAKE_PCT,
        },
        "loop_alive": (time.time() - _LOOP_HB) < max(WATCHDOG_STALE_S + 60, POLL_SECONDS * 6),
    }
    payload["recent_trades"] = trades[-20:][::-1]
    return jsonify(payload)


@app.route("/api/run-now", methods=["POST", "GET"])
def run_now():
    if kick_cycle(force=True):
        return jsonify({"ok": True, "msg": "cycle started"}), 202
    return jsonify({"ok": False, "msg": "cycle already running"}), 429


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DTC Trading Bot</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;700&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
<style>
  :root{
    --bg:#070a12; --card:rgba(255,255,255,.032); --card2:rgba(255,255,255,.05);
    --line:rgba(255,255,255,.07); --txt:#e8edf7; --mut:#7f8aa3; --dim:#5a6478;
    --green:#22c55e; --red:#f43f5e; --blue:#6ea8fe; --gold:#f7c948; --violet:#a78bfa;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  html{background:var(--bg)}
  body{background:
      radial-gradient(900px 500px at 8% -10%, rgba(110,168,254,.13), transparent 60%),
      radial-gradient(800px 500px at 95% -5%, rgba(167,139,250,.11), transparent 60%),
      radial-gradient(900px 600px at 50% 110%, rgba(34,197,94,.07), transparent 60%),
      var(--bg);
    color:var(--txt); font-family:'Inter',system-ui,sans-serif; padding:24px 20px 40px; min-height:100vh;}
  .wrap{max-width:1180px;margin:0 auto}
  .mono{font-family:'JetBrains Mono',monospace;font-variant-numeric:tabular-nums}
  /* ---------- nav ---------- */
  nav{display:flex;align-items:center;justify-content:space-between;gap:14px;flex-wrap:wrap;margin-bottom:22px}
  .brand{display:flex;align-items:center;gap:14px}
  .logo{width:46px;height:46px;border-radius:14px;display:flex;align-items:center;justify-content:center;
    background:linear-gradient(135deg,#f7c948,#f59e0b);color:#161003;font-weight:800;font-size:15px;letter-spacing:.5px;
    box-shadow:0 6px 24px rgba(247,201,72,.35)}
  .brand h1{font-size:19px;font-weight:800;letter-spacing:.3px}
  .brand .sub{font-size:12px;color:var(--mut);margin-top:1px}
  .navright{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
  .pill{display:inline-flex;align-items:center;gap:7px;padding:6px 13px;border-radius:99px;font-size:12px;font-weight:600;
    border:1px solid var(--line);background:var(--card);color:var(--mut)}
  .dot{width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 10px var(--green);animation:pulse 2s infinite}
  .dot.bad{background:var(--red);box-shadow:0 0 10px var(--red)}
  @keyframes pulse{50%{opacity:.45}}
  .badge{padding:5px 12px;border-radius:99px;font-size:11px;font-weight:700;letter-spacing:1px}
  .b-live{background:rgba(244,63,94,.15);color:var(--red);border:1px solid rgba(244,63,94,.4)}
  .b-paper{background:rgba(110,168,254,.13);color:var(--blue);border:1px solid rgba(110,168,254,.35)}
  .price{padding:6px 14px;border-radius:99px;background:var(--card);border:1px solid var(--line)}
  .price b{color:var(--gold)}
  button{cursor:pointer;border:none;border-radius:99px;padding:9px 18px;font-size:12.5px;font-weight:700;color:#fff;
    background:linear-gradient(135deg,#6ea8fe,#7c6cf6);box-shadow:0 4px 18px rgba(110,140,254,.35);transition:.2s}
  button:hover{transform:translateY(-1px);filter:brightness(1.1)}
  /* ---------- cards ---------- */
  .card{background:var(--card);border:1px solid var(--line);border-radius:18px;padding:20px;
    backdrop-filter:blur(14px);box-shadow:0 10px 40px rgba(0,0,0,.35)}
  .card h2{font-size:11.5px;color:var(--mut);text-transform:uppercase;letter-spacing:1.6px;font-weight:700;margin-bottom:14px;
    display:flex;align-items:center;justify-content:space-between}
  /* ---------- hero ---------- */
  .hero{display:grid;grid-template-columns:340px 1fr;gap:26px;align-items:center;margin-bottom:16px}
  @media(max-width:880px){.hero{grid-template-columns:1fr}}
  .hero .k{font-size:12px;color:var(--mut);text-transform:uppercase;letter-spacing:2px;font-weight:700}
  .hero .big{font-size:46px;font-weight:800;letter-spacing:-1px;margin:6px 0 10px}
  .delta{display:inline-flex;align-items:center;gap:6px;padding:5px 12px;border-radius:99px;font-size:12.5px;font-weight:700}
  .d-up{background:rgba(34,197,94,.13);color:var(--green)}
  .d-down{background:rgba(244,63,94,.13);color:var(--red)}
  .hero-sub{font-size:12px;color:var(--mut);margin-top:12px;line-height:1.8}
  /* ---------- tiles ---------- */
  .tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(165px,1fr));gap:14px;margin-bottom:16px}
  .tile{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:16px 18px;backdrop-filter:blur(14px);
    transition:.2s}
  .tile:hover{background:var(--card2)}
  .tile .k{font-size:10.5px;color:var(--mut);text-transform:uppercase;letter-spacing:1.5px;font-weight:700}
  .tile .v{font-size:23px;font-weight:800;margin-top:6px}
  .tile .s{font-size:11px;color:var(--dim);margin-top:3px}
  .pos{color:var(--green)} .neg{color:var(--red)}
  .grid2{display:grid;grid-template-columns:1.25fr 1fr;gap:16px;margin-bottom:16px}
  @media(max-width:880px){.grid2{grid-template-columns:1fr}}
  .chartbox{position:relative;height:260px}
  .chip{display:inline-block;padding:2px 10px;border-radius:99px;font-size:11px;font-weight:700}
  .c-long{background:rgba(34,197,94,.15);color:var(--green)}
  .c-short{background:rgba(244,63,94,.15);color:var(--red)}
  .c-block{background:rgba(127,138,163,.15);color:var(--mut)}
  /* ---------- ladder ---------- */
  .ladder{display:flex;flex-direction:column;gap:12px}
  .lrow{display:grid;grid-template-columns:44px 78px 1fr 22px;gap:10px;align-items:center;font-size:12px}
  .lrow .lbl{font-weight:700;color:var(--mut);font-size:11px}
  .bar{height:9px;border-radius:99px;background:rgba(255,255,255,.06);overflow:hidden}
  .bar i{display:block;height:100%;border-radius:99px;transition:width .6s ease}
  .i-tp i{background:linear-gradient(90deg,#16a34a,#4ade80);box-shadow:0 0 12px rgba(34,197,94,.5)}
  .i-sl i{background:linear-gradient(90deg,#be123c,#f43f5e);box-shadow:0 0 12px rgba(244,63,94,.5)}
  .tick{font-weight:700}
  .npos{color:var(--dim);font-size:13px;line-height:1.7;padding:14px 0;text-align:center}
  .npos b{color:var(--mut);display:block;font-size:15px;margin-bottom:4px}
  /* ---------- mtf ---------- */
  .mtf{display:grid;grid-template-columns:repeat(5,1fr);gap:10px}
  @media(max-width:880px){.mtf{grid-template-columns:repeat(3,1fr)}}
  .mtfcell{border:1px solid var(--line);border-radius:14px;padding:14px 10px;text-align:center;background:rgba(255,255,255,.02)}
  .mtfcell .tf{font-size:11px;color:var(--mut);font-weight:700;letter-spacing:1px}
  .mtfcell .trend{font-size:13px;font-weight:800;margin-top:7px}
  .bull{color:var(--green)} .bear{color:var(--red)} .na{color:var(--dim)}
  .arrow{font-size:16px;margin-right:3px}
  /* ---------- tables ---------- */
  table{width:100%;border-collapse:collapse;font-size:12.5px}
  th{text-align:left;color:var(--mut);font-weight:600;font-size:10.5px;text-transform:uppercase;letter-spacing:1.2px;
    padding:7px 9px;border-bottom:1px solid var(--line)}
  td{padding:8px 9px;border-bottom:1px solid rgba(255,255,255,.04)}
  tr:last-child td{border-bottom:none}
  .card.wide{margin-bottom:16px}
  footer{color:var(--dim);font-size:11.5px;margin-top:6px;line-height:2}
  footer a{color:var(--blue);text-decoration:none}
  .banner{background:rgba(244,63,94,.12);border:1px solid rgba(244,63,94,.45);color:#ffd7de;padding:11px 16px;
    border-radius:12px;margin-bottom:16px;font-size:12.5px;font-weight:600}
  .toprow{display:flex;justify-content:space-between;align-items:baseline;gap:8px}
  .readout{display:flex;flex-wrap:wrap;gap:7px;margin-top:4px}
  .kv{border:1px solid var(--line);border-radius:10px;padding:7px 11px;font-size:11.5px;background:rgba(255,255,255,.02)}
  .kv b{color:var(--txt)} .kv{color:var(--mut)}
  ::-webkit-scrollbar{width:8px;height:8px}
  ::-webkit-scrollbar-thumb{background:rgba(255,255,255,.1);border-radius:99px}
</style>
</head>
<body>
<div class="wrap">
  <nav>
    <div class="brand">
      <div class="logo">DTC</div>
      <div>
        <h1>DTC Trading Bot</h1>
        <div class="sub" id="sub">connecting…</div>
      </div>
    </div>
    <div class="navright">
      <span class="pill"><span class="dot" id="feeddot"></span><span id="feed">FEED —</span></span>
      <span class="price mono" id="ticker">BTC —</span>
      <span class="badge b-paper" id="mode">PAPER</span>
      <button onclick="fetch('/api/run-now',{method:'POST'}).then(tickSoon)">Run cycle</button>
    </div>
  </nav>
  <div id="banner"></div>

  <div class="card hero">
    <div>
      <div class="k">Paper equity</div>
      <div class="big mono" id="equity">$—</div>
      <span class="delta d-up" id="delta">—</span>
      <div class="hero-sub" id="herosub"></div>
    </div>
    <div class="chartbox"><canvas id="equityChart"></canvas></div>
  </div>

  <div class="tiles" id="tiles"></div>

  <div class="grid2">
    <div class="card">
      <h2><span>Price · entry / SL / TP map</span><span class="sub" id="pricetrend" style="text-transform:none;letter-spacing:0"></span></h2>
      <div class="chartbox"><canvas id="priceChart"></canvas></div>
    </div>
    <div class="card">
      <h2><span>Open position</span><span id="posbadge"></span></h2>
      <div id="position"></div>
    </div>
  </div>

  <div class="grid2">
    <div class="card">
      <h2>Multi-timeframe trend · EMA20 / EMA50</h2>
      <div class="mtf" id="mtf"></div>
    </div>
    <div class="card">
      <h2>Signal engine</h2>
      <div id="engine"></div>
    </div>
  </div>

  <div class="card wide">
    <h2>Signals</h2>
    <table id="signals"></table>
  </div>

  <div class="card wide">
    <h2>Closed trades</h2>
    <table id="trades"></table>
  </div>

  <footer id="footer"></footer>
</div>

<script>
const fmt=(x,d=2)=>x===null||x===undefined?'—':Number(x).toLocaleString('en-US',{maximumFractionDigits:d,minimumFractionDigits:d});
const sgn=x=>(x>0?'pos':(x<0?'neg':''));
let charts={};
Chart.defaults.color='#7f8aa3';
Chart.defaults.font.family="'Inter',sans-serif";
Chart.defaults.borderColor='rgba(255,255,255,.05)';

function makeEquityChart(){
  const g=document.getElementById('equityChart').getContext('2d');
  const grad=g.createLinearGradient(0,0,0,220);
  grad.addColorStop(0,'rgba(247,201,72,.35)'); grad.addColorStop(1,'rgba(247,201,72,0)');
  charts.eq=new Chart(g,{type:'line',data:{labels:[],datasets:[{data:[],borderColor:'#f7c948',borderWidth:2.5,
    backgroundColor:grad,fill:true,pointRadius:0,tension:.35}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},
      scales:{x:{ticks:{maxTicksLimit:6,font:{size:10}}},y:{ticks:{font:{size:10}}}}}});
}
function makePriceChart(){
  const g=document.getElementById('priceChart').getContext('2d');
  charts.px=new Chart(g,{type:'line',data:{labels:[],datasets:[{label:'Price',data:[],borderColor:'#6ea8fe',
    borderWidth:2,pointRadius:0,tension:.25,fill:false}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},
      scales:{x:{ticks:{maxTicksLimit:7,font:{size:10}}},y:{ticks:{font:{size:10}}}}}});
}
function addHLine(chart,label,value,color,dash){
  chart.data.datasets.push({label,data:Array(chart.data.labels.length).fill(value),borderColor:color,
    borderWidth:1.6,borderDash:dash,pointRadius:0,fill:false});
}
function hhmm(t){const d=new Date(t*1000);return d.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});}

function tickSoon(){setTimeout(tick,600);}

async function tick(){
  try{
    const r=await fetch('/api/status'); const s=await r.json();
    document.getElementById('sub').textContent=s.symbol+' · '+s.timeframe+' · '+s.mode.toLowerCase()+' trading';
    document.getElementById('mode').textContent=s.mode;
    document.getElementById('mode').className='badge '+(s.mode==='LIVE'?'b-live':'b-paper');
    document.getElementById('feed').textContent='FEED '+s.data_feed_status.toUpperCase();
    document.getElementById('feeddot').className='dot'+(s.data_feed_status==='ok'?'':' bad');
    document.getElementById('ticker').innerHTML=s.symbol.replace('USDT','')+' <b>$'+fmt(s.last_price)+'</b>';
    let b='';
    if(s.data_feed_status==='error') b+='<div class="banner">Data feed error: '+(s.last_error||'')+'</div>';
    if(s.kill_until) b+='<div class="banner">KILL-SWITCH: '+(s.kill_reason||'')+' — until '+s.kill_until+'</div>';
    if(s.day&&s.day.braked) b+='<div class="banner">Daily loss brake active — resumes next UTC day</div>';
    document.getElementById('banner').innerHTML=b;

    // hero
    const eq=s.equity, up=eq>=s.start_balance, pct=(eq/s.start_balance-1)*100;
    document.getElementById('equity').textContent='$'+fmt(eq);
    const d=document.getElementById('delta');
    d.className='delta '+(up?'d-up':'d-down');
    d.textContent=(up?'+':'')+fmt(pct)+'% · '+(up?'▲':'▼')+' $'+fmt(eq-s.start_balance)+' all-time';
    document.getElementById('herosub').innerHTML=
      'Balance $'+fmt(s.balance)+' &nbsp;·&nbsp; Open P&amp;L <span class="'+sgn(s.stats.unrealized)+'">'+(s.stats.unrealized>=0?'+':'')+fmt(s.stats.unrealized)+'</span>'+
      '<br>'+s.stats.closed_trades+' closed trades &nbsp;·&nbsp; win rate '+(s.stats.win_rate===null?'—':s.stats.win_rate+'%')+
      ' &nbsp;·&nbsp; profit factor '+(s.stats.profit_factor??'—');

    // tiles
    const dayPnl=s.day?s.day.realized:0;
    document.getElementById('tiles').innerHTML=`
      <div class="tile"><div class="k">Balance</div><div class="v mono">$${fmt(s.balance)}</div><div class="s">settled USDT</div></div>
      <div class="tile"><div class="k">Day P&amp;L</div><div class="v mono ${sgn(dayPnl)}">${dayPnl>=0?'+':''}${fmt(dayPnl)}</div><div class="s">since 00:00 UTC</div></div>
      <div class="tile"><div class="k">Open P&amp;L</div><div class="v mono ${sgn(s.stats.unrealized)}">${s.stats.unrealized>=0?'+':''}${fmt(s.stats.unrealized)}</div><div class="s">${s.position?'position live':'flat'}</div></div>
      <div class="tile"><div class="k">Win rate</div><div class="v mono">${s.stats.win_rate===null?'—':s.stats.win_rate+'%'}</div><div class="s">${s.stats.wins}W / ${s.stats.closed_trades-s.stats.wins}L</div></div>
      <div class="tile"><div class="k">Total P&amp;L</div><div class="v mono ${sgn(s.stats.total_pnl)}">${s.stats.total_pnl>=0?'+':''}${fmt(s.stats.total_pnl)}</div><div class="s">net of fees</div></div>
      <div class="tile"><div class="k">Engine</div><div class="v" style="color:${s.stats.loop_alive?'var(--green)':'var(--red)'}">${s.stats.loop_alive?'LIVE':'DEAD'}</div><div class="s">${s.cycle_count} cycles</div></div>`;

    // equity chart
    const ec=s.equity_curve||[];
    charts.eq.data.labels=ec.map(p=>hhmm(p.t));
    charts.eq.data.datasets[0].data=ec.map(p=>p.eq);
    charts.eq.update('none');

    // price chart
    const ph=s.price_history||[];
    const px=charts.px;
    px.data.labels=ph.map(p=>hhmm(p.t));
    px.data.datasets=[{label:'Price',data:ph.map(p=>p.c),borderColor:'#6ea8fe',borderWidth:2,pointRadius:0,tension:.25,fill:false}];
    const p=s.position;
    if(p){
      addHLine(px,'Entry',p.entry_price,'#6ea8fe',[4,4]);
      addHLine(px,'SL',p.sl,'#f43f5e',[4,4]);
      p.tps.forEach(t=>addHLine(px,'TP',t,'#22c55e',[4,4]));
    }
    px.update('none');
    document.getElementById('pricetrend').innerHTML=s.chart?
      '<span class="chip '+(s.chart.trend==='bullish'?'c-long':s.chart.trend==='bearish'?'c-short':'c-block')+'">'+s.chart.trend.toUpperCase()+' STACK</span>':'';

    // position card
    if(p){
      const price=s.last_price||p.entry_price;
      document.getElementById('posbadge').innerHTML='<span class="chip '+(p.side===1?'c-long':'c-short')+'">'+(p.side===1?'LONG':'SHORT')+'</span>';
      let html='<table><tr><th>Entry</th><th>Units</th><th>Remaining</th><th>Realized</th></tr>'+
        '<tr><td class="mono">$'+fmt(p.entry_price)+'</td><td class="mono">'+fmt(p.units,5)+'</td><td class="mono">'+fmt(p.remaining,5)+
        '</td><td class="mono '+sgn(p.realized)+'">'+(p.realized>=0?'+':'')+fmt(p.realized)+'</td></tr></table><div class="ladder" style="margin-top:16px">';
      const prog=(target)=>{
        if(p.side===1) return Math.max(0,Math.min(100,(price-p.entry_price)/(target-p.entry_price)*100));
        return Math.max(0,Math.min(100,(p.entry_price-price)/(p.entry_price-target)*100));
      };
      p.tps.forEach((t,i)=>{
        const done=p.tps_hit[i];
        const pct=done?100:prog(t);
        html+='<div class="lrow"><span class="lbl">TP'+(i+1)+'</span><span class="mono">$'+fmt(t)+'</span>'+
          '<span class="bar i-tp"><i style="width:'+pct+'%"></i></span><span class="tick" style="color:var(--green)">'+(done?'✓':'')+'</span></div>';
      });
      const slProg=Math.max(0,Math.min(100,(p.side===1?(p.entry_price-price):(price-p.entry_price))/Math.abs(p.entry_price-p.sl)*100));
      html+='<div class="lrow"><span class="lbl">SL</span><span class="mono">$'+fmt(p.sl)+'</span>'+
        '<span class="bar i-sl"><i style="width:'+slProg+'%"></i></span><span></span></div>';
      html+='</div><div style="font-size:11px;color:var(--dim);margin-top:12px">bars show live progress of the last price ($'+fmt(price)+') toward each level</div>';
      document.getElementById('position').innerHTML=html;
    } else {
      document.getElementById('posbadge').innerHTML='<span class="chip c-block">FLAT</span>';
      document.getElementById('position').innerHTML='<div class="npos"><b>No open position</b>waiting for the next EMA-stack signal…</div>';
    }

    // mtf
    const labels={'15m':'15M','30m':'30M','1h':'1H','4h':'4H','1d':'1D'};
    document.getElementById('mtf').innerHTML=Object.keys(labels).map(tf=>{
      const v=s.mtf[tf];
      const cls=v===undefined||v===null?'na':(v?'bull':'bear');
      const txt=v===undefined||v===null?'—':(v?'<span class="arrow">▲</span>Bull':'<span class="arrow">▼</span>Bear');
      return '<div class="mtfcell"><div class="tf">'+labels[tf]+'</div><div class="trend '+cls+'">'+txt+'</div></div>';
    }).join('');

    // engine readout
    const c=s.chart, cfg=s.stats.config;
    document.getElementById('engine').innerHTML=
      (c?'<div class="readout">'+
        '<span class="kv">EMA stack <b>'+cfg.ema_lens.join('/')+'</b></span>'+
        '<span class="kv">ATR(14) <b>'+fmt(c.atr,2)+'</b> '+(c.atr_ok?'<span class="pos">filter pass</span>':'<span class="neg">filter block</span>')+'</span>'+
        '<span class="kv">SL <b>'+cfg.sl_pct+'%</b></span>'+
        '<span class="kv">TP ladder <b>×'+cfg.tp_mults.join(' ×')+'</b></span>'+
        '<span class="kv">Risk <b>'+cfg.risk_pct+'%</b> / trade</span>'+
        '<span class="kv">Max pos <b>$'+fmt(cfg.max_position_usdt,0)+'</b></span>'+
        (s.kill_reason?'<span class="kv" style="border-color:rgba(244,63,94,.5)">'+s.kill_reason+'</span>':'')+
        '</div>':'<div class="npos">warming up…</div>');

    // signals
    document.getElementById('signals').innerHTML='<tr><th>Time</th><th>Signal</th><th>Price</th><th>Status</th></tr>'+
      (s.signals||[]).slice(-10).reverse().map(x=>
        '<tr><td style="color:var(--mut)">'+x.time+'</td>'+
        '<td><span class="chip '+(x.side==='LONG'?'c-long':'c-short')+'">'+x.side+'</span></td>'+
        '<td class="mono">$'+fmt(x.candle_close)+'</td>'+
        '<td>'+(x.blocked?'<span class="chip c-block">blocked · '+(x.reason||'')+'</span>':'<span class="chip c-long">traded</span>')+'</td></tr>').join('');

    document.getElementById('trades').innerHTML='<tr><th>Closed</th><th>Side</th><th>Entry</th><th>Exit</th><th>TPs</th><th>P&amp;L</th><th>Reason</th></tr>'+
      (s.recent_trades||[]).map(t=>
        '<tr><td style="color:var(--mut)">'+t.exit_time+'</td>'+
        '<td><span class="chip '+(t.side==='LONG'?'c-long':'c-short')+'">'+t.side+'</span></td>'+
        '<td class="mono">$'+fmt(t.entry_price)+'</td><td class="mono">$'+fmt(t.avg_exit)+'</td>'+
        '<td>'+t.tps_hit+'/4</td><td class="mono '+sgn(t.pnl)+'">'+(t.pnl>=0?'+':'')+fmt(t.pnl)+'</td>'+
        '<td style="color:var(--mut)">'+t.reason+'</td></tr>').join('');

    document.getElementById('footer').innerHTML=
      'DTC Bot — converted from the DTC v1.36 indicator · signals on closed candles only · '+
      'kill-switch '+(cfg.kill_switch?'armed':'off')+' · daily brake '+cfg.daily_brake_pct+'%'+
      ' &nbsp;|&nbsp; last cycle '+fmt((Date.now()/1000-s.last_cycle_ts),0)+'s ago'+
      ' &nbsp;|&nbsp; <a href="/api/status" target="_blank">raw JSON</a>';
  }catch(e){console.error(e);}
}
makeEquityChart(); makePriceChart(); tick(); setInterval(tick,10000);
</script>
</body>
</html>
"""


@app.route("/")
def dashboard():
    return DASHBOARD_HTML


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
