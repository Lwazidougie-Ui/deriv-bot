import os
import json
import websocket
from datetime import datetime

# =========================
# CONFIG
# =========================

TOKEN = os.getenv("DERIV_TOKEN")
APP_ID = "1089"
SYMBOL = "frxXAUUSD"

MAX_TRADES_PER_DAY = 3
RISK_PER_TRADE = 0.01
TP_PCT = 0.01
SL_PCT = 0.01

# =========================
# STATE
# =========================

prices = []
highs = []
lows = []

trade_count = 0
current_day = datetime.now().date()
active_trade = None

# =========================
# INDICATORS
# =========================

def ema(values, period=200):
    if len(values) < period:
        return None

    k = 2 / (period + 1)
    ema_val = values[0]

    for v in values[1:]:
        ema_val = v * k + ema_val * (1 - k)

    return ema_val


def detect_trend():
    if len(prices) < 200:
        return None

    ema200 = ema(prices, 200)

    return "UP" if prices[-1] > ema200 else "DOWN"


def support_resistance():
    if len(prices) < 20:
        return None, None

    return min(prices[-20:]), max(prices[-20:])


def rejection_buy(price, support):
    return price <= support * 1.001


def rejection_sell(price, resistance):
    return price >= resistance * 0.999

# =========================
# TRADE CONTROL
# =========================

def open_trade(ws, direction, price):
    global trade_count, active_trade

    if trade_count >= MAX_TRADES_PER_DAY:
        print("Max trades reached today")
        return

    if active_trade is not None:
        return

    sl = price * (1 - SL_PCT) if direction == "CALL" else price * (1 + SL_PCT)
    tp = price * (1 + TP_PCT) if direction == "CALL" else price * (1 - TP_PCT)

    active_trade = {
        "direction": direction,
        "entry": price,
        "sl": sl,
        "tp": tp
    }

    trade_count += 1

    print("\nTRADE OPENED")
    print("Direction:", direction)
    print("Entry:", price)
    print("SL:", sl)
    print("TP:", tp)

    order = {
        "buy": 1,
        "price": 1,
        "parameters": {
            "amount": 1,
            "basis": "stake",
            "contract_type": direction,
            "currency": "USD",
            "duration": 1,
            "duration_unit": "m",
            "symbol": SYMBOL
        }
    }

    ws.send(json.dumps(order))


def check_exit(price):
    global active_trade

    if not active_trade:
        return

    d = active_trade["direction"]

    if d == "CALL":
        if price >= active_trade["tp"]:
            print("TAKE PROFIT HIT")
            active_trade = None
        elif price <= active_trade["sl"]:
            print("STOP LOSS HIT")
            active_trade = None

    if d == "PUT":
        if price <= active_trade["tp"]:
            print("TAKE PROFIT HIT")
            active_trade = None
        elif price >= active_trade["sl"]:
            print("STOP LOSS HIT")
            active_trade = None

# =========================
# DAILY RESET
# =========================

def reset_daily():
    global trade_count, current_day

    if datetime.now().date() != current_day:
        trade_count = 0
        current_day = datetime.now().date()

# =========================
# STRATEGY
# =========================

def strategy(ws, price):
    trend = detect_trend()
    support, resistance = support_resistance()

    if not trend:
        return

    if trend == "UP" and support:
        if rejection_buy(price, support):
            print("BUY SIGNAL")
            open_trade(ws, "CALL", price)

    if trend == "DOWN" and resistance:
        if rejection_sell(price, resistance):
            print("SELL SIGNAL")
            open_trade(ws, "PUT", price)

# =========================
# WEBSOCKET EVENTS (FIXED)
# =========================

def on_open(ws):
    print("Connected to Deriv")

    ws.send(json.dumps({
        "authorize": TOKEN
    }))


def on_message(ws, message):
    global prices, highs, lows

    data = json.loads(message)

    if "authorize" in data:
        print("Authenticated")

        ws.send(json.dumps({
            "ticks": SYMBOL,
            "subscribe": 1
        }))

    if "tick" in data:
        price = data["tick"]["quote"]

        prices.append(price)
        highs.append(price)
        lows.append(price)

        if len(prices) > 500:
            prices.pop(0)

        reset_daily()
        check_exit(price)
        strategy(ws, price)

# 🔥 FIXED: THIS WAS YOUR ERROR
def on_error(ws, error):
    print("ERROR:", error)


# 🔥 FIXED: COMPATIBLE WITH ALL VERSIONS
def on_close(ws, *args):
    print("CONNECTION CLOSED:", args)

# =========================
# RUN BOT
# =========================

ws = websocket.WebSocketApp(
    f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}",
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

ws.run_forever()
