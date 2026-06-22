import os
import json
import websocket
import threading
import time
from datetime import datetime

print("BOT STARTING...")

# =========================
# CONFIG
# =========================

TOKEN = os.getenv("DERIV_TOKEN")
APP_ID = "1089"
SYMBOL = "frxXAUUSD"

MAX_TRADES_PER_DAY = 3
RISK_PER_TRADE = 0.01

print("TOKEN FOUND:", TOKEN is not None)

# =========================
# STATE
# =========================

prices = []
active_trade = None
trade_count = 0
current_day = datetime.now().date()

ws_global = None

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
# DAILY RESET
# =========================

def reset_daily():
    global trade_count, current_day

    if datetime.now().date() != current_day:
        trade_count = 0
        current_day = datetime.now().date()


# =========================
# DERIV TRADE FLOW (FIXED)
# =========================

def send_proposal(ws, direction):
    """Step 1: request contract proposal"""
    contract_type = "CALL" if direction == "CALL" else "PUT"

    request = {
        "proposal": 1,
        "amount": 1,
        "basis": "stake",
        "contract_type": contract_type,
        "currency": "USD",
        "duration": 1,
        "duration_unit": "m",
        "symbol": SYMBOL
    }

    ws.send(json.dumps(request))


def buy_contract(ws, proposal_id):
    """Step 2: buy contract using proposal"""
    ws.send(json.dumps({
        "buy": proposal_id,
        "price": 1
    }))


# =========================
# TRADING LOGIC
# =========================

def open_trade(ws, direction):
    global trade_count, active_trade

    if trade_count >= MAX_TRADES_PER_DAY:
        return

    if active_trade is not None:
        return

    print("\nTRADE SIGNAL:", direction)

    active_trade = direction
    trade_count += 1

    send_proposal(ws, direction)


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
            open_trade(ws, "CALL")

    if trend == "DOWN" and resistance:
        if rejection_sell(price, resistance):
            print("SELL SIGNAL")
            open_trade(ws, "PUT")


# =========================
# WEBSOCKET EVENTS
# =========================

def on_open(ws):
    global ws_global
    ws_global = ws

    print("Connected to Deriv")

    if not TOKEN:
        print("ERROR: Missing DERIV_TOKEN")
        return

    ws.send(json.dumps({
        "authorize": TOKEN
    }))


def on_message(ws, message):
    global prices

    data = json.loads(message)

    # AUTH
    if "authorize" in data:
        print("Authenticated")

        ws.send(json.dumps({
            "ticks": SYMBOL,
            "subscribe": 1
        }))

    # PROPOSAL RESPONSE
    if "proposal" in data:
        proposal_id = data["proposal"]["id"]
        print("Proposal received:", proposal_id)

        buy_contract(ws, proposal_id)

    # PRICE DATA
    if "tick" in data:
        price = data["tick"]["quote"]

        prices.append(price)

        if len(prices) > 500:
            prices.pop(0)

        print("Price:", price)

        reset_daily()
        strategy(ws, price)


def on_error(ws, error):
    print("WS ERROR:", error)


def on_close(ws, *args):
    print("CONNECTION CLOSED:", args)


# =========================
# RUN LOOP
# =========================

while True:
    try:
        ws = websocket.WebSocketApp(
            f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}",
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )

        ws.run_forever()

    except Exception as e:
        print("RECONNECT ERROR:", e)
        time.sleep(5)
