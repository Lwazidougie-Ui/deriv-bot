import os
import json
import websocket
import threading
import time
from datetime import datetime

# =========================
# CONFIG
# =========================

TOKEN = os.getenv("DERIV_TOKEN")
APP_ID = "1089"
SYMBOL = "frxXAUUSD"

# =========================
# STATE
# =========================

prices = []
active_trade = None
ws = None
heartbeat_thread = None
stop_heartbeat = False

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
# TRADING LOGIC
# =========================

def open_trade(ws, direction, price):
    global active_trade

    if active_trade is not None:
        return

    active_trade = {
        "direction": direction,
        "entry": price
    }

    print("\nTRADE SIGNAL")
    print("Direction:", direction)
    print("Entry:", price)

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
# HEARTBEAT (KEEP CONNECTION ALIVE)
# =========================

def heartbeat_loop(ws):
    global stop_heartbeat
    while not stop_heartbeat:
        try:
            if ws and ws.sock and ws.sock.connected:
                ws.send(json.dumps({"ping": 1}))
                print("[HEARTBEAT] Ping sent")
        except Exception as e:
            print(f"[HEARTBEAT] Error: {e}")
        
        time.sleep(30)

# =========================
# WEBSOCKET EVENTS
# =========================

def on_open(ws):
    global heartbeat_thread, stop_heartbeat
    print("Connected to Deriv")

    if not TOKEN:
        print("ERROR: Missing DERIV_TOKEN")
        return

    # Start heartbeat thread
    stop_heartbeat = False
    heartbeat_thread = threading.Thread(target=heartbeat_loop, args=(ws,), daemon=True)
    heartbeat_thread.start()

    ws.send(json.dumps({
        "authorize": TOKEN
    }))


def on_message(ws, message):
    global prices

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

        if len(prices) > 500:
            prices.pop(0)

        print("Price:", price)

        strategy(ws, price)


def on_error(ws, error):
    print("WS ERROR:", error)


def on_close(ws, *args):
    global stop_heartbeat
    print("CONNECTION CLOSED:", args)
    stop_heartbeat = True

# =========================
# RUN BOT WITH RECONNECT
# =========================

retry_count = 0
max_retry_wait = 60

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
        print(f"[RECONNECT] Connection failed: {e}")
        retry_count += 1
        wait_time = min(5 * (2 ** retry_count), max_retry_wait)
        print(f"[RECONNECT] Retrying in {wait_time}s (attempt {retry_count})...")
        time.sleep(wait_time)
