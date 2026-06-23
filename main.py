import sys
print("BOT STARTING", flush=True)

import os
import json
import websocket
import threading
import time

print("IMPORTS OK", flush=True)

TOKEN = os.getenv("DERIV_TOKEN")
APP_ID = "1089"
SYMBOL = "frxXAUUSD"

print(f"TOKEN SET: {bool(TOKEN)}", flush=True)

if not TOKEN:
    print("ERROR: DERIV_TOKEN is not set. Exiting.", flush=True)
    sys.exit(1)

prices = []
active_trade = None

def on_open(ws):
    print("CONNECTED", flush=True)
    ws.send(json.dumps({"authorize": TOKEN}))

def on_message(ws, message):
    data = json.loads(message)
    if "authorize" in data:
        print("AUTHENTICATED", flush=True)
        ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))
    if "tick" in data:
        price = data["tick"]["quote"]
        prices.append(price)
        if len(prices) > 500:
            prices.pop(0)
        print(f"Price: {price}", flush=True)

def on_error(ws, error):
    print(f"ERROR: {error}", flush=True)

def on_close(ws, *args):
    print(f"CLOSED: {args}", flush=True)

print("CONNECTING TO DERIV...", flush=True)

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
        print(f"CRASH: {e}", flush=True)
        time.sleep(10)
