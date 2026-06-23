import sys
import os
import json
import websocket
import threading
import time

print("BOT STARTING", flush=True)

TOKEN = os.getenv("DERIV_TOKEN")
APP_ID = "1089"
SYMBOL = "frxXAUUSD"

print(f"TOKEN SET: {bool(TOKEN)}", flush=True)

if not TOKEN:
    print("ERROR: DERIV_TOKEN not set", flush=True)
    sys.exit(1)

prices = []
active_trade = None
stop_heartbeat = False

def heartbeat_loop(ws):
    global stop_heartbeat
    while not stop_heartbeat:
        try:
            ws.send(json.dumps({"ping": 1}))
            print("[HEARTBEAT] Ping sent", flush=True)
        except Exception as e:
            print(f"[HEARTBEAT] Error: {e}", flush=True)
            break
        time.sleep(20)  # every 20s instead of 30s

def on_open(ws):
    global stop_heartbeat
    print("CONNECTED", flush=True)
    stop_heartbeat = False
    t = threading.Thread(target=heartbeat_loop, args=(ws,), daemon=True)
    t.start()
    ws.send(json.dumps({"authorize": TOKEN}))

def on_message(ws, message):
    data = json.loads(message)
    if "error" in data:
        print(f"DERIV ERROR: {data['error']}", flush=True)
    if "authorize" in data:
        print("AUTHENTICATED", flush=True)
        ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))
    if "pong" in data:
        print("[HEARTBEAT] Pong received", flush=True)
    if "tick" in data:
        price = data["tick"]["quote"]
        prices.append(price)
        if len(prices) > 500:
            prices.pop(0)
        print(f"Price: {price}", flush=True)

def on_error(ws, error):
    print(f"WS ERROR: {error}", flush=True)

def on_close(ws, code, reason):
    global stop_heartbeat
    stop_heartbeat = True
    print(f"CLOSED — code: {code}, reason: {reason}", flush=True)

print("STARTING CONNECTION LOOP", flush=True)
retry = 0

while True:
    try:
        print(f"CONNECTING (attempt {retry + 1})...", flush=True)
        ws = websocket.WebSocketApp(
            f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}",
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever(ping_interval=25, ping_timeout=10)
    except Exception as e:
        print(f"CRASH: {e}", flush=True)
    
    retry += 1
    wait = min(5 * retry, 60)
    print(f"RECONNECTING in {wait}s...", flush=True)
    time.sleep(wait)
