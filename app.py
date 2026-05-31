import os
import json
import asyncio
import threading
import traceback
import websockets
from flask import Flask, request, jsonify

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
API_KEY = os.environ.get("API_KEY")

current_presence = {
    "status": "online",
    "activities": []
}

# Globals set once the gateway is running
gateway_loop = None
gateway_ws = None


# --- Discord Gateway ---

async def send_presence():
    global gateway_ws
    if gateway_ws is None:
        print("send_presence called but ws is None", flush=True)
        return
    try:
        await gateway_ws.send(json.dumps({
            "op": 3,
            "d": {
                "since": None,
                "activities": current_presence["activities"],
                "status": current_presence["status"],
                "afk": False
            }
        }))
        print(f"Presence updated to: {current_presence}", flush=True)
    except Exception as e:
        print(f"Error sending presence: {e}", flush=True)


async def discord_gateway():
    global gateway_loop, gateway_ws
    gateway_loop = asyncio.get_running_loop()
    uri = "wss://gateway.discord.gg/?v=10&encoding=json"

    while True:
        try:
            print("Connecting to Discord Gateway...", flush=True)
            async with websockets.connect(uri) as ws:
                gateway_ws = ws
                hello = json.loads(await ws.recv())
                heartbeat_interval = hello["d"]["heartbeat_interval"] / 1000
                print(f"Connected. Heartbeat every {heartbeat_interval}s", flush=True)

                await ws.send(json.dumps({
                    "op": 2,
                    "d": {
                        "token": BOT_TOKEN,
                        "intents": 512,
                        "properties": {
                            "os": "linux",
                            "browser": "status-relay",
                            "device": "status-relay"
                        },
                        "presence": {
                            "status": current_presence["status"],
                            "activities": current_presence["activities"],
                            "afk": False
                        }
                    }
                }))
                print("Identified with Discord.", flush=True)

                async def heartbeat():
                    while True:
                        await asyncio.sleep(heartbeat_interval)
                        await ws.send(json.dumps({"op": 1, "d": None}))

                async def reader():
                    async for message in ws:
                        data = json.loads(message)
                        if data.get("t") == "READY":
                            print("Bot is READY.", flush=True)
                        if data.get("op") == 7:
                            raise Exception("Discord requested reconnect")
                        if data.get("op") == 9:
                            raise Exception("Invalid session")

                await asyncio.gather(heartbeat(), reader())

        except Exception as e:
            print(f"Gateway error: {e}", flush=True)
            traceback.print_exc()
        finally:
            gateway_ws = None
            print("Reconnecting in 5s...", flush=True)
            await asyncio.sleep(5)


def start_gateway():
    print("Gateway thread started.", flush=True)
    asyncio.run(discord_gateway())


# Start gateway thread when Gunicorn imports this module
if BOT_TOKEN and API_KEY:
    t = threading.Thread(target=start_gateway, daemon=True)
    t.start()
else:
    print("WARNING: BOT_TOKEN or API_KEY not set.", flush=True)


# --- HTTP Endpoints ---

@app.route("/status", methods=["POST"])
def update_status():
    provided_key = request.headers.get("X-API-Key")
    if provided_key != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body provided"}), 400

    status = data.get("status", "online")
    activity_type = data.get("activity_type")
    activity_name = data.get("activity_name")

    valid_statuses = ["online", "idle", "dnd", "invisible"]
    if status not in valid_statuses:
        return jsonify({"error": f"Invalid status. Choose from: {valid_statuses}"}), 400

    type_map = {"playing": 0, "streaming": 1, "listening": 2, "watching": 3, "competing": 5}
    activities = []
    if activity_type and activity_name:
        act_type_int = type_map.get(activity_type.lower(), 0)
        activities.append({"name": activity_name, "type": act_type_int})

    current_presence["status"] = status
    current_presence["activities"] = activities

    if gateway_loop and gateway_ws:
        asyncio.run_coroutine_threadsafe(send_presence(), gateway_loop)
        print(f"Scheduled presence update: {status}", flush=True)
    else:
        print("WARNING: Gateway not ready yet, update dropped.", flush=True)

    return jsonify({"success": True, "status": status, "activities": activities})


@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "ok": True,
        "current_status": current_presence["status"],
        "gateway_connected": gateway_ws is not None
    })
