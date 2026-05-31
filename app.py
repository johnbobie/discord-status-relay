import os
import json
import asyncio
import threading
import traceback
from queue import Queue
import websockets
from flask import Flask, request, jsonify

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
API_KEY = os.environ.get("API_KEY")

update_queue = Queue()

current_presence = {
    "status": "online",
    "activities": []
}


# --- Discord Gateway ---

async def discord_gateway():
    uri = "wss://gateway.discord.gg/?v=10&encoding=json"

    while True:
        try:
            print("Attempting websocket connection...", flush=True)
            async with websockets.connect(uri) as ws:
                print("Websocket connected, waiting for HELLO...", flush=True)
                hello = json.loads(await ws.recv())
                print(f"HELLO received: {hello}", flush=True)
                heartbeat_interval = hello["d"]["heartbeat_interval"] / 1000

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
                print("Identify sent. Waiting for READY...", flush=True)

                async def heartbeat():
                    while True:
                        await asyncio.sleep(heartbeat_interval)
                        await ws.send(json.dumps({"op": 1, "d": None}))
                        print("Heartbeat sent.", flush=True)

                async def queue_watcher():
                    while True:
                        await asyncio.sleep(0.5)
                        while not update_queue.empty():
                            presence = update_queue.get_nowait()
                            await ws.send(json.dumps({
                                "op": 3,
                                "d": {
                                    "since": None,
                                    "activities": presence["activities"],
                                    "status": presence["status"],
                                    "afk": False
                                }
                            }))
                            print(f"Presence updated to: {presence}", flush=True)

                async def reader():
                    async for message in ws:
                        data = json.loads(message)
                        print(f"Gateway message op={data.get('op')} t={data.get('t')}", flush=True)
                        if data.get("op") == 7:
                            raise Exception("Discord requested reconnect")
                        if data.get("op") == 9:
                            raise Exception("Invalid session")

                await asyncio.gather(heartbeat(), queue_watcher(), reader())

        except Exception as e:
            print(f"Gateway error: {e}", flush=True)
            traceback.print_exc()
            print("Reconnecting in 5s...", flush=True)
            await asyncio.sleep(5)


def start_gateway():
    print("Gateway thread started.", flush=True)
    try:
        asyncio.run(discord_gateway())
    except Exception as e:
        print(f"Fatal gateway thread error: {e}", flush=True)
        traceback.print_exc()


# --- Start gateway when Gunicorn imports this module ---
if BOT_TOKEN and API_KEY:
    print("Starting gateway thread...", flush=True)
    t = threading.Thread(target=start_gateway, daemon=True)
    t.start()
else:
    print("WARNING: BOT_TOKEN or API_KEY not set. Gateway not started.", flush=True)


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
    update_queue.put({"status": status, "activities": activities})

    return jsonify({"success": True, "status": status, "activities": activities})


@app.route("/", methods=["GET"])
def health():
    return jsonify({"ok": True, "current_status": current_presence["status"]})
