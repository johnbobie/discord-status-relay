import os
import json
import asyncio
import threading
import websockets
from flask import Flask, request, jsonify

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
API_KEY = os.environ.get("API_KEY")  # A secret key so only BotGhost can call your server

# Shared state
current_presence = {
    "status": "online",
    "activities": []
}
presence_update_event = asyncio.Event()
loop = None


# --- Discord Gateway ---

async def discord_gateway():
    global loop
    loop = asyncio.get_event_loop()

    uri = "wss://gateway.discord.gg/?v=10&encoding=json"

    while True:
        try:
            async with websockets.connect(uri) as ws:
                # Receive HELLO
                hello = json.loads(await ws.recv())
                heartbeat_interval = hello["d"]["heartbeat_interval"] / 1000

                # Identify
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

                async def heartbeat():
                    while True:
                        await asyncio.sleep(heartbeat_interval)
                        await ws.send(json.dumps({"op": 1, "d": None}))

                async def presence_watcher():
                    while True:
                        await presence_update_event.wait()
                        presence_update_event.clear()
                        await ws.send(json.dumps({
                            "op": 3,
                            "d": {
                                "since": None,
                                "activities": current_presence["activities"],
                                "status": current_presence["status"],
                                "afk": False
                            }
                        }))
                        print(f"Presence updated: {current_presence}")

                await asyncio.gather(heartbeat(), presence_watcher())

        except Exception as e:
            print(f"Gateway error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)


def start_gateway():
    asyncio.run(discord_gateway())


# --- HTTP Endpoint ---

@app.route("/status", methods=["POST"])
def update_status():
    # Check API key
    provided_key = request.headers.get("X-API-Key")
    if provided_key != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body provided"}), 400

    status = data.get("status", "online")
    activity_type = data.get("activity_type")  # playing, watching, listening, competing
    activity_name = data.get("activity_name")

    # Validate status
    valid_statuses = ["online", "idle", "dnd", "invisible"]
    if status not in valid_statuses:
        return jsonify({"error": f"Invalid status. Choose from: {valid_statuses}"}), 400

    # Build activities list
    type_map = {"playing": 0, "streaming": 1, "listening": 2, "watching": 3, "competing": 5}
    activities = []
    if activity_type and activity_name:
        act_type_int = type_map.get(activity_type.lower(), 0)
        activities.append({"name": activity_name, "type": act_type_int})

    # Update shared state and signal the gateway coroutine
    current_presence["status"] = status
    current_presence["activities"] = activities

    if loop:
        loop.call_soon_threadsafe(presence_update_event.set)

    return jsonify({"success": True, "status": status, "activities": activities})


@app.route("/", methods=["GET"])
def health():
    return jsonify({"ok": True, "current_status": current_presence["status"]})


# --- Start ---

if __name__ == "__main__":
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN environment variable is not set")
    if not API_KEY:
        raise ValueError("API_KEY environment variable is not set")

    # Run Discord gateway in a background thread
    t = threading.Thread(target=start_gateway, daemon=True)
    t.start()

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
