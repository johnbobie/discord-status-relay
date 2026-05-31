import os
import json
import asyncio
import threading
from queue import Queue
import websockets
from flask import Flask, request, jsonify
 
app = Flask(__name__)
 
BOT_TOKEN = os.environ.get("BOT_TOKEN")
API_KEY = os.environ.get("API_KEY")
 
# Thread-safe queue: Flask puts updates here, gateway reads them
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
            print("Connecting to Discord Gateway...")
            async with websockets.connect(uri) as ws:
                hello = json.loads(await ws.recv())
                heartbeat_interval = hello["d"]["heartbeat_interval"] / 1000
                print(f"Connected. Heartbeat interval: {heartbeat_interval}s")
 
                # Identify with initial presence
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
                print("Identified with Discord.")
 
                async def heartbeat():
                    while True:
                        await asyncio.sleep(heartbeat_interval)
                        await ws.send(json.dumps({"op": 1, "d": None}))
 
                async def queue_watcher():
                    while True:
                        # Poll the thread-safe queue every 0.5s
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
                            print(f"Presence updated to: {presence}")
 
                async def reader():
                    # Keep reading to handle pings/reconnect requests
                    async for message in ws:
                        data = json.loads(message)
                        if data.get("op") == 7:  # Reconnect
                            print("Discord requested reconnect.")
                            raise Exception("Reconnect requested")
                        if data.get("op") == 9:  # Invalid session
                            print("Invalid session, reconnecting...")
                            raise Exception("Invalid session")
 
                await asyncio.gather(heartbeat(), queue_watcher(), reader())
 
        except Exception as e:
            print(f"Gateway error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)
 
 
def start_gateway():
    asyncio.run(discord_gateway())
 
 
# --- HTTP Endpoint ---
 
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
 
    # Put update in the queue — gateway will pick it up within 0.5s
    update_queue.put({"status": status, "activities": activities})
 
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
 
    t = threading.Thread(target=start_gateway, daemon=True)
    t.start()
 
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
 
