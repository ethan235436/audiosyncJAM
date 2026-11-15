import os
import json
from aiohttp import web, WSMsgType

# Active rooms: { "ROOM_CODE": {"dj": WebSocketResponse, "listeners": set()} }
rooms = {}


async def health_check(request):
    return web.Response(text="Party Audio Relay is ONLINE!", status=200)


async def websocket_handler(request):
    ws = web.WebSocketResponse(max_msg_size=1024 * 1024)
    await ws.prepare(request)

    current_room = None
    role = None

    try:
        async for msg in ws:
            # 1. Binary Audio Frames -> Bounce directly from DJ to all Listeners
            if msg.type == WSMsgType.BINARY:
                if current_room and role == "dj" and current_room in rooms:
                    for listener in list(rooms[current_room]["listeners"]):
                        if not listener.closed:
                            await listener.send_bytes(msg.data)

            # 2. JSON Control Messages -> Host, Join, or Track Metadata
            elif msg.type == WSMsgType.TEXT:
                data = json.loads(msg.data)
                action = data.get("action")

                if action == "host":
                    code = data.get("room", "").upper().strip()
                    if not code:
                        continue
                    current_room = code
                    role = "dj"
                    rooms[code] = {"dj": ws, "listeners": set()}
                    await ws.send_json({"status": "hosted", "room": code})

                elif action == "join":
                    code = data.get("room", "").upper().strip()
                    if code not in rooms:
                        await ws.send_json({"status": "error", "msg": "Room not found."})
                        continue
                    current_room = code
                    role = "listener"
                    rooms[code]["listeners"].add(ws)
                    count = len(rooms[code]["listeners"])
                    await ws.send_json({"status": "joined", "room": code, "listeners": count})

                    dj_ws = rooms[code].get("dj")
                    if dj_ws and not dj_ws.closed:
                        await dj_ws.send_json({"status": "update", "listeners": count})

                elif action == "meta":
                    if current_room and role == "dj" and current_room in rooms:
                        for listener in list(rooms[current_room]["listeners"]):
                            if not listener.closed:
                                await listener.send_str(msg.data)

            elif msg.type == WSMsgType.ERROR:
                break

    finally:
        # Clean up when DJ or Listener closes the app
        if current_room and current_room in rooms:
            room = rooms[current_room]
            if role == "dj":
                for listener in list(room["listeners"]):
                    if not listener.closed:
                        await listener.send_json({"status": "closed", "msg": "Host ended the party."})
                del rooms[current_room]
            elif role == "listener":
                room["listeners"].discard(ws)
                dj_ws = room.get("dj")
                if dj_ws and not dj_ws.closed:
                    await dj_ws.send_json({"status": "update", "listeners": len(room["listeners"])})

    return ws


app = web.Application()
app.router.add_get("/", health_check)
app.router.add_get("/ws", websocket_handler)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    web.run_app(app, host="0.0.0.0", port=port)
