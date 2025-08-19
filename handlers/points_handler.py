from typing import Dict, Any
from utils import send_message

async def handle_points(callback_query: Dict[str, Any], registered: Dict[str, Any]):
    points = registered.get("points", 0)
    await send_message(callback_query["from"]["id"], f"Your balance: *{points} points*")
    return {"ok": True}
