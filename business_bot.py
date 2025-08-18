import os
import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
import logging
import httpx
from dotenv import load_dotenv
from supabase import create_client, Client
from fastapi import Request, FastAPI
from starlette.responses import Response, PlainTextResponse

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger("httpx").setLevel(logging.DEBUG)
logging.getLogger("httpcore").setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI()

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("BUSINESS_BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
CENTRAL_BOT_TOKEN = os.getenv("CENTRAL_BOT_TOKEN")

if not all([BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY, ADMIN_CHAT_ID, CENTRAL_BOT_TOKEN]):
    raise RuntimeError("BUSINESS_BOT_TOKEN / SUPABASE_URL / SUPABASE_KEY / ADMIN_CHAT_ID / CENTRAL_BOT_TOKEN must be set in .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# In-memory state
USER_STATES: Dict[int, Dict[str, Any]] = {}
STATE_TTL_SECONDS = 30 * 60  # 30 minutes
CATEGORIES = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining"]
WEEK_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def set_state(chat_id: int, state: Dict[str, Any]):
    state["updated_at"] = now_iso()
    USER_STATES[chat_id] = state

def get_state(chat_id: int) -> Optional[Dict[str, Any]]:
    st = USER_STATES.get(chat_id)
    if not st:
        return None
    try:
        updated = datetime.fromisoformat(st.get("updated_at"))
        if (datetime.now(timezone.utc) - updated).total_seconds() > STATE_TTL_SECONDS:
            USER_STATES.pop(chat_id, None)
            return None
    except Exception:
        USER_STATES.pop(chat_id, None)
        return None
    return st

async def send_message(chat_id: int, text: str, reply_markup: Optional[dict] = None, retries: int = 3):
    if not chat_id:
        logger.error("Invalid chat_id for send_message")
        return {"ok": False, "error": "Invalid chat_id"}
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        for attempt in range(retries):
            try:
                logger.debug(f"Sending message to chat_id {chat_id} (attempt {attempt + 1}): {text}")
                response = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Sent message to chat_id {chat_id}: {text}")
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to send message: HTTP {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 400 and "chat not found" in e.response.text.lower():
                    return {"ok": False, "error": "Chat not found"}
                if e.response.status_code == 429:
                    retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    await asyncio.sleep(retry_after)
                    continue
                return {"ok": False, "error": f"HTTP {e.response.status_code}"}
            except Exception as e:
                logger.error(f"Failed to send message: {str(e)}", exc_info=True)
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        logger.error(f"Failed to send message to chat_id {chat_id} after {retries} attempts")
        return {"ok": False, "error": "Max retries reached"}

async def edit_message(chat_id: int, message_id: int, text: str, reply_markup: Optional[dict] = None, retries: int = 3):
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "Markdown"}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        for attempt in range(retries):
            try:
                logger.debug(f"Editing message {message_id} in chat_id {chat_id} (attempt {attempt + 1}): {text}")
                response = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Edited message {message_id} in chat_id {chat_id}: {text}")
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to edit message: HTTP {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 429:
                    retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    await asyncio.sleep(retry_after)
                    continue
                return {"ok": False, "error": f"HTTP {e.response.status_code}"}
            except Exception as e:
                logger.error(f"Failed to edit message: {str(e)}", exc_info=True)
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        logger.error(f"Failed to edit message {message_id} in chat_id {chat_id} after {retries} attempts")
        return {"ok": False, "error": "Max retries reached"}

async def send_admin_message(text: str, reply_markup: Optional[dict] = None, retries: int = 3):
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        payload = {"chat_id": int(ADMIN_CHAT_ID), "text": text, "parse_mode": "Markdown"}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        for attempt in range(retries):
            try:
                response = await client.post(
                    f"https://api.telegram.org/bot{CENTRAL_BOT_TOKEN}/sendMessage",
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Sent admin message to {ADMIN_CHAT_ID}: {text}")
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to send admin message: HTTP {e.response.status_code}")
                if e.response.status_code == 429:
                    retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    await asyncio.sleep(retry_after)
                    continue
                return {"ok": False}
            except Exception as e:
                logger.error(f"Failed to send admin message: {str(e)}")
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        logger.error(f"Failed to send admin message after {retries} attempts")
        return {"ok": False}

async def supabase_insert_return(table: str, payload: dict) -> Optional[Dict[str, Any]]:
    payload['created_at'] = now_iso()  # Explicitly add created_at to payload
    payload['updated_at'] = now_iso()  # Explicitly add updated_at to payload
    try:
        def _ins():
            return supabase.table(table).insert(payload).execute()
        resp = await asyncio.to_thread(_ins)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.error(f"Failed to insert into {table}: no data returned")
            return None
        logger.info(f"Inserted into {table}: {data[0]}")
        return data[0]
    except Exception as e:
        logger.error(f"supabase_insert_return failed for table {table}: {str(e)}", exc_info=True)
        return None

async def supabase_find_business(chat_id: int) -> Optional[Dict[str, Any]]:
    try:
        def _q():
            return supabase.table("businesses").select("*").eq("telegram_id", chat_id).limit(1).execute()
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.info(f"No business found for chat_id {chat_id}")
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_find_business failed for chat_id {chat_id}: {str(e)}", exc_info=True)
        return None

async def webhook_handler(request: Request):
    try:
        update = await request.json()
        if not update:
            logger.error("Received empty update from Telegram")
            return Response(status_code=200)
        await initialize_bot()
        message = update.get("message")
        if message:
            await handle_message_update(message)
        callback_query = update.get("callback_query")
        if callback_query:
            await handle_callback_query(callback_query)
        return Response(status_code=200)
    except json.JSONDecodeError:
        logger.error("Invalid JSON in webhook", exc_info=True)
        return Response(status_code=200)
    except Exception as e:
        logger.error(f"Error processing webhook update: {str(e)}", exc_info=True)
        return Response(status_code=200)

@app.get("/health")
async def health() -> PlainTextResponse:
    return PlainTextResponse("OK", status_code=200)

async def handle_message_update(message: Dict[str, Any]):
    chat_id = message.get("chat", {}).get("id")
    if not chat_id:
        logger.error("No chat_id in message")
        return {"ok": True}
    text = (message.get("text") or "").strip()
    state = get_state(chat_id) or {}

    # Handle /start
    if text.lower() == "/start":
        business = await supabase_find_business(chat_id)
        if business:
            if business["status"] == "approved":
                await send_message(chat_id, "Your business is approved! You can now add discounts and giveaways.")
            else:
                await send_message(chat_id, "Your business registration is pending approval.")
            return {"ok": True}
        await send_message(chat_id, "Welcome! Register your business with /register.")
        return {"ok": True}

    # Handle /register
    if text.lower() == "/register":
        business = await supabase_find_business(chat_id)
        if business:
            await send_message(chat_id, "You’ve already registered!")
            return {"ok": True}
        state = {"stage": "awaiting_name", "data": {"telegram_id": chat_id, "prices": {}, "work_days": []}, "entry_id": None}
        await send_message(chat_id, "Enter your business name:")
        set_state(chat_id, state)
        return {"ok": True}

    # Handle registration steps (as in your code)
    if state.get("stage") == "awaiting_name":
        state["data"]["name"] = text
        await send_message(chat_id, "Choose your business category:", reply_markup=await create_category_keyboard())
        state["stage"] = "awaiting_category"
        set_state(chat_id, state)
        return {"ok": True}

    # (Other registration steps remain the same)

    return {"ok": True}

# (Rest of the code remains the same as your provided version, with the fixes applied to supabase_insert_return and send_message)

if __name__ == "__main__":
    import uvicorn
    asyncio.run(initialize_bot())
    uvicorn.run(app, host="0.0.0.0", port=8000)











'''import os
import asyncio
import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
import logging
import httpx
from dotenv import load_dotenv
from supabase import create_client, Client
from fastapi import FastAPI, Request
from starlette.responses import PlainTextResponse, Response

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger("httpx").setLevel(logging.DEBUG)
logging.getLogger("httpcore").setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI()

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("BUSINESS_BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
CENTRAL_BOT_TOKEN = os.getenv("CENTRAL_BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # e.g., https://your-domain.com/hook/business_bot

if not all([BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY, ADMIN_CHAT_ID, CENTRAL_BOT_TOKEN, WEBHOOK_URL]):
    raise RuntimeError("BUSINESS_BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY, ADMIN_CHAT_ID, CENTRAL_BOT_TOKEN, and WEBHOOK_URL must be set in .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Constants
USER_STATES: Dict[int, Dict[str, Any]] = {}
STATE_TTL_SECONDS = 30 * 60  # 30 minutes
CATEGORIES = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining"]
WEEK_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
MAX_DISCOUNT_PERCENTAGE = 100
MIN_DISCOUNT_PERCENTAGE = 1
DEFAULT_GIVEAWAY_COST = 200
DEFAULT_EXPIRY_DAYS = 30

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def set_state(chat_id: int, state: Dict[str, Any]):
    state["updated_at"] = now_iso()
    USER_STATES[chat_id] = state

def get_state(chat_id: int) -> Optional[Dict[str, Any]]:
    st = USER_STATES.get(chat_id)
    if not st:
        return None
    try:
        updated = datetime.fromisoformat(st.get("updated_at"))
        if (datetime.now(timezone.utc) - updated).total_seconds() > STATE_TTL_SECONDS:
            USER_STATES.pop(chat_id, None)
            return None
    except Exception:
        USER_STATES.pop(chat_id, None)
        return None
    return st

async def send_message(chat_id: int, text: str, reply_markup: Optional[dict] = None, retries: int = 3):
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        for attempt in range(retries):
            try:
                logger.debug(f"Sending message to chat_id {chat_id} (attempt {attempt + 1}): {text}")
                response = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Sent message to chat_id {chat_id}: {text}")
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to send message: HTTP {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 429:
                    retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    await asyncio.sleep(retry_after)
                    continue
                return {"ok": False, "error": f"HTTP {e.response.status_code}"}
            except Exception as e:
                logger.error(f"Failed to send message: {str(e)}", exc_info=True)
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        logger.error(f"Failed to send message to chat_id {chat_id} after {retries} attempts")
        return {"ok": False, "error": "Max retries reached"}

async def edit_message(chat_id: int, message_id: int, text: str, reply_markup: Optional[dict] = None, retries: int = 3):
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "Markdown"}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        for attempt in range(retries):
            try:
                logger.debug(f"Editing message {message_id} in chat_id {chat_id} (attempt {attempt + 1}): {text}")
                response = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Edited message {message_id} in chat_id {chat_id}: {text}")
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to edit message: HTTP {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 429:
                    retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    await asyncio.sleep(retry_after)
                    continue
                return {"ok": False, "error": f"HTTP {e.response.status_code}"}
            except Exception as e:
                logger.error(f"Failed to edit message: {str(e)}", exc_info=True)
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        logger.error(f"Failed to edit message {message_id} in chat_id {chat_id} after {retries} attempts")
        return {"ok": False, "error": "Max retries reached"}

async def send_admin_message(text: str, reply_markup: Optional[dict] = None, retries: int = 3):
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        payload = {"chat_id": int(ADMIN_CHAT_ID), "text": text, "parse_mode": "Markdown"}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        for attempt in range(retries):
            try:
                response = await client.post(
                    f"https://api.telegram.org/bot{CENTRAL_BOT_TOKEN}/sendMessage",
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Sent admin message to {ADMIN_CHAT_ID}: {text}")
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to send admin message: HTTP {e.response.status_code}")
                if e.response.status_code == 429:
                    retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    await asyncio.sleep(retry_after)
                    continue
                return {"ok": False}
            except Exception as e:
                logger.error(f"Failed to send admin message: {str(e)}")
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        logger.error(f"Failed to send admin message after {retries} attempts")
        return {"ok": False}

async def supabase_insert_return(table: str, payload: dict, retries: int = 3) -> Optional[Dict[str, Any]]:
    for attempt in range(retries):
        try:
            def _ins():
                return supabase.table(table).insert(payload).execute()
            resp = await asyncio.to_thread(_ins)
            data = resp.data if hasattr(resp, "data") else resp.get("data")
            if not data:
                logger.error(f"Failed to insert into {table}: no data returned")
                return None
            logger.info(f"Inserted into {table}: {data[0]}")
            return data[0]
        except Exception as e:
            logger.error(f"supabase_insert_return failed for table {table}: {str(e)}", exc_info=True)
            if str(e).startswith("Could not find the 'created_at' column") and attempt < retries - 1:
                logger.info(f"Retrying insert into {table} due to schema cache issue (attempt {attempt + 1})")
                await asyncio.sleep(1.0 * (2 ** attempt))
                continue
            return None
    logger.error(f"Failed to insert into {table} after {retries} attempts")
    return None

async def supabase_update_return(table: str, entry_id: str, payload: dict) -> Optional[Dict[str, Any]]:
    try:
        def _upd():
            return supabase.table(table).update(payload).eq("id", entry_id).execute()
        resp = await asyncio.to_thread(_upd)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.error(f"Failed to update {table} with id {entry_id}: no data returned")
            return None
        logger.info(f"Updated {table} with id {entry_id}: {data[0]}")
        return data[0]
    except Exception as e:
        logger.error(f"supabase_update_return failed for table {table}, id {entry_id}: {str(e)}", exc_info=True)
        return None

async def supabase_find_business(chat_id: int) -> Optional[Dict[str, Any]]:
    try:
        def _q():
            return supabase.table("businesses").select("*").eq("telegram_id", chat_id).limit(1).execute()
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.info(f"No business found for chat_id {chat_id}")
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_find_business failed for chat_id {chat_id}: {str(e)}", exc_info=True)
        return None

async def supabase_find_offers(business_id: str, offer_type: str) -> List[Dict[str, Any]]:
    try:
        def _q():
            table = "discounts" if offer_type == "discount" else "giveaways"
            return supabase.table(table).select("*").eq("business_id", business_id).execute()
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data", [])
        return data
    except Exception as e:
        logger.error(f"supabase_find_offers failed for business_id {business_id}, type {offer_type}: {str(e)}", exc_info=True)
        return []

async def supabase_insert_feedback(chat_id: int, feedback: str) -> Optional[Dict[str, Any]]:
    payload = {
        "telegram_id": chat_id,
        "feedback": feedback,
        "created_at": now_iso()
    }
    return await supabase_insert_return("feedback", payload)

async def create_main_menu_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "Manage Profile", "callback_data": "menu:profile"}],
            [{"text": "Add Discount/Giveaway", "callback_data": "menu:add_offer"}],
            [{"text": "View Discounts", "callback_data": "menu:view_discounts"}],
            [{"text": "View Giveaways", "callback_data": "menu:view_giveaways"}],
            [{"text": "Submit Feedback", "callback_data": "menu:feedback"}],
            [{"text": "View Analytics", "callback_data": "menu:analytics"}]
        ]
    }

async def create_category_keyboard():
    buttons = [[{"text": cat, "callback_data": f"category:{cat}"}] for cat in CATEGORIES]
    return {"inline_keyboard": buttons}

async def create_workdays_keyboard(selected_days: list):
    buttons = []
    for day in WEEK_DAYS:
        prefix = "✅ " if day in selected_days else ""
        buttons.append([{"text": f"{prefix}{day}", "callback_data": f"workday:{day}"}])
    buttons.append([{"text": "Confirm", "callback_data": "workday:confirm"}])
    return {"inline_keyboard": buttons}

async def create_yes_no_keyboard(prefix: str):
    return {
        "inline_keyboard": [
            [{"text": "Yes", "callback_data": f"{prefix}:yes"}, {"text": "No", "callback_data": f"{prefix}:no"}]
        ]
    }

async def create_discount_type_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "Discount", "callback_data": "discount_type:discount"}, {"text": "Giveaway", "callback_data": "discount_type:giveaway"}]
        ]
    }

async def create_offer_action_keyboard(offer_id: str, offer_type: str):
    return {
        "inline_keyboard": [
            [{"text": "Edit", "callback_data": f"edit_{offer_type}:{offer_id}"},
             {"text": "Delete", "callback_data": f"delete_{offer_type}:{offer_id}"}]
        ]
    }

async def initialize_bot():
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        try:
            response = await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
                json={"url": WEBHOOK_URL, "allowed_updates": ["message", "callback_query"]}
            )
            response.raise_for_status()
            response = await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/setChatMenuButton",
                json={"menu_button": {"type": "commands"}}
            )
            response.raise_for_status()
            response = await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands",
                json={
                    "commands": [
                        {"command": "start", "description": "Start the bot"},
                        {"command": "register", "description": "Register your business"},
                        {"command": "add_discount", "description": "Add a discount or giveaway"},
                        {"command": "menu", "description": "Open main menu"},
                        {"command": "cancel", "description": "Cancel current operation"}
                    ]
                }
            )
            response.raise_for_status()
            logger.info("Webhook, menu button, and commands set successfully")
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to set webhook or commands: HTTP {e.response.status_code} - {e.response.text}")
        except Exception as e:
            logger.error(f"Failed to initialize bot: {str(e)}", exc_info=True)

@app.post("/hook/business_bot")
async def webhook_handler(request: Request) -> Response:
    try:
        update = await request.json()
        if not update:
            logger.error("Received empty update from Telegram")
            return Response(status_code=200)
        await initialize_bot()
        message = update.get("message")
        if message:
            await handle_message_update(message)
        callback_query = update.get("callback_query")
        if callback_query:
            await handle_callback_query(callback_query)
        return Response(status_code=200)
    except json.JSONDecodeError:
        logger.error("Invalid JSON in webhook", exc_info=True)
        return Response(status_code=200)
    except Exception as e:
        logger.error(f"Error processing webhook update: {str(e)}", exc_info=True)
        return Response(status_code=200)

@app.get("/health")
async def health() -> PlainTextResponse:
    return PlainTextResponse("OK", status_code=200)

async def handle_message_update(message: Dict[str, Any]):
    chat_id = message.get("chat", {}).get("id")
    if not chat_id:
        logger.error("No chat_id in message")
        return {"ok": True}
    text = (message.get("text") or "").strip()
    state = get_state(chat_id) or {}

    # Handle /start
    if text.lower() == "/start":
        business = await supabase_find_business(chat_id)
        if business:
            if business["status"] == "approved":
                await send_message(chat_id, "Welcome back! Manage your business:", reply_markup=await create_main_menu_keyboard())
            else:
                await send_message(chat_id, "Your business is awaiting approval. We'll notify you soon!")
        else:
            await send_message(chat_id, "Welcome to the Business Bot! Register your business with /register.")
        return {"ok": True}

    # Handle /menu
    if text.lower() == "/menu":
        business = await supabase_find_business(chat_id)
        if business and business["status"] == "approved":
            await send_message(chat_id, "Manage your business:", reply_markup=await create_main_menu_keyboard())
        else:
            await send_message(chat_id, "Please register or wait for approval to access the menu.")
        return {"ok": True}

    # Handle /cancel
    if text.lower() == "/cancel":
        if state:
            USER_STATES.pop(chat_id, None)
            await send_message(chat_id, "Current operation cancelled.")
        else:
            await send_message(chat_id, "No operation to cancel.")
        return {"ok": True}

    # Handle /register
    if text.lower() == "/register":
        business = await supabase_find_business(chat_id)
        if business:
            await send_message(chat_id, "You’ve already registered! Use /menu to manage your business.")
            return {"ok": True}
        state = {"stage": "awaiting_name", "data": {"telegram_id": chat_id, "prices": {}, "work_days": []}, "entry_id": None}
        await send_message(chat_id, "Enter your business name:")
        set_state(chat_id, state)
        return {"ok": True}

    # Handle /add_discount
    if text.lower() == "/add_discount":
        business = await supabase_find_business(chat_id)
        if not business:
            await send_message(chat_id, "Please register your business first with /register.")
            return {"ok": True}
        if business["status"] != "approved":
            await send_message(chat_id, "Your business is not yet approved.")
            return {"ok": True}
        state = {"stage": "awaiting_discount_name", "data": {"business_id": business["id"]}, "entry_id": None}
        await send_message(chat_id, "Enter the discount/giveaway name (e.g., 'Summer Special 20% Off'):")
        set_state(chat_id, state)
        return {"ok": True}

    # Handle registration steps
    if state.get("stage") == "awaiting_name":
        if len(text) > 100:
            await send_message(chat_id, "Business name too long (max 100 characters). Try again:")
            return {"ok": True}
        state["data"]["name"] = text
        await send_message(chat_id, "Choose your business category:", reply_markup=await create_category_keyboard())
        state["stage"] = "awaiting_category"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_phone":
        if not text.startswith("+") or len(text) < 7:
            await send_message(chat_id, "Please enter a valid phone number starting with + (e.g., +1234567890):")
            return {"ok": True}
        state["data"]["phone_number"] = text
        await send_message(chat_id, "Enter your business location (e.g., 123 Main St, City):")
        state["stage"] = "awaiting_location"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_location":
        if len(text) > 200:
            await send_message(chat_id, "Location too long (max 200 characters). Try again:")
            return {"ok": True}
        state["data"]["location"] = text
        selected = state["data"].get("work_days", [])
        resp = await send_message(chat_id, f"Selected work days: {', '.join(selected) or 'None'}\nSelect work days:", reply_markup=await create_workdays_keyboard(selected))
        if resp.get("ok"):
            state["temp_message_id"] = resp["result"]["message_id"]
        state["stage"] = "awaiting_work_days"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_service_name":
        if text.lower() == "/skip":
            await send_message(chat_id, "No services added. Submitting registration.")
            await submit_business_registration(chat_id, state)
            return {"ok": True}
        if len(text) > 50:
            await send_message(chat_id, "Service name too long (max 50 characters). Try again:")
            return {"ok": True}
        state["temp_service_name"] = text
        await send_message(chat_id, f"Enter price for {text} (number):")
        state["stage"] = "awaiting_service_price"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_service_price":
        try:
            price = int(text)
            if price <= 0:
                await send_message(chat_id, "Price must be positive. Try again:")
                return {"ok": True}
            service = state.get("temp_service_name")
            if service:
                state["data"]["prices"][service] = price
                await send_message(chat_id, f"Added {service}: {price}. Add another service?", reply_markup=await create_yes_no_keyboard("add_service"))
                state["stage"] = "awaiting_add_another"
                del state["temp_service_name"]
                set_state(chat_id, state)
            else:
                await send_message(chat_id, "Error: No service name set. Please start over.")
        except ValueError:
            await send_message(chat_id, "Please enter a valid number for price.")
        return {"ok": True}

    # Handle discount/giveaway submission
    if state.get("stage") == "awaiting_discount_name":
        if len(text) > 100:
            await send_message(chat_id, "Name too long (max 100 characters). Try again:")
            return {"ok": True}
        state["data"]["name"] = text
        await send_message(chat_id, "Choose the category for this discount/giveaway:", reply_markup=await create_category_keyboard())
        state["stage"] = "awaiting_discount_category"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_discount_percentage":
        try:
            percentage = int(text)
            if not MIN_DISCOUNT_PERCENTAGE <= percentage <= MAX_DISCOUNT_PERCENTAGE:
                await send_message(chat_id, f"Percentage must be between {MIN_DISCOUNT_PERCENTAGE} and {MAX_DISCOUNT_PERCENTAGE}. Try again:")
                return {"ok": True}
            state["data"]["discount_percentage"] = percentage
            await send_message(chat_id, f"Enter expiry date (YYYY-MM-DD, or /default for {DEFAULT_EXPIRY_DAYS} days):")
            state["stage"] = "awaiting_discount_expiry"
            set_state(chat_id, state)
        except ValueError:
            await send_message(chat_id, "Please enter a valid number for percentage.")
        return {"ok": True}

    if state.get("stage") == "awaiting_discount_expiry":
        if text.lower() == "/default":
            state["data"]["expiry_date"] = (datetime.now(timezone.utc) + timedelta(days=DEFAULT_EXPIRY_DAYS)).date().isoformat()
        else:
            try:
                expiry = datetime.strptime(text, "%Y-%m-%d").date()
                if expiry < datetime.now(timezone.utc).date():
                    await send_message(chat_id, "Expiry date must be in the future. Try again or use /default:")
                    return {"ok": True}
                state["data"]["expiry_date"] = expiry.isoformat()
            except ValueError:
                await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 2025-12-31) or /default.")
                return {"ok": True}
        await send_message(chat_id, "Is this a discount or giveaway?", reply_markup=await create_discount_type_keyboard())
        state["stage"] = "awaiting_discount_type"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_giveaway_cost":
        try:
            cost = int(text)
            if cost < 0:
                await send_message(chat_id, "Cost must be non-negative. Try again:")
                return {"ok": True}
            state["data"]["cost"] = cost
            await send_message(chat_id, f"Enter expiry date (YYYY-MM-DD, or /default for {DEFAULT_EXPIRY_DAYS} days):")
            state["stage"] = "awaiting_giveaway_expiry"
            set_state(chat_id, state)
        except ValueError:
            await send_message(chat_id, "Please enter a valid number for cost.")
        return {"ok": True}

    if state.get("stage") == "awaiting_giveaway_expiry":
        if text.lower() == "/default":
            state["data"]["expiry_date"] = (datetime.now(timezone.utc) + timedelta(days=DEFAULT_EXPIRY_DAYS)).date().isoformat()
        else:
            try:
                expiry = datetime.strptime(text, "%Y-%m-%d").date()
                if expiry < datetime.now(timezone.utc).date():
                    await send_message(chat_id, "Expiry date must be in the future. Try again or use /default:")
                    return {"ok": True}
                state["data"]["expiry_date"] = expiry.isoformat()
            except ValueError:
                await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 2025-12-31) or /default.")
                return {"ok": True}
        await send_message(chat_id, "Is this a discount or giveaway?", reply_markup=await create_discount_type_keyboard())
        state["stage"] = "awaiting_discount_type"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_feedback":
        if len(text) > 500:
            await send_message(chat_id, "Feedback too long (max 500 characters). Try again:")
            return {"ok": True}
        feedback = await supabase_insert_feedback(chat_id, text)
        if feedback:
            await send_message(chat_id, "Thank you for your feedback!")
            await send_admin_message(f"New feedback from chat_id {chat_id}:\n{text}")
        else:
            await send_message(chat_id, "Failed to submit feedback. Please try again.")
        USER_STATES.pop(chat_id, None)
        return {"ok": True}

    if state.get("stage") == "awaiting_profile_update_field":
        if text.lower() == "/done":
            business = await supabase_find_business(chat_id)
            if business:
                await send_message(chat_id, "Profile update completed.", reply_markup=await create_main_menu_keyboard())
                USER_STATES.pop(chat_id, None)
            return {"ok": True}
        field = state.get("field")
        if field == "name":
            if len(text) > 100:
                await send_message(chat_id, "Business name too long (max 100 characters). Try again or /done:")
                return {"ok": True}
            await supabase_update_return("businesses", state["business_id"], {"name": text, "updated_at": now_iso()})
            await send_message(chat_id, "Name updated. Enter another field (name, phone, location, category) or /done:")
        elif field == "phone":
            if not text.startswith("+") or len(text) < 7:
                await send_message(chat_id, "Please enter a valid phone number starting with + (e.g., +1234567890) or /done:")
                return {"ok": True}
            await supabase_update_return("businesses", state["business_id"], {"phone_number": text, "updated_at": now_iso()})
            await send_message(chat_id, "Phone updated. Enter another field (name, phone, location, category) or /done:")
        elif field == "location":
            if len(text) > 200:
                await send_message(chat_id, "Location too long (max 200 characters). Try again or /done:")
                return {"ok": True}
            await supabase_update_return("businesses", state["business_id"], {"location": text, "updated_at": now_iso()})
            await send_message(chat_id, "Location updated. Enter another field (name, phone, location, category) or /done:")
        elif field == "category":
            if text not in CATEGORIES:
                await send_message(chat_id, f"Invalid category. Choose from {', '.join(CATEGORIES)} or /done:")
                return {"ok": True}
            await supabase_update_return("businesses", state["business_id"], {"category": text, "updated_at": now_iso()})
            await send_message(chat_id, "Category updated. Enter another field (name, phone, location, category) or /done:")
        set_state(chat_id, state)
        return {"ok": True}

    return {"ok": True}

async def submit_business_registration(chat_id: int, state: Dict[str, Any]):
    try:
        state["data"]["status"] = "pending"
        state["data"]["created_at"] = now_iso()
        state["data"]["updated_at"] = now_iso()
        business = await supabase_insert_return("businesses", state["data"])
        if not business:
            await send_message(chat_id, "Failed to register business. Please try again.")
            return
        await send_message(chat_id, "Business registered! Awaiting admin approval.")
        await send_admin_message(
            f"New business registration:\nName: {business['name']}\nCategory: {business['category']}\nPhone: {business['phone_number']}\nLocation: {business['location']}\nWork Days: {', '.join(business['work_days'])}\nPrices: {json.dumps(business['prices'])}",
            reply_markup={"inline_keyboard": [
                [{"text": "Approve", "callback_data": f"approve:{business['id']}"}, {"text": "Reject", "callback_data": f"reject:{business['id']}"}]
            ]}
        )
        USER_STATES.pop(chat_id, None)
    except Exception as e:
        logger.error(f"Failed to register business for chat_id {chat_id}: {str(e)}", exc_info=True)
        await send_message(chat_id, "Failed to register business. Please try again.")

async def submit_discount(chat_id: int, state: Dict[str, Any], discount_type: str):
    try:
        business = await supabase_find_business(chat_id)
        if not business:
            await send_message(chat_id, "Business not found.")
            return
        state["data"]["business_type"] = discount_type
        state["data"]["active"] = False
        state["data"]["salon_name"] = business["name"]
        state["data"]["created_at"] = now_iso()
        state["data"]["updated_at"] = now_iso()
        table = "discounts" if discount_type == "discount" else "giveaways"
        offer = await supabase_insert_return(table, state["data"])
        if not offer:
            await send_message(chat_id, f"Failed to submit {discount_type}. Please try again.")
            return
        await send_message(chat_id, f"{discount_type.capitalize()} submitted! Awaiting admin approval.")
        await send_admin_message(
            f"New {discount_type} submission:\nName: {offer['name']}\nCategory: {offer['category']}\nBusiness: {business['name']}\n{'Percentage: ' + str(offer.get('discount_percentage', 'N/A')) if discount_type == 'discount' else 'Cost: ' + str(offer.get('cost', 'N/A'))}",
            reply_markup={"inline_keyboard": [
                [{"text": "Approve", "callback_data": f"giveaway_approve:{offer['id']}"}, {"text": "Reject", "callback_data": f"giveaway_reject:{offer['id']}"}]
            ]}
        )
        USER_STATES.pop(chat_id, None)
    except Exception as e:
        logger.error(f"Failed to submit {discount_type} for chat_id {chat_id}: {str(e)}", exc_info=True)
        await send_message(chat_id, f"Failed to submit {discount_type}. Please try again.")

async def handle_callback_query(callback_query: Dict[str, Any]):
    chat_id = callback_query.get("from", {}).get("id")
    callback_data = callback_query.get("data")
    message_id = callback_query.get("message", {}).get("message_id")
    if not chat_id or not callback_data or not message_id:
        logger.error(f"Invalid callback query: chat_id={chat_id}, callback_data={callback_data}, message_id={message_id}")
        return {"ok": True}

    state = get_state(chat_id) or {}

    # Handle main menu actions
    if callback_data.startswith("menu:"):
        action = callback_data[len("menu:"):]
        business = await supabase_find_business(chat_id)
        if not business or business["status"] != "approved":
            await send_message(chat_id, "Please register or wait for approval.")
            return {"ok": True}
        if action == "profile":
            state = {"stage": "awaiting_profile_update_field", "business_id": business["id"], "field": None}
            await send_message(chat_id, "Enter field to update (name, phone, location, category) or /done:")
            set_state(chat_id, state)
        elif action == "add_offer":
            state = {"stage": "awaiting_discount_name", "data": {"business_id": business["id"]}, "entry_id": None}
            await send_message(chat_id, "Enter the discount/giveaway name (e.g., 'Summer Special 20% Off'):")
            set_state(chat_id, state)
        elif action in ["view_discounts", "view_giveaways"]:
            offer_type = "discount" if action == "view_discounts" else "giveaway"
            offers = await supabase_find_offers(business["id"], offer_type)
            if not offers:
                await send_message(chat_id, f"No {offer_type}s found.")
                return {"ok": True}
            for offer in offers:
                status = "Active" if offer["active"] else "Pending/Rejected"
                message = f"{offer_type.capitalize()}: {offer['name']}\nCategory: {offer['category']}\nStatus: {status}\n{'Percentage: ' + str(offer.get('discount_percentage', 'N/A')) if offer_type == 'discount' else 'Cost: ' + str(offer.get('cost', 'N/A'))}\nExpiry: {offer.get('expiry_date', 'N/A')}"
                await send_message(chat_id, message, reply_markup=await create_offer_action_keyboard(offer["id"], offer_type))
        elif action == "feedback":
            state = {"stage": "awaiting_feedback"}
            await send_message(chat_id, "Please enter your feedback (max 500 characters):")
            set_state(chat_id, state)
        elif action == "analytics":
            discounts = await supabase_find_offers(business["id"], "discount")
            giveaways = await supabase_find_offers(business["id"], "giveaway")
            def _q():
                return supabase.table("user_discounts").select("id").eq("business_id", business["id"]).execute()
            resp = await asyncio.to_thread(_q)
            discount_claims = len(resp.data if hasattr(resp, "data") else resp.get("data", []))
            def _q2():
                return supabase.table("user_giveaways").select("id").eq("business_id", business["id"]).execute()
            resp = await asyncio.to_thread(_q2)
            giveaway_entries = len(resp.data if hasattr(resp, "data") else resp.get("data", []))
            message = f"Analytics:\nTotal Discounts: {len(discounts)}\nTotal Giveaways: {len(giveaways)}\nDiscount Claims: {discount_claims}\nGiveaway Entries: {giveaway_entries}"
            await send_message(chat_id, message)
        return {"ok": True}

    # Handle category selection
    if state.get("stage") in ["awaiting_category", "awaiting_discount_category"] and callback_data.startswith("category:"):
        category = callback_data[len("category:"):]
        if category not in CATEGORIES:
            await send_message(chat_id, "Invalid category. Please choose again:", reply_markup=await create_category_keyboard())
            return {"ok": True}
        state["data"]["category"] = category
        next_stage = "awaiting_phone" if state.get("stage") == "awaiting_category" else "awaiting_discount_percentage" if state.get("data").get("business_type") == "discount" else "awaiting_giveaway_cost"
        message = {
            "awaiting_phone": "Enter your business phone number (e.g., +1234567890):",
            "awaiting_discount_percentage": "Enter discount percentage (1-100):",
            "awaiting_giveaway_cost": f"Enter giveaway cost in points (e.g., {DEFAULT_GIVEAWAY_COST}):"
        }[next_stage]
        await send_message(chat_id, message)
        state["stage"] = next_stage
        set_state(chat_id, state)
        return {"ok": True}

    # Handle work days selection
    if state.get("stage") == "awaiting_work_days":
        if callback_data.startswith("workday:"):
            day = callback_data[len("workday:"):]
            if day == "confirm":
                if not state["data"].get("work_days"):
                    await edit_message(chat_id, message_id, "Please select at least one work day.", reply_markup=await create_workdays_keyboard(state["data"].get("work_days", [])))
                    return {"ok": True}
                await send_message(chat_id, "Enter the name of your first service (or /skip if no services):")
                state["stage"] = "awaiting_service_name"
                set_state(chat_id, state)
                return {"ok": True}
            if day in WEEK_DAYS:
                selected = state["data"].get("work_days", [])
                if day in selected:
                    selected.remove(day)
                else:
                    selected.append(day)
                state["data"]["work_days"] = selected
                set_state(chat_id, state)
                await edit_message(chat_id, message_id, f"Selected work days: {', '.join(selected) or 'None'}\nSelect work days:", reply_markup=await create_workdays_keyboard(selected))
                return {"ok": True}

    # Handle add another service
    if state.get("stage") == "awaiting_add_another" and callback_data.startswith("add_service:"):
        choice = callback_data[len("add_service:"):]
        if choice == "yes":
            await send_message(chat_id, "Enter the next service name:")
            state["stage"] = "awaiting_service_name"
            set_state(chat_id, state)
        elif choice == "no":
            await submit_business_registration(chat_id, state)
        return {"ok": True}

    # Handle discount type selection
    if state.get("stage") == "awaiting_discount_type" and callback_data.startswith("discount_type:"):
        discount_type = callback_data[len("discount_type:"):]
        state["data"]["business_type"] = discount_type
        next_stage = "awaiting_discount_percentage" if discount_type == "discount" else "awaiting_giveaway_cost"
        message = "Enter discount percentage (1-100):" if discount_type == "discount" else f"Enter giveaway cost in points (e.g., {DEFAULT_GIVEAWAY_COST}):"
        await send_message(chat_id, message)
        state["stage"] = next_stage
        set_state(chat_id, state)
        return {"ok": True}

    # Handle offer actions
    if callback_data.startswith("edit_discount:") or callback_data.startswith("edit_giveaway:"):
        offer_id = callback_data[len("edit_discount:"):] if callback_data.startswith("edit_discount:") else callback_data[len("edit_giveaway:"):]
        offer_type = "discount" if callback_data.startswith("edit_discount:") else "giveaway"
        table = "discounts" if offer_type == "discount" else "giveaways"
        try:
            def _q():
                return supabase.table(table).select("*").eq("id", offer_id).limit(1).execute()
            resp = await asyncio.to_thread(_q)
            offer = resp.data[0] if (resp.data if hasattr(resp, "data") else resp.get("data")) else None
            if not offer:
                await send_message(chat_id, f"{offer_type.capitalize()} not found.")
                return {"ok": True}
            state = {
                "stage": "awaiting_discount_name",
                "data": {"business_id": offer["business_id"], "id": offer_id, "edit_mode": True, "business_type": offer_type},
                "entry_id": offer_id
            }
            await send_message(chat_id, f"Editing {offer_type}. Enter new name (current: {offer['name']}):")
            set_state(chat_id, state)
        except Exception as e:
            logger.error(f"Failed to fetch {offer_type} {offer_id}: {str(e)}")
            await send_message(chat_id, f"Failed to edit {offer_type}. Please try again.")
        return {"ok": True}

    if callback_data.startswith("delete_discount:") or callback_data.startswith("delete_giveaway:"):
        offer_id = callback_data[len("delete_discount:"):] if callback_data.startswith("delete_discount:") else callback_data[len("delete_giveaway:"):]
        offer_type = "discount" if callback_data.startswith("delete_discount:") else "giveaway"
        table = "discounts" if offer_type == "discount" else "giveaways"
        try:
            def _q():
                return supabase.table(table).delete().eq("id", offer_id).execute()
            await asyncio.to_thread(_q)
            await send_message(chat_id, f"{offer_type.capitalize()} deleted successfully.")
        except Exception as e:
            logger.error(f"Failed to delete {offer_type} {offer_id}: {str(e)}")
            await send_message(chat_id, f"Failed to delete {offer_type}. Please try again.")
        return {"ok": True}

    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    asyncio.run(initialize_bot())
    uvicorn.run(app, host="0.0.0.0", port=8000)
'''
