import os
import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import logging
import httpx
from dotenv import load_dotenv
from supabase import create_client, Client
from fastapi import Request

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger("httpx").setLevel(logging.DEBUG)
logging.getLogger("httpcore").setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

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

async def supabase_insert_return(table: str, payload: dict) -> Optional[Dict[str, Any]]:
    def _ins():
        return supabase.table(table).insert(payload).execute()
    try:
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
    def _q():
        return supabase.table("businesses").select("*").eq("telegram_id", chat_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.info(f"No business found for chat_id {chat_id}")
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_find_business failed for chat_id {chat_id}: {str(e)}", exc_info=True)
        return None

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
    return {"inline_keyboard": [
        [{"text": "Yes", "callback_data": f"{prefix}:yes"}, {"text": "No", "callback_data": f"{prefix}:no"}]
    ]}

async def create_discount_type_keyboard():
    return {"inline_keyboard": [
        [{"text": "Discount", "callback_data": "discount_type:discount"}, {"text": "Giveaway", "callback_data": "discount_type:giveaway"}]
    ]}

async def initialize_bot():
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        try:
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
                        {"command": "cancel", "description": "Cancel current operation"}
                    ]
                }
            )
            response.raise_for_status()
            logger.info("Set menu button and commands")
        except Exception as e:
            logger.error(f"Failed to set menu button or commands: {str(e)}", exc_info=True)

async def webhook_handler(request: Request):
    try:
        update = await request.json()
    except Exception as e:
        logger.error(f"Invalid JSON in webhook: {str(e)}", exc_info=True)
        return {"ok": True}
    await initialize_bot()
    message = update.get("message")
    if message:
        return await handle_message_update(message)
    callback_query = update.get("callback_query")
    if callback_query:
        return await handle_callback_query(callback_query)
    return {"ok": True}

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
                await send_message(chat_id, "Your business is approved! Add discounts or giveaways with /add_discount.")
            else:
                await send_message(chat_id, "Your business is awaiting approval. We'll notify you soon!")
        else:
            await send_message(chat_id, "Welcome to the Business Bot! Register your business with /register.")
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
            await send_message(chat_id, "You’ve already registered! Use /add_discount to add offers.")
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
        await send_message(chat_id, "Enter the discount/giveaway name (e.g., '20% Off Nail Art'):")
        set_state(chat_id, state)
        return {"ok": True}

    # Handle registration steps
    if state.get("stage") == "awaiting_name":
        state["data"]["name"] = text
        await send_message(chat_id, "Choose your business category:", reply_markup=await create_category_keyboard())
        state["stage"] = "awaiting_category"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_phone":
        if not text.startswith("+"):
            await send_message(chat_id, "Please enter a valid phone number starting with + (e.g., +1234567890):")
            return {"ok": True}
        state["data"]["phone_number"] = text
        await send_message(chat_id, "Enter your business location (e.g., 123 Main St, City):")
        state["stage"] = "awaiting_location"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_location":
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
        state["temp_service_name"] = text
        await send_message(chat_id, f"Enter price for {text} (number):")
        state["stage"] = "awaiting_service_price"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_service_price":
        try:
            price = int(text)
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
        state["data"]["name"] = text
        await send_message(chat_id, "Choose the category for this discount/giveaway:", reply_markup=await create_category_keyboard())
        state["stage"] = "awaiting_discount_category"
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
        state["data"]["business_type"] = discount_type
        state["data"]["active"] = False  # Pending approval
        state["data"]["created_at"] = now_iso()
        state["data"]["updated_at"] = now_iso()
        giveaway = await supabase_insert_return("giveaways", state["data"])
        if not giveaway:
            await send_message(chat_id, "Failed to submit discount/giveaway. Please try again.")
            return
        await send_message(chat_id, "Discount/giveaway submitted! Awaiting admin approval.")
        await send_admin_message(
            f"New {discount_type} submission:\nName: {giveaway['name']}\nCategory: {giveaway['category']}\nBusiness ID: {giveaway['business_id']}",
            reply_markup={"inline_keyboard": [
                [{"text": "Approve", "callback_data": f"giveaway_approve:{giveaway['id']}"}, {"text": "Reject", "callback_data": f"giveaway_reject:{giveaway['id']}"}]
            ]}
        )
        USER_STATES.pop(chat_id, None)
    except Exception as e:
        logger.error(f"Failed to submit discount for chat_id {chat_id}: {str(e)}", exc_info=True)
        await send_message(chat_id, "Failed to submit discount/giveaway. Please try again.")

async def handle_callback_query(callback_query: Dict[str, Any]):
    chat_id = callback_query.get("from", {}).get("id")
    callback_data = callback_query.get("data")
    message_id = callback_query.get("message", {}).get("message_id")
    if not chat_id or not callback_data or not message_id:
        logger.error(f"Invalid callback query: chat_id={chat_id}, callback_data={callback_data}, message_id={message_id}")
        return {"ok": True}

    state = get_state(chat_id) or {}

    # Handle category selection for registration
    if state.get("stage") == "awaiting_category" and callback_data.startswith("category:"):
        category = callback_data[len("category:"):]
        if category not in CATEGORIES:
            await send_message(chat_id, "Invalid category. Please choose again:", reply_markup=await create_category_keyboard())
            return {"ok": True}
        state["data"]["category"] = category
        await send_message(chat_id, "Enter your business phone number (e.g., +1234567890):")
        state["stage"] = "awaiting_phone"
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

    # Handle category selection for discount/giveaway
    if state.get("stage") == "awaiting_discount_category" and callback_data.startswith("category:"):
        category = callback_data[len("category:"):]
        if category not in CATEGORIES:
            await send_message(chat_id, "Invalid category. Please choose again:", reply_markup=await create_category_keyboard())
            return {"ok": True}
        state["data"]["category"] = category
        await send_message(chat_id, "Is this a discount or giveaway?", reply_markup=await create_discount_type_keyboard())
        state["stage"] = "awaiting_discount_type"
        set_state(chat_id, state)
        return {"ok": True}

    # Handle discount type selection
    if state.get("stage") == "awaiting_discount_type" and callback_data.startswith("discount_type:"):
        discount_type = callback_data[len("discount_type:"):]
        await submit_discount(chat_id, state, discount_type)
        return {"ok": True}

    return {"ok": True}
