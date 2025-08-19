import os
import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List
import random
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
BOT_TOKEN = os.getenv("CENTRAL_BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # e.g., https://backend-python-6q8a.onrender.com/hook/central_bot

if not all([BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY, ADMIN_CHAT_ID, WEBHOOK_URL]):
    raise RuntimeError("CENTRAL_BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY, ADMIN_CHAT_ID, and WEBHOOK_URL must be set in .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# In-memory state
USER_STATES: Dict[int, Dict[str, Any]] = {}
INTERESTS = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining", "Discounts only", "Giveaways only"]
CATEGORIES = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining"]
STARTER_POINTS = 100
STATE_TTL_SECONDS = 30 * 60  # 30 minutes
EMOJIS = ["1️⃣", "2️⃣", "3️⃣"]

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

async def clear_inline_keyboard(chat_id: int, message_id: int, retries: int = 3):
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        for attempt in range(retries):
            try:
                logger.debug(f"Clearing inline keyboard for chat_id {chat_id}, message_id {message_id}")
                response = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageReplyMarkup",
                    json={"chat_id": chat_id, "message_id": message_id, "reply_markup": {}}
                )
                response.raise_for_status()
                logger.info(f"Cleared keyboard for chat_id {chat_id}, message_id {message_id}")
                return
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to clear keyboard: HTTP {e.response.status_code}")
                if e.response.status_code == 429:
                    retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    await asyncio.sleep(retry_after)
                    continue
                break
            except Exception as e:
                logger.error(f"Failed to clear keyboard: {str(e)}")
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        logger.error(f"Failed to clear keyboard for chat_id {chat_id} after {retries} attempts")

async def safe_clear_markup(chat_id: int, message_id: int):
    try:
        await clear_inline_keyboard(chat_id, message_id)
    except Exception:
        logger.debug(f"Ignored error while clearing keyboard for chat_id {chat_id}")

async def edit_message_keyboard(chat_id: int, message_id: int, reply_markup: dict, retries: int = 3):
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        payload = {"chat_id": chat_id, "message_id": message_id, "reply_markup": reply_markup}
        for attempt in range(retries):
            try:
                logger.debug(f"Editing keyboard for chat_id {chat_id}, message_id {message_id}")
                response = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageReplyMarkup",
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Edited keyboard for chat_id {chat_id}, message_id {message_id}")
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to edit keyboard: HTTP {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 429:
                    retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    await asyncio.sleep(retry_after)
                    continue
                return {"ok": False, "error": f"HTTP {e.response.status_code}"}
            except Exception as e:
                logger.error(f"Failed to edit keyboard: {str(e)}", exc_info=True)
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        logger.error(f"Failed to edit keyboard for chat_id {chat_id} after {retries} attempts")
        return {"ok": False, "error": "Max retries reached"}

async def set_menu_button():
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
                        {"command": "menu", "description": "Open the menu"},
                        {"command": "myid", "description": "Get your Telegram ID"},
                        {"command": "approve", "description": "Approve a business (admin only)"},
                        {"command": "reject", "description": "Reject a business (admin only)"}
                    ]
                }
            )
            response.raise_for_status()
            logger.info("Set menu button and commands")
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to set menu button or commands: HTTP {e.response.status_code} - {e.response.text}")
        except Exception as e:
            logger.error(f"Failed to set menu button or commands: {str(e)}", exc_info=True)

def create_menu_options_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "Main Menu", "callback_data": "menu:main"}],
            [{"text": "Change Language", "callback_data": "menu:language"}]
        ]
    }

def create_language_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "English", "callback_data": "lang:en"}],
            [{"text": "Русский", "callback_data": "lang:ru"}]
        ]
    }

def create_gender_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "Female", "callback_data": "gender:female"},
                {"text": "Male", "callback_data": "gender:male"}
            ]
        ]
    }

def create_interests_keyboard(selected: List[str] = []):
    buttons = []
    for i, interest in enumerate(INTERESTS):
        text = interest
        for idx, sel in enumerate(selected):
            if sel == interest:
                text = f"{EMOJIS[idx]} {interest}"
                break
        buttons.append([{"text": text, "callback_data": f"interest:{interest}"}])
    buttons.append([{"text": "Done", "callback_data": "interests_done"}])
    return {"inline_keyboard": buttons}

def create_main_menu_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "My Points", "callback_data": "menu:points"}],
            [{"text": "Profile", "callback_data": "menu:profile"}],
            [{"text": "Discounts", "callback_data": "menu:discounts"}],
            [{"text": "Giveaways", "callback_data": "menu:giveaways"}]
        ]
    }

def create_categories_keyboard():
    buttons = []
    for cat in CATEGORIES:
        buttons.append([{"text": cat, "callback_data": f"discount_category:{cat}"}])
    return {"inline_keyboard": buttons}

def create_phone_keyboard():
    return {
        "keyboard": [[{"text": "Share phone", "request_contact": True}]],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

async def supabase_find_draft(chat_id: int) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("central_bot_leads").select("*").eq("telegram_id", chat_id).eq("is_draft", True).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.info(f"No draft found for chat_id {chat_id}")
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_find_draft failed for chat_id {chat_id}: {str(e)}", exc_info=True)
        return None

async def supabase_find_registered(chat_id: int) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("central_bot_leads").select("*").eq("telegram_id", chat_id).eq("is_draft", False).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.info(f"No registered user found for chat_id {chat_id}")
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_find_registered failed for chat_id {chat_id}: {str(e)}", exc_info=True)
        return None

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

async def supabase_update_by_id_return(table: str, entry_id: str, payload: dict) -> Optional[Dict[str, Any]]:
    def _upd():
        return supabase.table(table).update(payload).eq("id", entry_id).execute()
    try:
        resp = await asyncio.to_thread(_upd)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.error(f"Failed to update {table} with id {entry_id}: no data returned")
            return None
        logger.info(f"Updated {table} with id {entry_id}: {data[0]}")
        return data[0]
    except Exception as e:
        logger.error(f"supabase_update_by_id_return failed for table {table}, id {entry_id}: {str(e)}", exc_info=True)
        return None

async def supabase_find_business(business_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("businesses").select("*").eq("id", business_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.info(f"No business found for business_id {business_id}")
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_find_business failed for business_id {business_id}: {str(e)}", exc_info=True)
        return None

async def supabase_find_discount(discount_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("discounts").select("*").eq("id", discount_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.info(f"No discount found for discount_id {discount_id}")
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_find_discount failed for discount_id {discount_id}: {str(e)}", exc_info=True)
        return None

async def supabase_find_giveaway(giveaway_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("giveaways").select("*").eq("id", giveaway_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.info(f"No giveaway found for giveaway_id {giveaway_id}")
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_find_giveaway failed for giveaway_id {giveaway_id}: {str(e)}", exc_info=True)
        return None

async def generate_discount_code(chat_id: int, business_id: str, discount_id: str) -> tuple[str, str]:
    if not business_id or not discount_id:
        logger.error(f"Invalid business_id: {business_id} or discount_id: {discount_id} for chat_id {chat_id}")
        raise ValueError("Business ID or discount ID is missing or invalid")
    def _check_existing_code():
        return supabase.table("user_discounts").select("promo_code").eq("promo_code", code).eq("business_id", business_id).execute()
    def _check_claimed():
        return supabase.table("user_discounts").select("id").eq("telegram_id", chat_id).eq("discount_id", discount_id).execute()
    claimed = await asyncio.to_thread(_check_claimed)
    if claimed.data:
        raise ValueError("Already claimed this discount")
    while True:
        code = f"{random.randint(0, 9999):04d}"
        existing = await asyncio.to_thread(_check_existing_code)
        if not existing.data:
            break
    expiry = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
    payload = {
        "telegram_id": chat_id,
        "business_id": business_id,
        "discount_id": discount_id,
        "promo_code": code,
        "promo_expiry": expiry,
        "entry_status": "standard",
        "joined_at": now_iso()
    }
    inserted = await supabase_insert_return("user_discounts", payload)
    if not inserted:
        logger.error(f"Failed to insert discount promo code for chat_id: {chat_id}, discount_id: {discount_id}")
        raise RuntimeError("Failed to save promo code")
    logger.info(f"Generated discount promo code {code} for chat_id {chat_id}, discount_id {discount_id}")
    return code, expiry

async def generate_promo_code(chat_id: int, business_id: str, giveaway_id: str, discount_type: str = "standard") -> tuple[str, str]:
    if not business_id or not giveaway_id:
        logger.error(f"Invalid business_id: {business_id} or giveaway_id: {giveaway_id} for chat_id {chat_id}")
        raise ValueError("Business ID or giveaway ID is missing or invalid")
    def _check_existing_code():
        return supabase.table("user_giveaways").select("promo_code").eq("promo_code", code).eq("business_id", business_id).execute()
    while True:
        code = f"{random.randint(0, 9999):04d}"
        existing = await asyncio.to_thread(_check_existing_code)
        if not existing.data:
            break
    expiry = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
    payload = {
        "telegram_id": chat_id,
        "business_id": business_id,
        "giveaway_id": giveaway_id,
        "promo_code": code,
        "promo_expiry": expiry,
        "entry_status": discount_type
    }
    inserted = await supabase_insert_return("user_giveaways", payload)
    if not inserted:
        logger.error(f"Failed to insert giveaway promo code for chat_id: {chat_id}, giveaway_id: {giveaway_id}")
        raise RuntimeError("Failed to save promo code")
    logger.info(f"Generated giveaway promo code {code} for chat_id {chat_id}, giveaway_id {giveaway_id}")
    return code, expiry

async def has_redeemed_discount(chat_id: int) -> bool:
    def _q():
        current_month = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return supabase.table("user_discounts").select("id").eq("telegram_id", chat_id).eq("entry_status", "standard").gte("joined_at", current_month.isoformat()).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        has_redeemed = bool(data)
        logger.info(f"Checked redeemed discount for chat_id {chat_id}: {has_redeemed}")
        return has_redeemed
    except Exception as e:
        logger.error(f"has_redeemed_discount failed for chat_id {chat_id}: {str(e)}", exc_info=True)
        return False

async def notify_users(giveaway_id: str):
    try:
        giveaway = await supabase_find_giveaway(giveaway_id)
        if not giveaway:
            logger.error(f"Giveaway {giveaway_id} not found for notification")
            return
        users = supabase.table("central_bot_leads").select("telegram_id").eq("is_draft", False).contains("interests", [giveaway["category"]]).execute().data
        for user in users:
            await send_message(
                user["telegram_id"],
                f"New {giveaway['category']} offer: *{giveaway['name']}* at {giveaway['salon_name']}. Check it out:",
                create_main_menu_keyboard()
            )
        logger.info(f"Notified {len(users)} users for giveaway {giveaway_id}")
    except Exception as e:
        logger.error(f"Failed to notify users for giveaway {giveaway_id}: {str(e)}", exc_info=True)

async def initialize_bot():
    await set_menu_button()
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        try:
            response = await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
                json={"url": WEBHOOK_URL, "allowed_updates": ["message", "callback_query"]}
            )
            response.raise_for_status()
            logger.info(f"Webhook set to {WEBHOOK_URL}")
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to set webhook: HTTP {e.response.status_code} - {e.response.text}")
        except Exception as e:
            logger.error(f"Failed to set webhook: {str(e)}", exc_info=True)

@app.post("/hook/central_bot")
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
    contact = message.get("contact")
    state = get_state(chat_id) or {}

    # Handle /myid
    if text.lower() == "/myid":
        await send_message(chat_id, f"Your Telegram ID: {chat_id}")
        return {"ok": True}

    # Handle admin commands for business approval/rejection
    if chat_id == int(ADMIN_CHAT_ID) and text.startswith("/approve_"):
        business_id = text[len("/approve_"):]
        try:
            uuid.UUID(business_id)
            business = await supabase_find_business(business_id)
            if not business:
                await send_message(chat_id, f"Business with ID {business_id} not found.")
                return {"ok": True}
            def _update_business():
                return supabase.table("businesses").update({"status": "approved", "updated_at": now_iso()}).eq("id", business_id).execute()
            await asyncio.to_thread(_update_business)
            await send_message(chat_id, f"Business {business['name']} approved.")
            await send_message(business["telegram_id"], "Your business has been approved! You can now add discounts and giveaways.")
        except ValueError:
            await send_message(chat_id, f"Invalid business ID format: {business_id}")
        except Exception as e:
            logger.error(f"Failed to approve business {business_id}: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to approve business. Please try again.")
        return {"ok": True}

    if chat_id == int(ADMIN_CHAT_ID) and text.startswith("/reject_"):
        business_id = text[len("/reject_"):]
        try:
            uuid.UUID(business_id)
            business = await supabase_find_business(business_id)
            if not business:
                await send_message(chat_id, f"Business with ID {business_id} not found.")
                return {"ok": True}
            def _update_business():
                return supabase.table("businesses").update({"status": "rejected", "updated_at": now_iso()}).eq("id", business_id).execute()
            await asyncio.to_thread(_update_business)
            await send_message(chat_id, f"Business {business['name']} rejected.")
            await send_message(business["telegram_id"], "Your business registration was rejected. Please contact support.")
        except ValueError:
            await send_message(chat_id, f"Invalid business ID format: {business_id}")
        except Exception as e:
            logger.error(f"Failed to reject business {business_id}: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to reject business. Please try again.")
        return {"ok": True}

    # Handle /menu
    if text.lower() == "/menu":
        await send_message(chat_id, "Choose an option:", reply_markup=create_menu_options_keyboard())
        return {"ok": True}

    # Handle phone number
    if contact and state.get("stage") == "awaiting_phone_profile":
        phone_number = contact.get("phone_number")
        if not phone_number:
            await send_message(chat_id, "Invalid phone number. Please try again:", reply_markup=create_phone_keyboard())
            return {"ok": True}
        state["data"]["phone_number"] = phone_number
        entry_id = state.get("entry_id")
        if entry_id:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"phone_number": phone_number})
        registered = await supabase_find_registered(chat_id)
        if registered and not registered.get("dob"):
            await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:")
            state["stage"] = "awaiting_dob_profile"
        else:
            interests = registered.get("interests", []) or []
            interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
            await send_message(chat_id, f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}")
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
        set_state(chat_id, state)
        return {"ok": True}

    # Handle DOB (initial registration)
    if state.get("stage") == "awaiting_dob":
        if text.lower() == "/skip":
            state["data"]["dob"] = None
            entry_id = state.get("entry_id")
            if entry_id:
                await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": None})
            state["stage"] = "awaiting_interests"
            await send_message(chat_id, "Choose exactly 3 interests for this month:", reply_markup=create_interests_keyboard())
            set_state(chat_id, state)
            return {"ok": True}
        try:
            dob_obj = datetime.strptime(text, "%Y-%m-%d").date()
            if dob_obj.year < 1900 or dob_obj > datetime.now().date():
                await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.")
                return {"ok": True}
            state["data"]["dob"] = dob_obj.isoformat()
            entry_id = state.get("entry_id")
            if entry_id:
                await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": state["data"]["dob"]})
            state["stage"] = "awaiting_interests"
            await send_message(chat_id, "Choose exactly 3 interests for this month:", reply_markup=create_interests_keyboard())
            set_state(chat_id, state)
        except ValueError:
            await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.")
        return {"ok": True}

    # Handle DOB (profile update)
    if state.get("stage") == "awaiting_dob_profile":
        if text.lower() == "/skip":
            state["data"]["dob"] = None
            entry_id = state.get("entry_id")
            if entry_id:
                await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": None})
            registered = await supabase_find_registered(chat_id)
            interests = registered.get("interests", []) or []
            interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
            await send_message(chat_id, f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}")
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
            return {"ok": True}
        try:
            dob_obj = datetime.strptime(text, "%Y-%m-%d").date()
            if dob_obj.year < 1900 or dob_obj > datetime.now().date():
                await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.")
                return {"ok": True}
            state["data"]["dob"] = dob_obj.isoformat()
            entry_id = state.get("entry_id")
            if entry_id:
                await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": state["data"]["dob"]})
            registered = await supabase_find_registered(chat_id)
            interests = registered.get("interests", []) or []
            interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
            await send_message(chat_id, f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}")
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
            return {"ok": True}
        except ValueError:
            await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.")
        return {"ok": True}

    # Handle /start
    if text.lower().startswith("/start"):
        if text.lower() != "/start":
            business_id = text[len("/start "):]
            try:
                uuid.UUID(business_id)
                state["referred_by"] = business_id
            except ValueError:
                logger.error(f"Invalid referral business_id: {business_id}")
        registered = await supabase_find_registered(chat_id)
        if registered:
            await send_message(chat_id, "You're already registered! Explore options:", reply_markup=create_main_menu_keyboard())
            return {"ok": True}
        existing = await supabase_find_draft(chat_id)
        if existing:
            state = {
                "stage": "awaiting_gender",
                "data": {"language": existing.get("language")},
                "entry_id": existing.get("id"),
                "selected_interests": []
            }
            await send_message(chat_id, "What's your gender? (optional, helps target offers)", reply_markup=create_gender_keyboard())
        else:
            state = {"stage": "awaiting_language", "data": {}, "entry_id": None, "selected_interests": []}
            await send_message(chat_id, "Welcome! Choose your language:", reply_markup=create_language_keyboard())
        set_state(chat_id, state)
        return {"ok": True}

    return {"ok": True}

async def handle_callback_query(callback_query: Dict[str, Any]):
    chat_id = callback_query.get("from", {}).get("id")
    callback_data = callback_query.get("data")
    message_id = callback_query.get("message", {}).get("message_id")
    if not chat_id or not callback_data or not message_id:
        logger.error(f"Invalid callback query: chat_id={chat_id}, callback_data={callback_data}, message_id={message_id}")
        return {"ok": True}

    registered = await supabase_find_registered(chat_id)
    state = get_state(chat_id) or {}

    # Handle business approval/rejection (from inline buttons)
    if chat_id == int(ADMIN_CHAT_ID) and callback_data.startswith("approve:"):
        business_id = callback_data[len("approve:"):]
        try:
            uuid.UUID(business_id)
            business = await supabase_find_business(business_id)
            if not business:
                await send_message(chat_id, f"Business with ID {business_id} not found.")
                await safe_clear_markup(chat_id, message_id)
                return {"ok": True}
            def _update_business():
                return supabase.table("businesses").update({"status": "approved", "updated_at": now_iso()}).eq("id", business_id).execute()
            await asyncio.to_thread(_update_business)
            await send_message(chat_id, f"Business {business['name']} approved.")
            await send_message(business["telegram_id"], "Your business has been approved! You can now add discounts and giveaways.")
            await safe_clear_markup(chat_id, message_id)
        except ValueError:
            await send_message(chat_id, f"Invalid business ID format: {business_id}")
        except Exception as e:
            logger.error(f"Failed to approve business {business_id}: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to approve business. Please try again.")
        return {"ok": True}

    if chat_id == int(ADMIN_CHAT_ID) and callback_data.startswith("reject:"):
        business_id = callback_data[len("reject:"):]
        try:
            uuid.UUID(business_id)
            business = await supabase_find_business(business_id)
            if not business:
                await send_message(chat_id, f"Business with ID {business_id} not found.")
                await safe_clear_markup(chat_id, message_id)
                return {"ok": True}
            def _update_business():
                return supabase.table("businesses").update({"status": "rejected", "updated_at": now_iso()}).eq("id", business_id).execute()
            await asyncio.to_thread(_update_business)
            await send_message(chat_id, f"Business {business['name']} rejected.")
            await send_message(business["telegram_id"], "Your business registration was rejected. Please contact support.")
            await safe_clear_markup(chat_id, message_id)
        except ValueError:
            await send_message(chat_id, f"Invalid business ID format: {business_id}")
        except Exception as e:
            logger.error(f"Failed to reject business {business_id}: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to reject business. Please try again.")
        return {"ok": True}

    # Handle giveaway approval/rejection
    if chat_id == int(ADMIN_CHAT_ID) and callback_data.startswith("giveaway_approve:"):
        giveaway_id = callback_data[len("giveaway_approve:"):]
        try:
            uuid.UUID(giveaway_id)
            giveaway = await supabase_find_giveaway(giveaway_id)
            if not giveaway:
                await send_message(chat_id, f"Giveaway with ID {giveaway_id} not found.")
                await safe_clear_markup(chat_id, message_id)
                return {"ok": True}
            def _update_giveaway():
                return supabase.table("giveaways").update({"active": True, "updated_at": now_iso()}).eq("id", giveaway_id).execute()
            await asyncio.to_thread(_update_giveaway)
            await send_message(chat_id, f"Approved {giveaway['business_type']}: {giveaway['name']}.")
            business = await supabase_find_business(giveaway["business_id"])
            await send_message(business["telegram_id"], f"Your {giveaway['business_type']} '{giveaway['name']}' is approved and live!")
            await notify_users(giveaway_id)
            await safe_clear_markup(chat_id, message_id)
        except ValueError:
            await send_message(chat_id, f"Invalid giveaway ID: {giveaway_id}")
        except Exception as e:
            logger.error(f"Failed to approve giveaway {giveaway_id}: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to approve giveaway. Please try again.")
        return {"ok": True}

    if chat_id == int(ADMIN_CHAT_ID) and callback_data.startswith("giveaway_reject:"):
        giveaway_id = callback_data[len("giveaway_reject:"):]
        try:
            uuid.UUID(giveaway_id)
            giveaway = await supabase_find_giveaway(giveaway_id)
            if not giveaway:
                await send_message(chat_id, f"Giveaway with ID {giveaway_id} not found.")
                await safe_clear_markup(chat_id, message_id)
                return {"ok": True}
            def _update_giveaway():
                return supabase.table("giveaways").update({"active": False, "updated_at": now_iso()}).eq("id", giveaway_id).execute()
            await asyncio.to_thread(_update_giveaway)
            await send_message(chat_id, f"Rejected {giveaway['business_type']}: {giveaway['name']}.")
            business = await supabase_find_business(giveaway["business_id"])
            await send_message(business["telegram_id"], f"Your {giveaway['business_type']} '{giveaway['name']}' was rejected. Contact support.")
            await safe_clear_markup(chat_id, message_id)
        except ValueError:
            await send_message(chat_id, f"Invalid giveaway ID: {giveaway_id}")
        except Exception as e:
            logger.error(f"Failed to reject giveaway {giveaway_id}: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to reject giveaway. Please try again.")
        return {"ok": True}

    # Menu options
    if callback_data == "menu:main":
        await safe_clear_markup(chat_id, message_id)
        await send_message(chat_id, "Explore options:", reply_markup=create_main_menu_keyboard())
        return {"ok": True}
    elif callback_data == "menu:language":
        await safe_clear_markup(chat_id, message_id)
        await send_message(chat_id, "Choose your language:", reply_markup=create_language_keyboard())
        state["stage"] = "awaiting_language_change"
        set_state(chat_id, state)
        return {"ok": True}

    # Language selection
    if state.get("stage") in ["awaiting_language", "awaiting_language_change"] and callback_data.startswith("lang:"):
        language = callback_data[len("lang:"):]
        if language not in ["en", "ru"]:
            await send_message(chat_id, "Invalid language:", reply_markup=create_language_keyboard())
            return {"ok": True}
        state["data"]["language"] = language
        entry_id = state.get("entry_id")
        if not entry_id:
            created = await supabase_insert_return("central_bot_leads", {"telegram_id": chat_id, "language": language, "is_draft": True})
            state["entry_id"] = created.get("id") if created else None
        else:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"language": language})
        await safe_clear_markup(chat_id, message_id)
        if state.get("stage") == "awaiting_language":
            await send_message(chat_id, "What's your gender? (optional, helps target offers)", reply_markup=create_gender_keyboard())
            state["stage"] = "awaiting_gender"
        else:
            await send_message(chat_id, "Language updated! Explore options:", reply_markup=create_main_menu_keyboard())
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
        set_state(chat_id, state)
        return {"ok": True}

    # Gender selection
    if state.get("stage") == "awaiting_gender" and callback_data.startswith("gender:"):
        gender = callback_data[len("gender:"):]
        if gender not in ["female", "male"]:
            await send_message(chat_id, "Invalid gender:", reply_markup=create_gender_keyboard())
            return {"ok": True}
        state["data"]["gender"] = gender
        entry_id = state.get("entry_id")
        if entry_id:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"gender": gender})
        await safe_clear_markup(chat_id, message_id)
        await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:")
        state["stage"] = "awaiting_dob"
        set_state(chat_id, state)
        return {"ok": True}

    # Interests selection
    if state.get("stage") == "awaiting_interests":
        if callback_data.startswith("interest:"):
            interest = callback_data[len("interest:"):]
            if interest not in INTERESTS:
                logger.warning(f"Invalid interest selected: {interest}")
                return {"ok": True}
            selected = state.get("selected_interests", [])
            if interest in selected:
                selected.remove(interest)
            elif len(selected) < 3:
                selected.append(interest)
            state["selected_interests"] = selected
            await edit_message_keyboard(chat_id, message_id, create_interests_keyboard(selected))
            set_state(chat_id, state)
            return {"ok": True}
        elif callback_data == "interests_done":
            selected = state.get("selected_interests", [])
            if len(selected) != 3:
                await send_message(chat_id, f"Please select exactly 3 interests (currently {len(selected)}):", reply_markup=create_interests_keyboard(selected))
                return {"ok": True}
            await send_message(chat_id, "Interests saved! Finalizing registration...")
            entry_id = state.get("entry_id")
            if entry_id:
                await supabase_update_by_id_return("central_bot_leads", entry_id, {
                    "interests": selected,
                    "is_draft": False,
                    "points": STARTER_POINTS
                })
            await send_message(chat_id, f"Congrats! You've earned {STARTER_POINTS} points. Explore options:", reply_markup=create_main_menu_keyboard())
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
            return {"ok": True}

    # Registered user actions
    if registered:
        if callback_data == "menu:points":
            points = registered.get("points", 0)
            await send_message(chat_id, f"Your balance: *{points} points*")
            return {"ok": True}
        elif callback_data == "menu:profile":
            if not registered.get("phone_number"):
                await send_message(chat_id, "Please share your phone number to complete your profile:", reply_markup=create_phone_keyboard())
                state["stage"] = "awaiting_phone_profile"
                state["data"] = registered
                state["entry_id"] = registered["id"]
                set_state(chat_id, state)
                return {"ok": True}
            if not registered.get("dob"):
                await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:")
                state["stage"] = "awaiting_dob_profile"
                state["data"] = registered
                state["entry_id"] = registered["id"]
                set_state(chat_id, state)
                return {"ok": True}
            interests = registered.get("interests", []) or []
            interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
            await send_message(chat_id, f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}")
            return {"ok": True}
        elif callback_data == "menu:discounts":
            if not registered.get("phone_number") or not registered.get("dob"):
                await send_message(chat_id, "Complete your profile to access discounts:", reply_markup=create_phone_keyboard())
                state["stage"] = "awaiting_phone_profile"
                state["data"] = registered
                state["entry_id"] = registered["id"]
                set_state(chat_id, state)
                return {"ok": True}
            interests = registered.get("interests", []) or []
            if not interests:
                await send_message(chat_id, "No interests set. Please update your profile.")
                return {"ok": True}
            await send_message(chat_id, "Choose a category for discounts:", reply_markup=create_categories_keyboard())
            return {"ok": True}
        elif callback_data == "menu:giveaways":
            try:
                if not await has_redeemed_discount(chat_id):
                    await send_message(chat_id, "Claim a discount first to unlock giveaways. Check Discounts:", reply_markup=create_main_menu_keyboard())
                    return {"ok": True}
                interests = registered.get("interests", []) or []
                if not interests:
                    await send_message(chat_id, "No interests set. Please update your profile.")
                    return {"ok": True}
                def _query_giveaways():
                    return supabase.table("giveaways").select("*").in_("category", interests).eq("active", True).eq("business_type", "giveaway").execute()
                resp = await asyncio.to_thread(_query_giveaways)
                giveaways = resp.data if hasattr(resp, "data") else resp.get("data", [])
                if not giveaways:
                    await send_message(chat_id, "No giveaways available for your interests. Check Discover Offers:", reply_markup=create_main_menu_keyboard())
                    return {"ok": True}
                for g in giveaways:
                    business_type = g.get("business_type", "salon").capitalize()
                    cost = g.get("cost", 200)
                    message = f"{business_type}: *{g['name']}* at {g.get('salon_name')} ({g.get('category')})"
                    keyboard = {"inline_keyboard": [
                        [{"text": f"Join ({cost} pts)", "callback_data": f"giveaway_points:{g['id']}"}],
                        [{"text": "Join via Booking", "callback_data": f"giveaway_book:{g['id']}"}]
                    ]}
                    await send_message(chat_id, message, keyboard)
            except Exception as e:
                logger.error(f"Failed to fetch giveaways for chat_id {chat_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to load giveaways. Please try again later.")
            return {"ok": True}
        elif callback_data.startswith("discount_category:"):
            category = callback_data[len("discount_category:"):]
            if category not in CATEGORIES:
                await send_message(chat_id, "Invalid category.")
                return {"ok": True}
            try:
                def _query_discounts():
                    return supabase.table("discounts").select("id, name, discount_percentage, category, business_id").eq("category", category).eq("active", True).execute()
                resp = await asyncio.to_thread(_query_discounts)
                discounts = resp.data if hasattr(resp, "data") else resp.get("data", [])
                if not discounts:
                    await send_message(chat_id, f"No discounts available in *{category}*.")
                    return {"ok": True}
                for d in discounts:
                    business = await supabase_find_business(d["business_id"])
                    if not business:
                        await send_message(chat_id, f"Business not found for discount {d['name']}.")
                        continue
                    # Fetch business categories
                    def _query_categories():
                        return supabase.table("business_categories").select("category").eq("business_id", d["business_id"]).execute()
                    categories_resp = await asyncio.to_thread(_query_categories)
                    categories = [cat["category"] for cat in (categories_resp.data if hasattr(categories_resp, "data") else categories_resp.get("data", []))] or ["None"]
                    location = business.get("location", "Unknown")
                    message = (
                        f"Discount: *{d['name']}*\n"
                        f"Category: *{d['category']}*\n"
                        f"Percentage: {d['discount_percentage']}%\n"
                        f"At: {business['name']}\n"
                        f"Location: {location}\n"
                        f"Business Categories: {', '.join(categories)}"
                    )
                    keyboard = {"inline_keyboard": [
                        [
                            {"text": "View Profile", "callback_data": f"profile:{d['business_id']}"},
                            {"text": "View Services", "callback_data": f"services:{d['business_id']}"}
                        ],
                        [
                            {"text": "Book", "callback_data": f"book:{d['business_id']}"},
                            {"text": "Get Discount", "callback_data": f"get_discount:{d['id']}"}
                        ]
                    ]}
                    await send_message(chat_id, message, keyboard)
            except Exception as e:
                logger.error(f"Failed to fetch discounts for category {category}, chat_id {chat_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to load discounts. Please try again later.")
            return {"ok": True}
        elif callback_data.startswith("profile:"):
            business_id = callback_data[len("profile:"):]
            try:
                business = await supabase_find_business(business_id)
                if not business:
                    await send_message(chat_id, "Business not found.")
                    return {"ok": True}
                # Fetch business categories
                def _query_categories():
                    return supabase.table("business_categories").select("category").eq("business_id", business_id).execute()
                categories_resp = await asyncio.to_thread(_query_categories)
                categories = [cat["category"] for cat in (categories_resp.data if hasattr(categories_resp, "data") else categories_resp.get("data", []))] or ["None"]
                work_days = business.get("work_days", []) or ["Not set"]
                msg = (
                    f"Business Profile:\n"
                    f"Name: {business['name']}\n"
                    f"Categories: {', '.join(categories)}\n"
                    f"Location: {business.get('location', 'Not set')}\n"
                    f"Phone: {business.get('phone_number', 'Not set')}\n"
                    f"Work Days: {', '.join(work_days)}"
                )
                await send_message(chat_id, msg)
            except Exception as e:
                logger.error(f"Failed to fetch business profile {business_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to load profile.")
            return {"ok": True}
        elif callback_data.startswith("services:"):
            business_id = callback_data[len("services:"):]
            try:
                business = await supabase_find_business(business_id)
                if not business:
                    await send_message(chat_id, "Business not found.")
                    return {"ok": True}
                prices = business.get("prices", {})
                msg = "Services:\n" + "\n".join(f"{k}: {v}" for k, v in prices.items()) if prices else "No services listed."
                await send_message(chat_id, msg)
            except Exception as e:
                logger.error(f"Failed to fetch business services {business_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to load services.")
            return {"ok": True}
        elif callback_data.startswith("book:"):
            business_id = callback_data[len("book:"):]
            try:
                business = await supabase_find_business(business_id)
                if not business:
                    await send_message(chat_id, "Business not found.")
                    return {"ok": True}
                msg = f"To book, please contact {business['name']} at {business.get('phone_number', 'Not set')}."
                await send_message(chat_id, msg)
            except Exception as e:
                logger.error(f"Failed to fetch book info {business_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to load booking info.")
            return {"ok": True}
        elif callback_data.startswith("get_discount:"):
            discount_id = callback_data[len("get_discount:"):]
            try:
                uuid.UUID(discount_id)
                discount = await supabase_find_discount(discount_id)
                if not discount or not discount["active"]:
                    await send_message(chat_id, "Discount not found or inactive.")
                    return {"ok": True}
                if not discount.get("business_id"):
                    logger.error(f"Missing business_id for discount_id: {discount_id}")
                    await send_message(chat_id, "Sorry, this discount is unavailable due to a configuration issue. Please try another.")
                    return {"ok": True}
                code, expiry = await generate_discount_code(chat_id, discount["business_id"], discount_id)
                await send_message(chat_id, f"Your promo code: *{code}* for {discount['name']}. Valid until {expiry.split('T')[0]}.")
            except ValueError as ve:
                await send_message(chat_id, str(ve))
            except Exception as e:
                logger.error(f"Failed to generate discount code for discount_id: {discount_id}, chat_id: {chat_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to generate promo code. Please try again later.")
            return {"ok": True}
        elif callback_data.startswith("giveaway_points:"):
            giveaway_id = callback_data[len("giveaway_points:"):]
            try:
                uuid.UUID(giveaway_id)
                def _query_giveaway():
                    return supabase.table("giveaways").select("*").eq("id", giveaway_id).eq("active", True).limit(1).execute()
                resp = await asyncio.to_thread(_query_giveaway)
                giveaway = resp.data[0] if resp.data else None
                if not giveaway:
                    await send_message(chat_id, "Giveaway not found or inactive.")
                    return {"ok": True}
                if not giveaway.get("business_id"):
                    logger.error(f"Missing business_id for giveaway_id: {giveaway_id}")
                    await send_message(chat_id, "Sorry, this giveaway is unavailable due to a configuration issue. Please try another.")
                    return {"ok": True}
                cost = giveaway.get("cost", 200)
                if registered.get("points", 0) < cost:
                    await send_message(chat_id, f"Not enough points (need {cost}).")
                    return {"ok": True}
                def _check_existing():
                    current_month = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    return supabase.table("user_giveaways").select("id").eq("telegram_id", chat_id).eq("giveaway_id", giveaway_id).gte("joined_at", current_month.isoformat()).execute()
                resp = await asyncio.to_thread(_check_existing)
                existing = resp.data if hasattr(resp, "data") else resp.get("data", [])
                if existing:
                    await send_message(chat_id, "You've already joined this giveaway this month.")
                    return {"ok": True}
                await supabase_update_by_id_return("central_bot_leads", registered["id"], {"points": registered["points"] - cost})
                code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "loser")
                await supabase_insert_return("user_giveaways", {
                    "telegram_id": chat_id,
                    "giveaway_id": giveaway_id,
                    "business_id": giveaway["business_id"],
                    "entry_status": "pending",
                    "joined_at": now_iso()
                })
                business_type = giveaway.get("business_type", "salon").capitalize()
                await send_message(chat_id, f"Joined {business_type} {giveaway['name']} with {cost} points. Your 20% loser discount code: *{code}*, valid until {expiry.split('T')[0]}.")
            except ValueError:
                logger.error(f"Invalid giveaway_id format: {giveaway_id}")
                await send_message(chat_id, "Invalid giveaway ID.")
            except Exception as e:
                logger.error(f"Failed to process giveaway_points for giveaway_id: {giveaway_id}, chat_id: {chat_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to join giveaway. Please try again later.")
            return {"ok": True}
        elif callback_data.startswith("giveaway_book:"):
            giveaway_id = callback_data[len("giveaway_book:"):]
            try:
                uuid.UUID(giveaway_id)
                def _query_giveaway():
                    return supabase.table("giveaways").select("*").eq("id", giveaway_id).eq("active", True).limit(1).execute()
                resp = await asyncio.to_thread(_query_giveaway)
                giveaway = resp.data[0] if resp.data else None
                if not giveaway:
                    await send_message(chat_id, "Giveaway not found or inactive.")
                    return {"ok": True}
                if not giveaway.get("business_id"):
                    logger.error(f"Missing business_id for giveaway_id: {giveaway_id}")
                    await send_message(chat_id, "Sorry, this giveaway is unavailable due to a configuration issue. Please try another.")
                    return {"ok": True}
                def _check_existing():
                    current_month = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    return supabase.table("user_giveaways").select("id").eq("telegram_id", chat_id).eq("giveaway_id", giveaway_id).gte("joined_at", current_month.isoformat()).execute()
                resp = await asyncio.to_thread(_check_existing)
                existing = resp.data if hasattr(resp, "data") else resp.get("data", [])
                if existing:
                    await send_message(chat_id, "You've already joined this giveaway this month.")
                    return {"ok": True}
                code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "awaiting_booking")
                await supabase_insert_return("user_giveaways", {
                    "telegram_id": chat_id,
                    "giveaway_id": giveaway_id,
                    "business_id": giveaway["business_id"],
                    "entry_status": "awaiting_booking",
                    "joined_at": now_iso()
                })
                business_type = giveaway.get("business_type", "salon").capitalize()
                await send_message(chat_id, f"Book a service at {business_type} {giveaway.get('salon_name')} with code *{code}* to join {giveaway['name']}. Valid until {expiry.split('T')[0]}.")
            except ValueError:
                logger.error(f"Invalid giveaway_id format: {giveaway_id}")
                await send_message(chat_id, "Invalid giveaway ID.")
            except Exception as e:
                logger.error(f"Failed to process giveaway_book for giveaway_id: {giveaway_id}, chat_id: {chat_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to join giveaway. Please try again later.")
            return {"ok": True}

    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    asyncio.run(initialize_bot())
    uvicorn.run(app, host="0.0.0.0", port=8000)
