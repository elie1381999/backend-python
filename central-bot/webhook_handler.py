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
                if e.response.status_code in [403, 400]:
                    return {"ok": False, "error": f"HTTP {e.response.status_code} (skipped)"}
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
        data = resp.data if hasattr(resp, "data") else resp.get("data", [])
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
        data = resp.data if hasattr(resp, "data") else resp.get("data", [])
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
        data = resp.data if hasattr(resp, "data") else resp.get("data", [])
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
        data = resp.data if hasattr(resp, "data") else resp.get("data", [])
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
        data = resp.data if hasattr(resp, "data") else resp.get("data", [])
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
        data = resp.data if hasattr(resp, "data") else resp.get("data", [])
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
        data = resp.data if hasattr(resp, "data") else resp.get("data", [])
        if not data:
            logger.info(f"No giveaway found for giveaway_id {giveaway_id}")
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_find_giveaway failed for giveaway_id {giveaway_id}: {str(e)}", exc_info=True)
        return None

async def notify_users(giveaway_id: str):
    try:
        giveaway = await supabase_find_giveaway(giveaway_id)
        if not giveaway:
            logger.error(f"Giveaway {giveaway_id} not found for notification")
            return
        def _get_users():
            return supabase.table("central_bot_leads").select("telegram_id").eq("is_draft", False).contains("interests", [giveaway["category"]]).execute()
        users_resp = await asyncio.to_thread(_get_users)
        users = users_resp.data if hasattr(users_resp, "data") else users_resp.get("data", [])
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

async def lifespan(app: FastAPI):
    await initialize_bot()
    yield

# Initialize FastAPI app with lifespan
app = FastAPI(lifespan=lifespan)

@app.post("/hook/central_bot")
async def webhook_handler(request: Request) -> Response:
    try:
        update = await request.json()
        if not update:
            logger.error("Received empty update from Telegram")
            return Response(status_code=200)
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
            await award_profile_bonus(chat_id, supabase)  # Call to award bonus if complete
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
                await award_profile_bonus(chat_id, supabase)  # Call to award bonus if complete
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
                await award_profile_bonus(chat_id, supabase)  # Call to award bonus if complete
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
        referral_code = text[len("/start "):].strip() if len(text) > len("/start") else None
        registered = await supabase_find_registered(chat_id)
        if registered:
            await send_message(chat_id, "You're already registered! Explore options:", reply_markup=create_main_menu_keyboard())
            return {"ok": True}
        existing = await supabase_find_draft(chat_id)
        referrer_id = None
        if referral_code:
            def _find_referrer():
                return supabase.table("central_bot_leads").select("id").eq("referral_code", referral_code).limit(1).execute()
            referrer_resp = await asyncio.to_thread(_find_referrer)
            if referrer_resp.data:
                referrer_id = referrer_resp.data[0]["id"]
            else:
                logger.warning(f"Invalid referral_code: {referral_code} for chat_id {chat_id}")
        if existing:
            state = {
                "stage": "awaiting_gender",
                "data": {"language": existing.get("language")},
                "entry_id": existing.get("id"),
                "selected_interests": []
            }
            if referrer_id:
                state["data"]["referred_by"] = referrer_id
            await send_message(chat_id, "What's your gender? (optional, helps target offers)", reply_markup=create_gender_keyboard())
        else:
            state = {"stage": "awaiting_language", "data": {}, "entry_id": None, "selected_interests": []}
            if referrer_id:
                state["data"]["referred_by"] = referrer_id
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
                interests = registered.get("interests", []) or []
                if not interests:
                    await send_message(chat_id, "No interests set. Please update your profile.")
                    return {"ok": True}
                def _query_giveaways():
                    return supabase.table("giveaways").select("*").in_("category", interests).eq("active", True).execute()  # Removed eq("business_type", "giveaway")
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
        elif callback_data.startswith("get_discount:"):
            discount_id = callback_data[len("get_discount:"):]
            try:
                if await has_redeemed_discount(chat_id, supabase):
                    await send_message(chat_id, "You have already redeemed a discount this month.")
                    return {"ok": True}
                discount = await supabase_find_discount(discount_id)
                if not discount:
                    await send_message(chat_id, "Discount not found.")
                    return {"ok": True}
                code, expiry = await generate_discount_code(chat_id, discount["business_id"], discount_id, supabase)
                await send_message(chat_id, f"Your discount promo code: *{code}*\nExpires: {expiry}")
            except ValueError as ve:
                await send_message(chat_id, str(ve))
            except Exception as e:
                logger.error(f"Failed to get discount {discount_id} for chat_id {chat_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to generate discount code. Please try again.")
            return {"ok": True}
        elif callback_data.startswith("giveaway_points:"):
            giveaway_id = callback_data[len("giveaway_points:"):]
            try:
                giveaway = await supabase_find_giveaway(giveaway_id)
                if not giveaway:
                    await send_message(chat_id, "Giveaway not found.")
                    return {"ok": True}
                cost = giveaway.get("cost", 200)
                user_points = (await get_user_points(chat_id, supabase))["points"]
                if user_points < cost:
                    await send_message(chat_id, f"You need {cost} points to join. You have {user_points}.")
                    return {"ok": True}
                # Deduct points (update user points)
                def _update_points():
                    return supabase.table("central_bot_leads").update({"points": user_points - cost}).eq("telegram_id", chat_id).execute()
                await asyncio.to_thread(_update_points)
                code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "standard", supabase)
                await send_message(chat_id, f"Joined giveaway with points! Promo code: *{code}*\nExpires: {expiry}")
            except Exception as e:
                logger.error(f"Failed to join giveaway {giveaway_id} with points for chat_id {chat_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to join giveaway. Please try again.")
            return {"ok": True}
        elif callback_data.startswith("giveaway_book:"):
            giveaway_id = callback_data[len("giveaway_book:"):]
            try:
                giveaway = await supabase_find_giveaway(giveaway_id)
                if not giveaway:
                    await send_message(chat_id, "Giveaway not found.")
                    return {"ok": True}
                code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "awaiting_booking", supabase)
                await send_message(chat_id, f"Joined giveaway via booking! Promo code: *{code}*\nExpires: {expiry}\nBook now to confirm.")
            except Exception as e:
                logger.error(f"Failed to join giveaway {giveaway_id} via booking for chat_id {chat_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to join giveaway. Please try again.")
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
                # Query services table instead of non-existent 'prices'
                def _query_services():
                    return supabase.table("services").select("name, price, category").eq("business_id", business_id).execute()
                services_resp = await asyncio.to_thread(_query_services)
                services = services_resp.data if hasattr(services_resp, "data") else services_resp.get("data", [])
                msg = "Services:\n" + "\n".join(f"{s['name']} ({s['category']}): {s['price']}" for s in services) if services else "No services listed."
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

    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
