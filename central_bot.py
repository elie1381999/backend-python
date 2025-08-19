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
from fastapi import FastAPI, Request, Response
from starlette.responses import PlainTextResponse
from convo_central import handle_message, handle_callback

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
BOT_USERNAME = os.getenv("BOT_USERNAME")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
VERIFY_KEY = os.getenv("VERIFY_KEY")

if not all([BOT_TOKEN, BOT_USERNAME, SUPABASE_URL, SUPABASE_KEY, ADMIN_CHAT_ID, WEBHOOK_URL]):
    raise RuntimeError("CENTRAL_BOT_TOKEN, BOT_USERNAME, SUPABASE_URL, SUPABASE_KEY, ADMIN_CHAT_ID, and WEBHOOK_URL must be set in .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Constants
INTERESTS = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining", "Discounts only", "Giveaways only"]
CATEGORIES = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining"]
STARTER_POINTS = 100
POINTS_PROFILE_COMPLETE = 40
POINTS_CLAIM_PROMO = 10
POINTS_BOOKING_CREATED = 15
POINTS_REFERRAL_JOIN = 10
POINTS_REFERRAL_VERIFIED = 100
EMOJIS = ["1️⃣", "2️⃣", "3️⃣"]
STATE_TTL_SECONDS = 30 * 60

# In-memory state
USER_STATES: Dict[int, Dict[str, Any]] = {}

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

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
            [{"text": "Giveaways", "callback_data": "menu:giveaways"}],
            [{"text": "Refer Friends", "callback_data": "menu:refer"}]
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

async def award_points(user_id: str, delta: int, reason: str, booking_id: Optional[str] = None) -> dict:
    if delta == 0:
        return {"ok": True, "message": "no-op"}
    def _get_user():
        return supabase.table("central_bot_leads").select("*").eq("id", user_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_get_user)
        rows = resp.data if hasattr(resp, "data") else resp.get("data", []) or []
        if not rows:
            return {"ok": False, "error": "user_not_found"}
        user = rows[0]
    except Exception:
        logger.exception("award_points: fetch user failed")
        return {"ok": False, "error": "fetch_failed"}
    old_points = int(user.get("points") or 0)
    new_points = max(0, old_points + delta)
    def _upd_user():
        return supabase.table("central_bot_leads").update({"points": new_points, "last_login": now_iso()}).eq("id", user_id).execute()
    try:
        await asyncio.to_thread(_upd_user)
    except Exception:
        logger.exception("award_points: update failed")
        return {"ok": False, "error": "update_failed"}
    hist = {"user_id": user_id, "points": delta, "reason": reason, "awarded_at": now_iso()}
    await supabase_insert_return("points_history", hist)
    logger.info(f"Awarded {delta} pts to user {user_id} for {reason} ({old_points} -> {new_points})")
    if reason.startswith("booking_verified:"):
        referred_by = user.get("referred_by")
        if referred_by:
            ref_reason = f"referral_booking_verified:{booking_id or reason.split(':')[-1]}"
            if not await has_history(referred_by, ref_reason):
                try:
                    await award_points(referred_by, POINTS_REFERRAL_VERIFIED, ref_reason, booking_id)
                except Exception:
                    logger.exception(f"Failed to award referral bonus for {ref_reason}")
    return {"ok": True, "old_points": old_points, "new_points": new_points}

async def has_history(user_id: str, reason: str) -> bool:
    def _q():
        return supabase.table("points_history").select("id").eq("user_id", user_id).eq("reason", reason).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = resp.data if hasattr(resp, "data") else resp.get("data", []) or []
        return bool(rows)
    except Exception:
        logger.exception("has_history failed")
        return False

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
    def _check_claimed():
        return supabase.table("user_discounts").select("id").eq("telegram_id", chat_id).eq("discount_id", discount_id).execute()
    claimed = await asyncio.to_thread(_check_claimed)
    if claimed.data:
        raise ValueError("Already claimed this discount")
    while True:
        code = f"{random.randint(0, 9999):04d}"
        def _check_existing_code():
            return supabase.table("user_discounts").select("promo_code").eq("promo_code", code).eq("business_id", business_id).execute()
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

async def generate_promo_code(chat_id: int, business_id: str, giveaway_id: str, entry_status: str) -> tuple[str, str]:
    if not business_id or not giveaway_id:
        logger.error(f"Invalid business_id: {business_id} or giveaway_id: {giveaway_id} for chat_id {chat_id}")
        raise ValueError("Business ID or giveaway ID is missing or invalid")
    while True:
        code = f"{random.randint(0, 9999):04d}"
        def _check_existing_code():
            return supabase.table("user_giveaways").select("promo_code").eq("promo_code", code).eq("business_id", business_id).execute()
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
        "entry_status": entry_status,
        "joined_at": now_iso()
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
        def _q_users():
            return supabase.table("central_bot_leads").select("telegram_id").eq("is_draft", False).contains("interests", [giveaway["category"]]).execute()
        resp = await asyncio.to_thread(_q_users)
        users = resp.data if hasattr(resp, "data") else resp.get("data", [])
        for user in users:
            await send_message(
                user["telegram_id"],
                f"New {giveaway['category']} offer: *{giveaway['name']}* at {giveaway.get('salon_name', 'Unknown')}. Check it out:",
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
        callback_query = update.get("callback_query")
        chat_id = (message.get("chat", {}).get("id") if message else None) or (callback_query.get("from", {}).get("id") if callback_query else None)

        if chat_id and chat_id == int(ADMIN_CHAT_ID):
            if message and message.get("text", "").startswith("/approve_"):
                business_id = message["text"][len("/approve_"):]
                try:
                    uuid.UUID(business_id)
                    business = await supabase_find_business(business_id)
                    if not business:
                        await send_message(chat_id, f"Business with ID {business_id} not found.")
                        return Response(status_code=200)
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
                return Response(status_code=200)

            if message and message.get("text", "").startswith("/reject_"):
                business_id = message["text"][len("/reject_"):]
                try:
                    uuid.UUID(business_id)
                    business = await supabase_find_business(business_id)
                    if not business:
                        await send_message(chat_id, f"Business with ID {business_id} not found.")
                        return Response(status_code=200)
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
                return Response(status_code=200)

            if callback_query:
                callback_data = callback_query.get("data")
                message_id = callback_query.get("message", {}).get("message_id")
                if callback_data and callback_data.startswith("approve:"):
                    business_id = callback_data[len("approve:"):]
                    try:
                        uuid.UUID(business_id)
                        business = await supabase_find_business(business_id)
                        if not business:
                            await send_message(chat_id, f"Business with ID {business_id} not found.")
                            await safe_clear_markup(chat_id, message_id)
                            return Response(status_code=200)
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
                    return Response(status_code=200)

                if callback_data and callback_data.startswith("reject:"):
                    business_id = callback_data[len("reject:"):]
                    try:
                        uuid.UUID(business_id)
                        business = await supabase_find_business(business_id)
                        if not business:
                            await send_message(chat_id, f"Business with ID {business_id} not found.")
                            await safe_clear_markup(chat_id, message_id)
                            return Response(status_code=200)
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
                    return Response(status_code=200)

                if callback_data and callback_data.startswith("giveaway_approve:"):
                    giveaway_id = callback_data[len("giveaway_approve:"):]
                    try:
                        uuid.UUID(giveaway_id)
                        giveaway = await supabase_find_giveaway(giveaway_id)
                        if not giveaway:
                            await send_message(chat_id, f"Giveaway with ID {giveaway_id} not found.")
                            await safe_clear_markup(chat_id, message_id)
                            return Response(status_code=200)
                        def _update_giveaway():
                            return supabase.table("giveaways").update({"active": True, "updated_at": now_iso()}).eq("id", giveaway_id).execute()
                        await asyncio.to_thread(_update_giveaway)
                        await send_message(chat_id, f"Approved {giveaway.get('business_type', 'giveaway')}: {giveaway['name']}.")
                        business = await supabase_find_business(giveaway["business_id"])
                        await send_message(business["telegram_id"], f"Your {giveaway.get('business_type', 'giveaway')} '{giveaway['name']}' is approved and live!")
                        await notify_users(giveaway_id)
                        await safe_clear_markup(chat_id, message_id)
                    except ValueError:
                        await send_message(chat_id, f"Invalid giveaway ID: {giveaway_id}")
                    except Exception as e:
                        logger.error(f"Failed to approve giveaway {giveaway_id}: {str(e)}", exc_info=True)
                        await send_message(chat_id, "Failed to approve giveaway. Please try again.")
                    return Response(status_code=200)

                if callback_data and callback_data.startswith("giveaway_reject:"):
                    giveaway_id = callback_data[len("giveaway_reject:"):]
                    try:
                        uuid.UUID(giveaway_id)
                        giveaway = await supabase_find_giveaway(giveaway_id)
                        if not giveaway:
                            await send_message(chat_id, f"Giveaway with ID {giveaway_id} not found.")
                            await safe_clear_markup(chat_id, message_id)
                            return Response(status_code=200)
                        def _update_giveaway():
                            return supabase.table("giveaways").update({"active": False, "updated_at": now_iso()}).eq("id", giveaway_id).execute()
                        await asyncio.to_thread(_update_giveaway)
                        await send_message(chat_id, f"Rejected {giveaway.get('business_type', 'giveaway')}: {giveaway['name']}.")
                        business = await supabase_find_business(giveaway["business_id"])
                        await send_message(business["telegram_id"], f"Your {giveaway.get('business_type', 'giveaway')} '{giveaway['name']}' was rejected. Contact support.")
                        await safe_clear_markup(chat_id, message_id)
                    except ValueError:
                        await send_message(chat_id, f"Invalid giveaway ID: {giveaway_id}")
                    except Exception as e:
                        logger.error(f"Failed to reject giveaway {giveaway_id}: {str(e)}", exc_info=True)
                        await send_message(chat_id, "Failed to reject giveaway. Please try again.")
                    return Response(status_code=200)

        if message:
            await handle_message(chat_id, message)
        if callback_query:
            await handle_callback(chat_id, callback_query)
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

@app.post("/verify_booking")
async def verify_booking(request: Request):
    if VERIFY_KEY:
        provided = request.headers.get("x-verify-key")
        if provided != VERIFY_KEY:
            return PlainTextResponse("Forbidden", status_code=403)
    try:
        body = await request.json()
        promo_code = body.get("promo_code")
        business_id = body.get("business_id")
        if not promo_code or not business_id:
            return PlainTextResponse("promo_code and business_id required", status_code=400)
        def _q_giveaway():
            return supabase.table("user_giveaways").select("*").eq("promo_code", promo_code).eq("business_id", business_id).limit(1).execute()
        resp = await asyncio.to_thread(_q_giveaway)
        promo_row = resp.data[0] if resp.data else None
        if not promo_row:
            return PlainTextResponse("Invalid promo code or business ID", status_code=400)
        if promo_row["entry_status"] != "awaiting_booking":
            return PlainTextResponse("Promo code not eligible for verification", status_code=400)
        giveaway_id = promo_row["giveaway_id"]
        giveaway = await supabase_find_giveaway(giveaway_id)
        if not giveaway:
            return PlainTextResponse("Giveaway not found", status_code=400)
        def _update_giveaway():
            return supabase.table("user_giveaways").update({
                "entry_status": "winner",
                "updated_at": now_iso()
            }).eq("id", promo_row["id"]).execute()
        await asyncio.to_thread(_update_giveaway)
        chat_id = promo_row["telegram_id"]
        business = await supabase_find_business(business_id)
        await send_message(
            chat_id,
            f"Congratulations! Your booking for {giveaway['name']} at {business.get('name', 'Unknown')} has been verified. You're a winner!"
        )
        user = await supabase_find_registered(chat_id)
        if user:
            reason = f"booking_verified:{giveaway_id}"
            if not await has_history(user["id"], reason):
                await award_points(user["id"], POINTS_REFERRAL_VERIFIED, reason)
        return PlainTextResponse("Booking verified", status_code=200)
    except json.JSONDecodeError:
        return PlainTextResponse("Invalid JSON", status_code=400)
    except Exception as e:
        logger.error(f"Failed to verify booking: {str(e)}", exc_info=True)
        return PlainTextResponse("Internal server error", status_code=500)

