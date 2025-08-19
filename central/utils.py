import os
import asyncio
import json
import uuid
import random
import logging
import httpx
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from starlette.responses import PlainTextResponse, Response
from central.db_utils import (
    supabase_find_registered,
    supabase_find_giveaway,
    supabase_insert_return,
    supabase_update_by_id_return,
    supabase_find_business,
    supabase_find_discount,
    supabase_find_discounts_by_category,
    supabase_find_business_categories,
    supabase_find_discount_by_id,
    get_state,
    set_state,
    award_points,
    has_history,
    USER_STATES,
    POINTS_CLAIM_PROMO,
    logger
)

# Import handler functions
try:
    from handlers.central_handler import (
        handle_start,
        handle_menu,
        handle_language_selection,
        handle_gender_selection,
        handle_interests_selection,
    )
    from handlers.points_handler import handle_points
    from handlers.profile_handler import handle_profile, handle_phone_contact, handle_dob_input
    from handlers.discount_handler import handle_discounts, handle_discount_callback
    from handlers.giveaway_handler import handle_giveaways, handle_giveaway_callback
    from handlers.admin_handler import handle_admin_command, handle_admin_callback
except ImportError as e:
    logger.error(f"Failed to import handler modules: {e}")
    raise

# Logging setup
logging.getLogger("httpx").setLevel(logging.DEBUG)
logging.getLogger("httpcore").setLevel(logging.DEBUG)

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("CENTRAL_BOT_TOKEN")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
VERIFY_KEY = os.getenv("VERIFY_KEY")

if not all([BOT_TOKEN, ADMIN_CHAT_ID, WEBHOOK_URL]):
    raise RuntimeError("Required env vars missing: BOT_TOKEN, ADMIN_CHAT_ID, WEBHOOK_URL")

# Constants
INTERESTS = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining", "Discounts only", "Giveaways only"]
CATEGORIES = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining"]
EMOJIS = ["1️⃣", "2️⃣", "3️⃣"]

# FastAPI app
app = FastAPI()

# Webhook setup
_WEBHOOK_SET: bool = False
_WEBHOOK_LOCK = asyncio.Lock()

async def get_referral_link(referral_code: str) -> str:
    return f"https://t.me/giveawaycentralhub?start={referral_code}"

async def send_message(chat_id: int, text: str, reply_markup: Optional[dict] = None, retries: int = 3):
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        for attempt in range(retries):
            try:
                logger.debug(f"Sending message to {chat_id} (attempt {attempt + 1}): {text}")
                response = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Sent message to {chat_id}: {text}")
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to send: HTTP {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 429:
                    retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    await asyncio.sleep(retry_after)
                    continue
                return {"ok": False, "error": f"HTTP {e.response.status_code}"}
            except Exception as e:
                logger.error(f"Failed to send: {str(e)}", exc_info=True)
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        logger.error(f"Failed after {retries} attempts to {chat_id}")
        return {"ok": False, "error": "Max retries reached"}

async def clear_inline_keyboard(chat_id: int, message_id: int, retries: int = 3):
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        for attempt in range(retries):
            try:
                logger.debug(f"Clearing keyboard for {chat_id}, {message_id}")
                response = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageReplyMarkup",
                    json={"chat_id": chat_id, "message_id": message_id, "reply_markup": {}}
                )
                response.raise_for_status()
                logger.info(f"Cleared keyboard for {chat_id}, {message_id}")
                return
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to clear: HTTP {e.response.status_code}")
                if e.response.status_code == 429:
                    retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    await asyncio.sleep(retry_after)
                    continue
                break
            except Exception as e:
                logger.error(f"Failed to clear: {str(e)}")
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        logger.error(f"Failed to clear for {chat_id} after {retries} attempts")

async def safe_clear_markup(chat_id: int, message_id: int):
    try:
        await clear_inline_keyboard(chat_id, message_id)
    except Exception:
        logger.debug(f"Ignored error clearing keyboard for {chat_id}")

async def edit_message_keyboard(chat_id: int, message_id: int, reply_markup: dict, retries: int = 3):
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        payload = {"chat_id": chat_id, "message_id": message_id, "reply_markup": reply_markup}
        for attempt in range(retries):
            try:
                logger.debug(f"Editing keyboard for {chat_id}, {message_id}")
                response = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageReplyMarkup",
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Edited keyboard for {chat_id}, {message_id}")
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to edit: HTTP {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 429:
                    retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    await asyncio.sleep(retry_after)
                    continue
                return {"ok": False, "error": f"HTTP {e.response.status_code}"}
            except Exception as e:
                logger.error(f"Failed to edit: {str(e)}", exc_info=True)
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        logger.error(f"Failed to edit for {chat_id} after {retries} attempts")
        return {"ok": False, "error": "Max retries reached"}

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

async def generate_discount_code(chat_id: int, business_id: str, discount_id: str) -> tuple[str, str]:
    if not business_id or not discount_id:
        raise ValueError("Business or discount ID missing")
    def _check_existing_code():
        return supabase.table("user_discounts").select("promo_code").eq("promo_code", code).eq("business_id", business_id).execute()
    def _check_claimed():
        return supabase.table("user_discounts").select("id").eq("telegram_id", chat_id).eq("discount_id", discount_id).execute()
    claimed = await asyncio.to_thread(_check_claimed)
    if claimed.data:
        raise ValueError("Already claimed")
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
        "joined_at": (datetime.now(timezone.utc)).isoformat()
    }
    inserted = await supabase_insert_return("user_discounts", payload)
    if not inserted:
        raise RuntimeError("Failed to save promo")

    try:
        user_row = await supabase_find_registered(chat_id)
        if user_row:
            if not await has_history(user_row["id"], "claim_promo"):
                await award_points(user_row["id"], POINTS_CLAIM_PROMO, "claim_promo")
    except Exception:
        logger.exception("Failed claim promo points")

    return code, expiry

async def generate_promo_code(chat_id: int, business_id: str, giveaway_id: str, discount_type: str = "standard") -> tuple[str, str]:
    if not business_id or not giveaway_id:
        raise ValueError("Business or giveaway ID missing")
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
        raise RuntimeError("Failed to save promo")
    return code, expiry

async def has_redeemed_discount(chat_id: int) -> bool:
    def _q():
        current_month = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return supabase.table("user_discounts").select("id").eq("telegram_id", chat_id).eq("entry_status", "standard").gte("joined_at", current_month.isoformat()).execute()
    try:
        resp = await asyncio.to_thread(_q)
        return bool(resp.data if hasattr(resp, "data") else resp.get("data"))
    except Exception as e:
        logger.error(f"has_redeemed_discount failed: {str(e)}", exc_info=True)
        return False

async def notify_users(giveaway_id: str):
    try:
        giveaway = await supabase_find_giveaway(giveaway_id)
        if not giveaway:
            return
        users = supabase.table("central_bot_leads").select("telegram_id").eq("is_draft", False).contains("interests", [giveaway["category"]]).execute().data
        for user in users:
            await send_message(
                user["telegram_id"],
                f"New {giveaway['category']} offer: *{giveaway['name']}* at {giveaway['salon_name']}. Check it out:",
                create_main_menu_keyboard()
            )
    except Exception as e:
        logger.error(f"notify_users failed: {str(e)}", exc_info=True)

async def set_menu_button():
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        try:
            await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/setChatMenuButton",
                json={"menu_button": {"type": "commands"}}
            )
            await client.post(
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
            logger.info("Set menu button and commands")
        except Exception as e:
            logger.error(f"Failed to set menu/commands: {str(e)}", exc_info=True)

async def initialize_bot_once():
    global _WEBHOOK_SET
    if _WEBHOOK_SET:
        return
    async with _WEBHOOK_LOCK:
        if _WEBHOOK_SET:
            return
        if not BOT_TOKEN or not WEBHOOK_URL:
            logger.error("BOT_TOKEN or WEBHOOK_URL missing; cannot set webhook.")
            return
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
                resp = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
                    json={"url": WEBHOOK_URL, "allowed_updates": ["message", "callback_query"]},
                )
                resp.raise_for_status()
                logger.info("Webhook set successfully to %s", WEBHOOK_URL)
                _WEBHOOK_SET = True
        except Exception:
            logger.exception("Failed to set webhook")

@app.post("/hook/central_bot")
async def webhook_handler(request: Request) -> Response:
    try:
        update = await request.json()
    except json.JSONDecodeError:
        logger.error("Invalid JSON received in webhook", exc_info=True)
        return Response(status_code=200)

    asyncio.create_task(initialize_bot_once())

    if not update:
        return Response(status_code=200)

    callback_query = update.get("callback_query")
    message = update.get("message")

    try:
        if callback_query:
            await handle_callback_query(callback_query)
        if message:
            await handle_message_update(message)
    except Exception:
        logger.exception("Error handling update")
    return Response(status_code=200)

async def handle_message_update(message: Dict[str, Any]) -> Dict[str, Any]:
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    if not chat_id:
        logger.error("Message update without chat id: %s", message)
        return {"ok": True}

    text = (message.get("text") or "").strip()
    contact = message.get("contact")
    state = get_state(chat_id)

    if isinstance(text, str) and text.lower().startswith("/myid"):
        await send_message(chat_id, f"Your Telegram ID: {chat_id}")
        return {"ok": True}

    if ADMIN_CHAT_ID and str(chat_id) == str(ADMIN_CHAT_ID) and isinstance(text, str):
        if text.startswith("/approve_") or text.startswith("/reject_"):
            try:
                return await handle_admin_command(text, chat_id)
            except Exception:
                logger.exception("handle_admin_command failed")
                return {"ok": True}

    if isinstance(text, str) and text.lower() == "/menu":
        await send_message(chat_id, "Choose an option:", reply_markup=create_menu_options_keyboard())
        return {"ok": True}

    if contact and state.get("stage") == "awaiting_phone_profile":
        try:
            return await handle_phone_contact(contact, state, chat_id)
        except Exception:
            logger.exception("handle_phone_contact failed")
            return {"ok": True}

    if state.get("stage") in ["awaiting_dob", "awaiting_dob_profile"]:
        try:
            return await handle_dob_input(text, state, chat_id)
        except Exception:
            logger.exception("handle_dob_input failed")
            return {"ok": True}

    if isinstance(text, str) and text.lower().startswith("/start"):
        try:
            return await handle_start(message, state, chat_id)
        except Exception:
            logger.exception("handle_start failed")
            return {"ok": True}

    return {"ok": True}

async def handle_callback_query(callback_query: Dict[str, Any]) -> Dict[str, Any]:
    user = callback_query.get("from") or {}
    chat_id = user.get("id")
    callback_data = callback_query.get("data")
    message = callback_query.get("message") or {}
    message_id = message.get("message_id")

    if not chat_id or not callback_data or message_id is None:
        logger.error(
            "Invalid callback_query: chat_id=%s callback_data=%s message_id=%s", chat_id, callback_data, message_id
        )
        return {"ok": True}

    try:
        registered = await supabase_find_registered(chat_id)
    except Exception:
        logger.exception("Failed to fetch registered user")
        registered = None

    state = get_state(chat_id)

    if ADMIN_CHAT_ID and str(chat_id) == str(ADMIN_CHAT_ID):
        if callback_data.startswith(("approve:", "reject:", "giveaway_approve:", "giveaway_reject:")):
            try:
                return await handle_admin_callback(callback_query, message_id)
            except Exception:
                logger.exception("handle_admin_callback failed")
                return {"ok": True}

    if callback_data in ["menu:main", "menu:language"]:
        try:
            return await handle_menu(callback_data, chat_id, message_id, state)
        except Exception:
            logger.exception("handle_menu failed")
            return {"ok": True}

    if state.get("stage") in ["awaiting_language", "awaiting_language_change"] and callback_data.startswith("lang:"):
        try:
            return await handle_language_selection(callback_data, state, chat_id, message_id)
        except Exception:
            logger.exception("handle_language_selection failed")
            return {"ok": True}

    if state.get("stage") == "awaiting_gender" and callback_data.startswith("gender:"):
        try:
            return await handle_gender_selection(callback_data, state, chat_id, message_id)
        except Exception:
            logger.exception("handle_gender_selection failed")
            return {"ok": True}

    if state.get("stage") == "awaiting_interests" and (
        callback_data.startswith("interest:") or callback_data == "interests_done"
    ):
        try:
            return await handle_interests_selection(callback_data, state, chat_id, message_id, registered)
        except Exception:
            logger.exception("handle_interests_selection failed")
            return {"ok": True}

    if registered:
        try:
            if callback_data == "menu:points":
                return await handle_points(callback_query, registered)
            if callback_data == "menu:profile":
                return await handle_profile(callback_query, registered, state, chat_id)
            if callback_data == "menu:discounts":
                return await handle_discounts(callback_query, registered, chat_id)
            if callback_data.startswith(("discount_category:", "profile:", "services:", "book:", "get_discount:")):
                return await handle_discount_callback(callback_data, chat_id, registered)
            if callback_data == "menu:giveaways":
                return await handle_giveaways(callback_query, registered, chat_id)
            if callback_data.startswith(("giveaway_points:", "giveaway_book:")):
                return await handle_giveaway_callback(callback_data, chat_id, registered)
        except Exception:
            logger.exception("Registered-user callback handling failed")
            return {"ok": True}

    return {"ok": True}

@app.post("/verify_booking")
async def verify_booking(request: Request):
    if VERIFY_KEY:
        provided = request.headers.get("x-verify-key")
        if provided != VERIFY_KEY:
            return PlainTextResponse("Forbidden", status_code=403)

    try:
        body = await request.json()
    except json.JSONDecodeError:
        return PlainTextResponse("Invalid JSON", status_code=400)

    promo_code = body.get("promo_code")
    business_id = body.get("business_id")
    if not promo_code or not business_id:
        return PlainTextResponse("promo_code and business_id required", status_code=400)

    try:
        def _q_giveaway():
            return supabase.table("user_giveaways").select("*").eq("promo_code", promo_code).eq("business_id", business_id).limit(1).execute()
        resp = await asyncio.to_thread(_q_giveaway)
        ug = resp.data[0] if resp.data else None
    except Exception:
        logger.exception("verify_booking: supabase lookup user_giveaways failed")
        ug = None

    found_row = None
    table_name = None
    if ug:
        found_row = ug
        table_name = "user_giveaways"
    else:
        try:
            def _q_disc():
                return supabase.table("user_discounts").select("*").eq("promo_code", promo_code).eq("business_id", business_id).limit(1).execute()
            resp2 = await asyncio.to_thread(_q_disc)
            ud = resp2.data[0] if resp2.data else None
        except Exception:
            logger.exception("verify_booking: supabase lookup user_discounts failed")
            ud = None
        if ud:
            found_row = ud
            table_name = "user_discounts"

    if not found_row:
        return PlainTextResponse("Promo not found", status_code=404)

    telegram_id = found_row.get("telegram_id")
    if not telegram_id:
        return PlainTextResponse("No telegram_id", status_code=400)

    try:
        def _q_user():
            return supabase.table("central_bot_leads").select("*").eq("telegram_id", telegram_id).limit(1).execute()
        resp_user = await asyncio.to_thread(_q_user)
        users = resp_user.data if hasattr(resp_user, "data") else resp_user.get("data", []) or []
    except Exception:
        logger.exception("verify_booking: supabase lookup central_bot_leads failed")
        users = []

    if not users:
        return PlainTextResponse("User not found", status_code=404)

    user = users[0]
    user_id = user["id"]

    try:
        def _find_booking():
            return supabase.table("user_bookings").select("*").eq("user_id", user_id).eq("business_id", business_id).limit(1).execute()
        resp_b = await asyncio.to_thread(_find_booking)
        booking = resp_b.data[0] if resp_b.data else None
    except Exception:
        logger.exception("verify_booking: find booking failed")
        booking = None

    booking_id = None
    if booking:
        if booking.get("status") == "completed" or booking.get("points_awarded"):
            return {"ok": True, "message": "already_verified"}
        try:
            def _upd_booking():
                return supabase.table("user_bookings").update({"status": "completed", "points_awarded": True, "booking_date": (datetime.now(timezone.utc)).isoformat()}).eq("id", booking["id"]).execute()
            await asyncio.to_thread(_upd_booking)
            booking_id = booking["id"]
        except Exception:
            logger.exception("verify_booking: update booking failed")
    else:
        try:
            created = await supabase_insert_return("user_bookings", {
                "user_id": user_id,
                "business_id": business_id,
                "booking_date": (datetime.now(timezone.utc)).isoformat(),
                "status": "completed",
                "points_awarded": True
            })
            booking_id = created["id"] if created else None
        except Exception:
            logger.exception("verify_booking: create booking failed")
            booking_id = None

    reason = f"booking_verified:{promo_code}"
    try:
        if not await has_history(user_id, reason):
            await award_points(user_id, POINTS_BOOKING_VERIFIED, reason, booking_id)
    except Exception:
        logger.exception("verify_booking: award_points failed")

    try:
        if table_name == "user_giveaways":
            await supabase_update_by_id_return("user_giveaways", found_row["id"], {"entry_status": "redeemed", "redeemed_at": (datetime.now(timezone.utc)).isoformat()})
        else:
            await supabase_update_by_id_return("user_discounts", found_row["id"], {"entry_status": "redeemed", "redeemed_at": (datetime.now(timezone.utc)).isoformat()})
    except Exception:
        logger.exception("verify_booking: failed to update promo entry_status")

    return {"ok": True, "user_id": user_id, "booking_id": booking_id}

@app.get("/health")
async def health() -> PlainTextResponse:
    return PlainTextResponse("OK", status_code=200)

if __name__ == "__main__":
    import uvicorn
    try:
        asyncio.run(set_menu_button())
    except Exception:
        logger.exception("Failed to set menu/button during startup")
    uvicorn.run("utils:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
