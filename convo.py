# convo.py
import os
import asyncio
import logging
import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

from supabase import create_client, Client

from config import ADMIN_CHAT_ID, SUPABASE_URL, SUPABASE_KEY
from utils import (
    send_message,
    edit_message_text,
    edit_message_keyboard,
    safe_clear_markup,
    create_menu_options_keyboard,
    create_language_keyboard,
    create_gender_keyboard,
    create_interests_keyboard,
    create_categories_keyboard,
    create_main_menu_keyboard,
    create_phone_keyboard,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in .env")
    
# --- Constants -------------------------------------------------------------
INTERESTS = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining", "Discounts only", "Giveaways only"]
CATEGORIES = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining"]
EMOJIS = ["1️⃣", "2️⃣", "3️⃣"]

STARTER_POINTS = 100
POINTS_SIGNUP = 20
POINTS_PROFILE_COMPLETE = 40
POINTS_VIEW_DISCOUNT = 5
POINTS_CLAIM_PROMO = 10
POINTS_BOOKING_CREATED = 15
POINTS_BOOKING_VERIFIED = 200
POINTS_REFERRAL_JOIN = 10
POINTS_REFERRAL_VERIFIED = 100
DAILY_POINTS_CAP = 2000
STATE_TTL_SECONDS = 30 * 60

TIER_THRESHOLDS = [
    ("Bronze", 0),
    ("Silver", 200),
    ("Gold", 500),
    ("Platinum", 1000),
]

USER_STATES: Dict[int, Dict[str, Any]] = {}

def create_business_profile_keyboard(business_id: str):
    """Create keyboard with web app button for business profile"""
    web_app_url = f"https://flutter-web-app-3q0r.onrender.com/?business_id={business_id}&action=view_profile"
    
    return {
        "inline_keyboard": [
            [
                {"text": "View Profile", "web_app": {"url": web_app_url}},
                {"text": "View Services", "callback_data": f"services:{business_id}"}
            ],
            [
                {"text": "Book", "callback_data": f"book:{business_id}"},
                {"text": "Get Discount", "callback_data": f"get_discount:{business_id}"}
            ]
        ]
    }
# --- Utilities -------------------------------------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def compute_tier(points: int) -> str:
    tier = "Bronze"
    for name, threshold in TIER_THRESHOLDS:
        if points >= threshold:
            tier = name
    return tier

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

# --- Supabase helpers (all executed in threads because supabase client is sync) ----

async def supabase_find_registered(chat_id: int) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("central_bot_leads").select("*").eq("telegram_id", chat_id).eq("is_draft", False).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_registered failed")
        return None

async def supabase_find_draft(chat_id: int) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("central_bot_leads").select("*").eq("telegram_id", chat_id).eq("is_draft", True).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_draft failed")
        return None

async def supabase_insert_return(table: str, payload: dict) -> Optional[Dict[str, Any]]:
    def _ins():
        return supabase.table(table).insert(payload).execute()
    try:
        resp = await asyncio.to_thread(_ins)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.error("supabase_insert_return: no data")
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_insert_return failed")
        return None

async def supabase_update_by_id_return(table: str, entry_id: str, payload: dict) -> Optional[Dict[str, Any]]:
    def _upd():
        return supabase.table(table).update(payload).eq("id", entry_id).execute()
    try:
        resp = await asyncio.to_thread(_upd)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.error(f"supabase_update_by_id_return: no data for {table} id {entry_id}")
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_update_by_id_return failed")
        return None

async def supabase_find_business(business_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("businesses").select("*").eq("id", business_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_business failed")
        return None

async def supabase_find_discount(discount_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("discounts").select("*").eq("id", discount_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_discount failed")
        return None

async def supabase_find_giveaway(giveaway_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("giveaways").select("*").eq("id", giveaway_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_giveaway failed")
        return None

async def supabase_find_user_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("central_bot_leads").select("*").eq("id", user_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_user_by_id failed")
        return None

async def get_points_awarded_today(user_id: str) -> int:
    """Return sum of points awarded to user_id since UTC midnight."""
    def _q():
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        return supabase.table("points_history").select("points").eq("user_id", user_id).gte("awarded_at", today_start).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = resp.data if hasattr(resp, "data") else resp.get("data", []) or []
        return sum(int(r["points"]) for r in rows)
    except Exception:
        logger.exception("get_points_awarded_today failed")
        return 0

# --- Points / promos ------------------------------------------------------

async def has_history(user_id: str, reason: str) -> bool:
    def _q():
        return supabase.table("points_history").select("id").eq("user_id", user_id).eq("reason", reason).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = resp.data if hasattr(resp, "data") else resp.get("data", [])
        return bool(rows)
    except Exception:
        logger.exception("has_history failed")
        return False

async def award_points(user_id: str, delta: int, reason: str, booking_id: Optional[str] = None) -> dict:
    if delta == 0:
        return {"ok": True}
    
    # daily cap check
    awarded_today = await get_points_awarded_today(user_id)
    if awarded_today + abs(delta) > DAILY_POINTS_CAP:
        logger.warning(f"Daily cap reached for user {user_id}: today {awarded_today}, trying to add {delta}")
        return {"ok": False, "error": "daily_cap_reached"}

    try:
        user = await supabase_find_user_by_id(user_id)
        if not user:
            return {"ok": False, "error": "user_not_found"}
        
        old_points = int(user.get("points") or 0)
        new_points = max(0, old_points + delta)
        new_tier = compute_tier(new_points)
        
        def _upd_user():
            return supabase.table("central_bot_leads").update({
                "points": new_points, 
                "tier": new_tier,
                "last_login": now_iso()
            }).eq("id", user_id).execute()
        
        await asyncio.to_thread(_upd_user)
        
        hist = {"user_id": user_id, "points": delta, "reason": reason, "awarded_at": now_iso()}
        await supabase_insert_return("points_history", hist)
        
        logger.info(f"Awarded {delta} pts to user {user_id} ({old_points} -> {new_points}) for {reason}")
        
        # referral bonus for booking_verified
        if reason == "booking_verified":
            referred_by = user.get("referred_by")
            if referred_by:
                try:
                    await award_points(referred_by, POINTS_REFERRAL_VERIFIED, "referral_booking_verified", booking_id)
                except Exception:
                    logger.exception("Failed to award referral bonus")
        
        return {"ok": True, "old_points": old_points, "new_points": new_points, "tier": new_tier}
    except Exception:
        logger.exception("award_points failed")
        return {"ok": False, "error": "award_failed"}

async def generate_discount_code(chat_id: int, business_id: str, discount_id: str) -> (str, str):
    if not business_id or not discount_id:
        raise ValueError("Business ID or discount ID missing")

    # Check if user has already claimed this discount
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
        raise RuntimeError("Failed to save promo code")
    
    # award points for claiming promo
    try:
        user_row = await supabase_find_registered(chat_id)
        if user_row and not await has_history(user_row["id"], "claim_promo"):
            await award_points(user_row["id"], POINTS_CLAIM_PROMO, "claim_promo")
    except Exception:
        logger.exception("Failed to award claim promo points")
    
    logger.info("Generated discount code %s for chat %s", code, chat_id)
    return code, expiry

async def generate_promo_code(chat_id: int, business_id: str, giveaway_id: str, entry_status: str) -> (str, str):
    if not business_id or not giveaway_id:
        raise ValueError("Business ID or giveaway ID missing")

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
        raise RuntimeError("Failed to save giveaway promo code")
    logger.info("Generated giveaway code %s for chat %s", code, chat_id)
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
        logger.error(f"has_redeemed_discount failed for chat_id {chat_id}: {str(e)}")
        return False

# --- Notifications --------------------------------------------------------

async def notify_users(giveaway_id: str):
    try:
        giveaway = await supabase_find_giveaway(giveaway_id)
        if not giveaway:
            logger.error("notify_users: giveaway not found %s", giveaway_id)
            return
        
        def _q_users():
            return supabase.table("central_bot_leads").select("telegram_id").contains("interests", [giveaway["category"]]).execute()
        
        resp = await asyncio.to_thread(_q_users)
        users = resp.data if hasattr(resp, "data") else resp.get("data", [])
        
        for user in users:
            await send_message(
                user["telegram_id"], 
                f"New {giveaway['category']} offer: *{giveaway['name']}* at {giveaway.get('salon_name', 'Unknown')}. Check it out:"
            )
        
        logger.info("Notified %d users for giveaway %s", len(users), giveaway_id)
    except Exception:
        logger.exception("notify_users failed")

# --- Bot init ---------------------------------------------------

async def initialize_bot(webhook_url: str, token: str):
    # Set menu button and commands
    await set_menu_button(token)
    
    # Set webhook
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        try:
            response = await client.post(
                f"https://api.telegram.org/bot{token}/setWebhook",
                json={"url": webhook_url, "allowed_updates": ["message", "callback_query"]}
            )
            response.raise_for_status()
            logger.info(f"Webhook set to {webhook_url}")
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to set webhook: HTTP {e.response.status_code} - {e.response.text}")
        except Exception as e:
            logger.error(f"Failed to set webhook: {str(e)}")

# --- Conversation handlers -----------------------------------------------

async def handle_message(chat_id: int, message: Dict[str, Any], token: str):
    text = (message.get("text") or "").strip()
    contact = message.get("contact")
    state = get_state(chat_id) or {}

    # Handle /myid
    if text.lower() == "/myid":
        await send_message(chat_id, f"Your Telegram ID: {chat_id}", token=token)
        return

    # Handle /menu
    if text.lower() == "/menu":
        await send_message(chat_id, "Choose an option:", reply_markup=create_menu_options_keyboard(), token=token)
        return

    # Handle phone number
    if contact and state.get("stage") == "awaiting_phone_profile":
        phone_number = contact.get("phone_number")
        if not phone_number:
            await send_message(chat_id, "Invalid phone number. Please try again:", reply_markup=create_phone_keyboard(), token=token)
            return
        
        state["data"]["phone_number"] = phone_number
        entry_id = state.get("entry_id")
        if entry_id:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"phone_number": phone_number})
        
        registered = await supabase_find_registered(chat_id)
        # If user now has both phone and dob -> award profile-complete points (idempotent)
        try:
            if registered:
                # fetch fresh row
                user_row = await supabase_find_registered(chat_id)
                if user_row and user_row.get("dob") and user_row.get("phone_number"):
                    user_id = user_row["id"]
                    if not await has_history(user_id, "profile_complete"):
                        await award_points(user_id, POINTS_PROFILE_COMPLETE, "profile_complete")
        except Exception:
            logger.exception("Failed during profile completion points flow")

        if registered and not registered.get("dob"):
            await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:", token=token)
            state["stage"] = "awaiting_dob_profile"
        else:
            interests = registered.get("interests", []) or []
            interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
            await send_message(
                chat_id, 
                f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}",
                token=token
            )
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
        
        set_state(chat_id, state)
        return

    # Handle DOB (initial registration)
    if state.get("stage") == "awaiting_dob":
        if text.lower() == "/skip":
            state["data"]["dob"] = None
            entry_id = state.get("entry_id")
            if entry_id:
                await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": None})
            state["stage"] = "awaiting_interests"
            await send_message(chat_id, "Choose exactly 3 interests for this month:", reply_markup=create_interests_keyboard(), token=token)
            set_state(chat_id, state)
            return
        
        try:
            dob_obj = datetime.strptime(text, "%Y-%m-%d").date()
            if dob_obj.year < 1900 or dob_obj > datetime.now().date():
                await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.", token=token)
                return
            
            state["data"]["dob"] = dob_obj.isoformat()
            entry_id = state.get("entry_id")
            if entry_id:
                await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": state["data"]["dob"]})
            
            state["stage"] = "awaiting_interests"
            await send_message(chat_id, "Choose exactly 3 interests for this month:", reply_markup=create_interests_keyboard(), token=token)
            set_state(chat_id, state)
        except ValueError:
            await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.", token=token)
        return

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
            
            await send_message(
                chat_id, 
                f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}",
                token=token
            )
            
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
            return
        
        try:
            dob_obj = datetime.strptime(text, "%Y-%m-%d").date()
            if dob_obj.year < 1900 or dob_obj > datetime.now().date():
                await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.", token=token)
                return
            
            state["data"]["dob"] = dob_obj.isoformat()
            entry_id = state.get("entry_id")
            if entry_id:
                await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": state["data"]["dob"]})
            
            registered = await supabase_find_registered(chat_id)
            # If user now has phone and dob -> award profile complete
            try:
                if registered and registered.get("phone_number") and registered.get("dob"):
                    if not await has_history(registered["id"], "profile_complete"):
                        await award_points(registered["id"], POINTS_PROFILE_COMPLETE, "profile_complete")
            except Exception:
                logger.exception("Failed awarding profile_complete after dob update")

            interests = registered.get("interests", []) or []
            interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
            
            await send_message(
                chat_id, 
                f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}",
                token=token
            )
            
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
            return
        except ValueError:
            await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.", token=token)
        return

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
            await send_message(chat_id, "You're already registered! Explore options:", reply_markup=create_main_menu_keyboard(), token=token)
            return
        
        existing = await supabase_find_draft(chat_id)
        if existing:
            state = {
                "stage": "awaiting_gender",
                "data": {"language": existing.get("language")},
                "entry_id": existing.get("id"),
                "selected_interests": []
            }
            await send_message(chat_id, "What's your gender? (optional, helps target offers)", reply_markup=create_gender_keyboard(), token=token)
        else:
            state = {"stage": "awaiting_language", "data": {}, "entry_id": None, "selected_interests": []}
            await send_message(chat_id, "Welcome! Choose your language:", reply_markup=create_language_keyboard(), token=token)
        
        set_state(chat_id, state)
        return

    # Default response for unhandled messages
    registered = await supabase_find_registered(chat_id)
    if registered:
        await send_message(chat_id, "Please use /menu to interact or provide a valid command.", token=token)
    else:
        await send_message(chat_id, "Please start registration with /start.", token=token)

async def handle_callback(chat_id: int, callback_query: Dict[str, Any], token: str):
    data = callback_query.get("data")
    message_id = callback_query.get("message", {}).get("message_id")
    
    if not data or not message_id:
        await safe_clear_markup(chat_id, message_id, token=token)
        return

    registered = await supabase_find_registered(chat_id)
    state = get_state(chat_id) or {}

    # Handle admin approval/rejection callbacks
    if ADMIN_CHAT_ID is None:
        logger.warning("ADMIN_CHAT_ID is not set; admin functionality disabled")
    if chat_id and ADMIN_CHAT_ID is not None and int(chat_id) == int(ADMIN_CHAT_ID):
        if data.startswith("approve:"):
            business_id = data[len("approve:"):]
            try:
                uuid.UUID(business_id)
                business = await supabase_find_business(business_id)
                if not business:
                    await send_message(chat_id, f"Business with ID {business_id} not found.", token=token)
                    await safe_clear_markup(chat_id, message_id, token=token)
                    return
                
                await supabase_update_by_id_return("businesses", business_id, {"status": "approved", "updated_at": now_iso()})
                await send_message(chat_id, f"Business {business['name']} approved.", token=token)
                await send_message(business["telegram_id"], "Your business has been approved! You can now add discounts and giveaways.", token=token)
                await safe_clear_markup(chat_id, message_id, token=token)
            except ValueError:
                await send_message(chat_id, f"Invalid business ID format: {business_id}", token=token)
            except Exception as e:
                logger.error(f"Failed to approve business {business_id}: {str(e)}")
                await send_message(chat_id, "Failed to approve business. Please try again.", token=token)
            return

        if data.startswith("reject:"):
            business_id = data[len("reject:"):]
            try:
                uuid.UUID(business_id)
                business = await supabase_find_business(business_id)
                if not business:
                    await send_message(chat_id, f"Business with ID {business_id} not found.", token=token)
                    await safe_clear_markup(chat_id, message_id, token=token)
                    return
                
                await supabase_update_by_id_return("businesses", business_id, {"status": "rejected", "updated_at": now_iso()})
                await send_message(chat_id, f"Business {business['name']} rejected.", token=token)
                await send_message(business["telegram_id"], "Your business registration was rejected. Please contact support.", token=token)
                await safe_clear_markup(chat_id, message_id, token=token)
            except ValueError:
                await send_message(chat_id, f"Invalid business ID format: {business_id}", token=token)
            except Exception as e:
                logger.error(f"Failed to reject business {business_id}: {str(e)}")
                await send_message(chat_id, "Failed to reject business. Please try again.", token=token)
            return

        # Handle giveaway approval/rejection
        if data.startswith("giveaway_approve:"):
            giveaway_id = data[len("giveaway_approve:"):]
            try:
                uuid.UUID(giveaway_id)
                giveaway = await supabase_find_giveaway(giveaway_id)
                if not giveaway:
                    await send_message(chat_id, f"Giveaway with ID {giveaway_id} not found.", token=token)
                    await safe_clear_markup(chat_id, message_id, token=token)
                    return
                
                await supabase_update_by_id_return("giveaways", giveaway_id, {"active": True, "updated_at": now_iso()})
                await send_message(chat_id, f"Approved {giveaway['business_type']}: {giveaway['name']}.", token=token)
                
                business = await supabase_find_business(giveaway["business_id"])
                await send_message(business["telegram_id"], f"Your {giveaway['business_type']} '{giveaway['name']}' is approved and live!", token=token)
                
                await notify_users(giveaway_id)
                await safe_clear_markup(chat_id, message_id, token=token)
            except ValueError:
                await send_message(chat_id, f"Invalid giveaway ID: {giveaway_id}", token=token)
            except Exception as e:
                logger.error(f"Failed to approve giveaway {giveaway_id}: {str(e)}")
                await send_message(chat_id, "Failed to approve giveaway. Please try again.", token=token)
            return

        if data.startswith("giveaway_reject:"):
            giveaway_id = data[len("giveaway_reject:"):]
            try:
                uuid.UUID(giveaway_id)
                giveaway = await supabase_find_giveaway(giveaway_id)
                if not giveaway:
                    await send_message(chat_id, f"Giveaway with ID {giveaway_id} not found.", token=token)
                    await safe_clear_markup(chat_id, message_id, token=token)
                    return
                
                await supabase_update_by_id_return("giveaways", giveaway_id, {"active": False, "updated_at": now_iso()})
                await send_message(chat_id, f"Rejected {giveaway['business_type']}: {giveaway['name']}.", token=token)
                
                business = await supabase_find_business(giveaway["business_id"])
                await send_message(business["telegram_id"], f"Your {giveaway['business_type']} '{giveaway['name']}' was rejected. Contact support.", token=token)
                
                await safe_clear_markup(chat_id, message_id, token=token)
            except ValueError:
                await send_message(chat_id, f"Invalid giveaway ID: {giveaway_id}", token=token)
            except Exception as e:
                logger.error(f"Failed to reject giveaway {giveaway_id}: {str(e)}")
                await send_message(chat_id, "Failed to reject giveaway. Please try again.", token=token)
            return

    # Menu options
    if data == "menu:main":
        await safe_clear_markup(chat_id, message_id, token=token)
        await send_message(chat_id, "Explore options:", reply_markup=create_main_menu_keyboard(), token=token)
        return
    
    elif data == "menu:language":
        await safe_clear_markup(chat_id, message_id, token=token)
        await send_message(chat_id, "Choose your language:", reply_markup=create_language_keyboard(), token=token)
        state["stage"] = "awaiting_language_change"
        set_state(chat_id, state)
        return

    # Language selection
    if state.get("stage") in ["awaiting_language", "awaiting_language_change"] and data.startswith("lang:"):
        language = data[len("lang:"):]
        if language not in ["en", "ru"]:
            await send_message(chat_id, "Invalid language:", reply_markup=create_language_keyboard(), token=token)
            return
        
        state["data"]["language"] = language
        entry_id = state.get("entry_id")
        
        if not entry_id:
            created = await supabase_insert_return("central_bot_leads", {"telegram_id": chat_id, "language": language, "is_draft": True})
            state["entry_id"] = created.get("id") if created else None
        else:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"language": language})
        
        await safe_clear_markup(chat_id, message_id, token=token)
        
        if state.get("stage") == "awaiting_language":
            await send_message(chat_id, "What's your gender? (optional, helps target offers)", reply_markup=create_gender_keyboard(), token=token)
            state["stage"] = "awaiting_gender"
        else:
            await send_message(chat_id, "Language updated! Explore options:", reply_markup=create_main_menu_keyboard(), token=token)
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
        
        set_state(chat_id, state)
        return

    # Gender selection
    if state.get("stage") == "awaiting_gender" and data.startswith("gender:"):
        gender = data[len("gender:"):]
        if gender not in ["female", "male"]:
            await send_message(chat_id, "Invalid gender:", reply_markup=create_gender_keyboard(), token=token)
            return
        
        state["data"]["gender"] = gender
        entry_id = state.get("entry_id")
        if entry_id:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"gender": gender})
        
        await safe_clear_markup(chat_id, message_id, token=token)
        await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:", token=token)
        state["stage"] = "awaiting_dob"
        set_state(chat_id, state)
        return

    # Interests selection
    if state.get("stage") == "awaiting_interests":
        if data.startswith("interest:"):
            interest = data[len("interest:"):]
            if interest not in INTERESTS:
                logger.warning(f"Invalid interest selected: {interest}")
                return
            
            selected = state.get("selected_interests", [])
            if interest in selected:
                selected.remove(interest)
            elif len(selected) < 3:
                selected.append(interest)
            
            state["selected_interests"] = selected
            await edit_message_keyboard(chat_id, message_id, create_interests_keyboard(selected), token=token)
            set_state(chat_id, state)
            return
        
        elif data == "interests_done":
            selected = state.get("selected_interests", [])
            if len(selected) != 3:
                await send_message(chat_id, f"Please select exactly 3 interests (currently {len(selected)}):", reply_markup=create_interests_keyboard(selected), token=token)
                return
            
            await send_message(chat_id, "Interests saved! Finalizing registration...", token=token)
            entry_id = state.get("entry_id")
            
            if entry_id:
                # Remove direct points update; use award_points to handle tier & history
                await supabase_update_by_id_return("central_bot_leads", entry_id, {
                    "interests": selected,
                    "is_draft": False
                })
                
                try:
                    # award starter points (idempotent via history check)
                    if not await has_history(entry_id, "signup"):
                        await award_points(entry_id, STARTER_POINTS, "signup")
                    
                    # if referred_by present in state (and looks like uuid), set it and award referrer join points
                    referred = state.get("referred_by")
                    if referred:
                        try:
                            # ensure it's a valid UUID of another user
                            ref_uuid = str(uuid.UUID(referred))
                            # set referred_by column on the new user (best effort)
                            await supabase_update_by_id_return("central_bot_leads", entry_id, {"referred_by": ref_uuid})
                            # award referral join points to referrer (idempotent)
                            if not await has_history(ref_uuid, "referral_join"):
                                await award_points(ref_uuid, POINTS_REFERRAL_JOIN, "referral_join")
                        except Exception:
                            logger.debug("referred_by value is not a user UUID or awarding failed; skipping referral join")
                except Exception:
                    logger.exception("Failed awarding signup or referral points")
            
            await send_message(chat_id, f"Congrats! You've earned {STARTER_POINTS} points. Explore options:", reply_markup=create_main_menu_keyboard(), token=token)
            
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
            return

    # Registered user actions
    if registered:
        if data == "menu:points":
            points = registered.get("points", 0)
            tier = registered.get("tier", "Bronze")
            await send_message(chat_id, f"Your balance: *{points} points*\nYour tier: *{tier}*", token=token)
            return
        
        elif data == "menu:profile":
            if not registered.get("phone_number"):
                await send_message(chat_id, "Please share your phone number to complete your profile:", reply_markup=create_phone_keyboard(), token=token)
                state["stage"] = "awaiting_phone_profile"
                state["data"] = registered
                state["entry_id"] = registered["id"]
                set_state(chat_id, state)
                return
            
            if not registered.get("dob"):
                await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:", token=token)
                state["stage"] = "awaiting_dob_profile"
                state["data"] = registered
                state["entry_id"] = registered["id"]
                set_state(chat_id, state)
                return
            
            interests = registered.get("interests", []) or []
            interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
            
            await send_message(
                chat_id, 
                f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}",
                token=token
            )
            return
        
        elif data == "menu:discounts":
            if not registered.get("phone_number") or not registered.get("dob"):
                await send_message(chat_id, "Complete your profile to access discounts:", reply_markup=create_phone_keyboard(), token=token)
                state["stage"] = "awaiting_phone_profile"
                state["data"] = registered
                state["entry_id"] = registered["id"]
                set_state(chat_id, state)
                return
            
            interests = registered.get("interests", []) or []
            if not interests:
                await send_message(chat_id, "No interests set. Please update your profile.", token=token)
                return
            
            await send_message(chat_id, "Choose a category for discounts:", reply_markup=create_categories_keyboard(), token=token)
            return
        
        elif data == "menu:giveaways":
            try:
                if not await has_redeemed_discount(chat_id):
                    await send_message(chat_id, "Claim a discount first to unlock giveaways. Check Discounts:", reply_markup=create_main_menu_keyboard(), token=token)
                    return
                
                interests = registered.get("interests", []) or []
                if not interests:
                    await send_message(chat_id, "No interests set. Please update your profile.", token=token)
                    return
                
                def _query_giveaways():
                    return supabase.table("giveaways").select("*").in_("category", interests).eq("active", True).eq("business_type", "giveaway").execute()
                
                resp = await asyncio.to_thread(_query_giveaways)
                giveaways = resp.data if hasattr(resp, "data") else resp.get("data", [])
                
                if not giveaways:
                    await send_message(chat_id, "No giveaways available for your interests. Check Discover Offers:", reply_markup=create_main_menu_keyboard(), token=token)
                    return
                
                for g in giveaways:
                    business_type = g.get("business_type", "salon").capitalize()
                    cost = g.get("cost", 200)
                    message = f"{business_type}: *{g['name']}* at {g.get('salon_name')} ({g.get('category')})"
                    keyboard = {
                        "inline_keyboard": [
                            [{"text": f"Join ({cost} pts)", "callback_data": f"giveaway_points:{g['id']}"}],
                            [{"text": "Join via Booking", "callback_data": f"giveaway_book:{g['id']}"}]
                        ]
                    }
                    await send_message(chat_id, message, keyboard, token=token)
            except Exception as e:
                logger.error(f"Failed to fetch giveaways for chat_id {chat_id}: {str(e)}")
                await send_message(chat_id, "Failed to load giveaways. Please try again later.", token=token)
            return
        
        elif data.startswith("discount_category:"):
            category = data[len("discount_category:"):]
            if category not in CATEGORIES:
                await send_message(chat_id, "Invalid category.", token=token)
                return
            
            try:
                def _query_discounts():
                    return supabase.table("discounts").select("id, name, discount_percentage, category, business_id").eq("category", category).eq("active", True).execute()
                
                resp = await asyncio.to_thread(_query_discounts)
                discounts = resp.data if hasattr(resp, "data") else resp.get("data", [])
                
                if not discounts:
                    await send_message(chat_id, f"No discounts available in *{category}*.", token=token)
                    return
                
                for d in discounts:
                    business = await supabase_find_business(d["business_id"])
                    if not business:
                        await send_message(chat_id, f"Business not found for discount {d['name']}.", token=token)
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
                    
                    keyboard = {
                        "inline_keyboard": [
                            [
                                {"text": "View Profile", "callback_data": f"profile:{d['business_id']}"},
                                {"text": "View Services", "callback_data": f"services:{d['business_id']}"}
                            ],
                            [
                                {"text": "Book", "callback_data": f"book:{d['business_id']}"},
                                {"text": "Get Discount", "callback_data": f"get_discount:{d['id']}"}
                            ]
                        ]
                    }
                    
                    await send_message(chat_id, message, keyboard, token=token)
            except Exception as e:
                logger.error(f"Failed to fetch discounts for category {category}, chat_id {chat_id}: {str(e)}")
                await send_message(chat_id, "Failed to load discounts. Please try again later.", token=token)
            return
        
        elif data.startswith("profile:"):
            business_id = data[len("profile:"):]
            try:
                business = await supabase_find_business(business_id)
                if not business:
                    await send_message(chat_id, "Business not found.", token=token)
                    return
                
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
                
                await send_message(chat_id, msg, token=token)
            except Exception as e:
                logger.error(f"Failed to fetch business profile {business_id}: {str(e)}")
                await send_message(chat_id, "Failed to load profile.", token=token)
            return
        
        elif data.startswith("services:"):
            business_id = data[len("services:"):]
            try:
                business = await supabase_find_business(business_id)
                if not business:
                    await send_message(chat_id, "Business not found.", token=token)
                    return
                
                prices = business.get("prices", {})
                msg = "Services:\n" + "\n".join(f"{k}: {v}" for k, v in prices.items()) if prices else "No services listed."
                
                await send_message(chat_id, msg, token=token)
            except Exception as e:
                logger.error(f"Failed to fetch business services {business_id}: {str(e)}")
                await send_message(chat_id, "Failed to load services.", token=token)
            return
        
        elif data.startswith("book:"):
            business_id = data[len("book:"):]
            try:
                business = await supabase_find_business(business_id)
                if not business:
                    await send_message(chat_id, "Business not found.", token=token)
                    return
                
                # If we have a registered user, create a booking record (pending) and award booking-created points
                if registered:
                    try:
                        booking_payload = {
                            "user_id": registered["id"],
                            "business_id": business_id,
                            "booking_date": now_iso(),
                            "status": "pending",
                            "points_awarded": False,
                            "referral_awarded": False
                        }
                        
                        created_booking = await supabase_insert_return("user_bookings", booking_payload)
                        if created_booking:
                            # award small points for creating booking action (idempotency by history check)
                            if not await has_history(registered["id"], f"booking_created:{created_booking['id']}"):
                                await award_points(registered["id"], POINTS_BOOKING_CREATED, f"booking_created:{created_booking['id']}", created_booking["id"])
                            
                            await send_message(chat_id, f"Booking request created (ref: {created_booking['id']}). To confirm, contact {business['name']} at {business.get('phone_number', 'Not set')}.", token=token)
                        else:
                            await send_message(chat_id, f"Booking: Please contact {business['name']} at {business.get('phone_number', 'Not set')}.", token=token)
                    except Exception:
                        logger.exception("Failed to create booking in DB")
                        await send_message(chat_id, f"Booking: Please contact {business['name']} at {business.get('phone_number', 'Not set')}.", token=token)
                else:
                    await send_message(chat_id, f"To book, please contact {business['name']} at {business.get('phone_number', 'Not set')}.", token=token)
            except Exception as e:
                logger.error(f"Failed to fetch book info {business_id}: {str(e)}")
                await send_message(chat_id, "Failed to load booking info.", token=token)
            return
        
        elif data.startswith("get_discount:"):
            discount_id = data[len("get_discount:"):]
            try:
                uuid.UUID(discount_id)
                discount = await supabase_find_discount(discount_id)
                if not discount or not discount["active"]:
                    await send_message(chat_id, "Discount not found or inactive.", token=token)
                    return
                
                if not discount.get("business_id"):
                    logger.error(f"Missing business_id for discount_id: {discount_id}")
                    await send_message(chat_id, "Sorry, this discount is unavailable due to a configuration issue. Please try another.", token=token)
                    return
                
                code, expiry = await generate_discount_code(chat_id, discount["business_id"], discount_id)
                await send_message(chat_id, f"Your promo code: *{code}* for {discount['name']}. Valid until {expiry.split('T')[0]}.", token=token)
            except ValueError as ve:
                await send_message(chat_id, str(ve), token=token)
            except Exception as e:
                logger.error(f"Failed to generate discount code for discount_id: {discount_id}, chat_id: {chat_id}: {str(e)}")
                await send_message(chat_id, "Failed to generate promo code. Please try again later.", token=token)
            return
        
        elif data.startswith("giveaway_points:"):
            giveaway_id = data[len("giveaway_points:"):]
            try:
                uuid.UUID(giveaway_id)
                
                def _query_giveaway():
                    return supabase.table("giveaways").select("*").eq("id", giveaway_id).eq("active", True).limit(1).execute()
                
                resp = await asyncio.to_thread(_query_giveaway)
                giveaway = resp.data[0] if resp.data else None
                
                if not giveaway:
                    await send_message(chat_id, "Giveaway not found or inactive.", token=token)
                    return
                
                if not giveaway.get("business_id"):
                    logger.error(f"Missing business_id for giveaway_id: {giveaway_id}")
                    await send_message(chat_id, "Sorry, this giveaway is unavailable due to a configuration issue. Please try another.", token=token)
                    return
                
                cost = giveaway.get("cost", 200)
                if registered.get("points", 0) < cost:
                    await send_message(chat_id, f"Not enough points (need {cost}).", token=token)
                    return
                
                def _check_existing():
                    current_month = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    return supabase.table("user_giveaways").select("id").eq("telegram_id", chat_id).eq("giveaway_id", giveaway_id).gte("joined_at", current_month.isoformat()).execute()
                
                resp = await asyncio.to_thread(_check_existing)
                existing = resp.data if hasattr(resp, "data") else resp.get("data", [])
                
                if existing:
                    await send_message(chat_id, "You've already joined this giveaway this month.", token=token)
                    return
                
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
                await send_message(chat_id, f"Joined {business_type} {giveaway['name']} with {cost} points. Your 20% loser discount code: *{code}*, valid until {expiry.split('T')[0]}.", token=token)
            except ValueError:
                logger.error(f"Invalid giveaway_id format: {giveaway_id}")
                await send_message(chat_id, "Invalid giveaway ID.", token=token)
            except Exception as e:
                logger.error(f"Failed to process giveaway_points for giveaway_id: {giveaway_id}, chat_id: {chat_id}: {str(e)}")
                await send_message(chat_id, "Failed to join giveaway. Please try again later.", token=token)
            return
        
        elif data.startswith("giveaway_book:"):
            giveaway_id = data[len("giveaway_book:"):]
            try:
                uuid.UUID(giveaway_id)
                
                def _query_giveaway():
                    return supabase.table("giveaways").select("*").eq("id", giveaway_id).eq("active", True).limit(1).execute()
                
                resp = await asyncio.to_thread(_query_giveaway)
                giveaway = resp.data[0] if resp.data else None
                
                if not giveaway:
                    await send_message(chat_id, "Giveaway not found or inactive.", token=token)
                    return
                
                if not giveaway.get("business_id"):
                    logger.error(f"Missing business_id for giveaway_id: {giveaway_id}")
                    await send_message(chat_id, "Sorry, this giveaway is unavailable due to a configuration issue. Please try another.", token=token)
                    return
                
                def _check_existing():
                    current_month = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    return supabase.table("user_giveaways").select("id").eq("telegram_id", chat_id).eq("giveaway_id", giveaway_id).gte("joined_at", current_month.isoformat()).execute()
                
                resp = await asyncio.to_thread(_check_existing)
                existing = resp.data if hasattr(resp, "data") else resp.get("data", [])
                
                if existing:
                    await send_message(chat_id, "You've already joined this giveaway this month.", token=token)
                    return
                
                code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "awaiting_booking")
                
                await supabase_insert_return("user_giveaways", {
                    "telegram_id": chat_id,
                    "giveaway_id": giveaway_id,
                    "business_id": giveaway["business_id"],
                    "entry_status": "awaiting_booking",
                    "joined_at": now_iso()
                })
                
                business_type = giveaway.get("business_type", "salon").capitalize()
                await send_message(chat_id, f"Book a service at {business_type} {giveaway.get('salon_name')} with code *{code}* to join {giveaway['name']}. Valid until {expiry.split('T')[0]}.", token=token)
            except ValueError:
                logger.error(f"Invalid giveaway_id format: {giveaway_id}")
                await send_message(chat_id, "Invalid giveaway ID.", token=token)
            except Exception as e:
                logger.error(f"Failed to process giveaway_book for giveaway_id: {giveaway_id}, chat_id: {chat_id}: {str(e)}")
                await send_message(chat_id, "Failed to join giveaway. Please try again later.", token=token)
            return

    await safe_clear_markup(chat_id, message_id, token=token)









'''
try to connect with the url
# convo.py
import os
import asyncio
import logging
import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

from supabase import create_client, Client

from config import ADMIN_CHAT_ID, SUPABASE_URL, SUPABASE_KEY
from utils import (
    send_message,
    edit_message_text,
    edit_message_keyboard,
    safe_clear_markup,
    create_menu_options_keyboard,
    create_language_keyboard,
    create_gender_keyboard,
    create_interests_keyboard,
    create_categories_keyboard,
    create_main_menu_keyboard,
    create_phone_keyboard,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in .env")
    
# --- Constants -------------------------------------------------------------
INTERESTS = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining", "Discounts only", "Giveaways only"]
CATEGORIES = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining"]
EMOJIS = ["1️⃣", "2️⃣", "3️⃣"]

STARTER_POINTS = 100
POINTS_SIGNUP = 20
POINTS_PROFILE_COMPLETE = 40
POINTS_VIEW_DISCOUNT = 5
POINTS_CLAIM_PROMO = 10
POINTS_BOOKING_CREATED = 15
POINTS_BOOKING_VERIFIED = 200
POINTS_REFERRAL_JOIN = 10
POINTS_REFERRAL_VERIFIED = 100
DAILY_POINTS_CAP = 2000
STATE_TTL_SECONDS = 30 * 60

TIER_THRESHOLDS = [
    ("Bronze", 0),
    ("Silver", 200),
    ("Gold", 500),
    ("Platinum", 1000),
]

USER_STATES: Dict[int, Dict[str, Any]] = {}


# --- Utilities -------------------------------------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def compute_tier(points: int) -> str:
    tier = "Bronze"
    for name, threshold in TIER_THRESHOLDS:
        if points >= threshold:
            tier = name
    return tier

def create_business_profile_keyboard(business_id: str):
    """Create keyboard with web app button for business profile"""
    web_app_url = f"https://flutter-web-app-3q0r.onrender.com/?business_id={business_id}&action=view_profile"

    return {
        "inline_keyboard": [
            [
                {"text": "View Profile", "web_app": {"url": web_app_url}},
                {"text": "View Services", "callback_data": f"services:{business_id}"}
            ],
            [
                {"text": "Book", "callback_data": f"book:{business_id}"},
                {"text": "Get Discount", "callback_data": f"get_discount:{business_id}"}
            ]
        ]
    }

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

# --- Supabase helpers (all executed in threads because supabase client is sync) ----

async def supabase_find_registered(chat_id: int) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("central_bot_leads").select("*").eq("telegram_id", chat_id).eq("is_draft", False).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_registered failed")
        return None

async def supabase_find_draft(chat_id: int) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("central_bot_leads").select("*").eq("telegram_id", chat_id).eq("is_draft", True).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_draft failed")
        return None

async def supabase_insert_return(table: str, payload: dict) -> Optional[Dict[str, Any]]:
    def _ins():
        return supabase.table(table).insert(payload).execute()
    try:
        resp = await asyncio.to_thread(_ins)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.error("supabase_insert_return: no data")
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_insert_return failed")
        return None

async def supabase_update_by_id_return(table: str, entry_id: str, payload: dict) -> Optional[Dict[str, Any]]:
    def _upd():
        return supabase.table(table).update(payload).eq("id", entry_id).execute()
    try:
        resp = await asyncio.to_thread(_upd)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.error(f"supabase_update_by_id_return: no data for {table} id {entry_id}")
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_update_by_id_return failed")
        return None

async def supabase_find_business(business_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("businesses").select("*").eq("id", business_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_business failed")
        return None

async def supabase_find_discount(discount_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("discounts").select("*").eq("id", discount_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_discount failed")
        return None

async def supabase_find_giveaway(giveaway_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("giveaways").select("*").eq("id", giveaway_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_giveaway failed")
        return None

async def supabase_find_user_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("central_bot_leads").select("*").eq("id", user_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_user_by_id failed")
        return None

async def get_points_awarded_today(user_id: str) -> int:
    """Return sum of points awarded to user_id since UTC midnight."""
    def _q():
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        return supabase.table("points_history").select("points").eq("user_id", user_id).gte("awarded_at", today_start).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = resp.data if hasattr(resp, "data") else resp.get("data", []) or []
        return sum(int(r["points"]) for r in rows)
    except Exception:
        logger.exception("get_points_awarded_today failed")
        return 0

# --- Points / promos ------------------------------------------------------

async def has_history(user_id: str, reason: str) -> bool:
    def _q():
        return supabase.table("points_history").select("id").eq("user_id", user_id).eq("reason", reason).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = resp.data if hasattr(resp, "data") else resp.get("data", [])
        return bool(rows)
    except Exception:
        logger.exception("has_history failed")
        return False

async def award_points(user_id: str, delta: int, reason: str, booking_id: Optional[str] = None) -> dict:
    if delta == 0:
        return {"ok": True}
    
    # daily cap check
    awarded_today = await get_points_awarded_today(user_id)
    if awarded_today + abs(delta) > DAILY_POINTS_CAP:
        logger.warning(f"Daily cap reached for user {user_id}: today {awarded_today}, trying to add {delta}")
        return {"ok": False, "error": "daily_cap_reached"}

    try:
        user = await supabase_find_user_by_id(user_id)
        if not user:
            return {"ok": False, "error": "user_not_found"}
        
        old_points = int(user.get("points") or 0)
        new_points = max(0, old_points + delta)
        new_tier = compute_tier(new_points)
        
        def _upd_user():
            return supabase.table("central_bot_leads").update({
                "points": new_points, 
                "tier": new_tier,
                "last_login": now_iso()
            }).eq("id", user_id).execute()
        
        await asyncio.to_thread(_upd_user)
        
        hist = {"user_id": user_id, "points": delta, "reason": reason, "awarded_at": now_iso()}
        await supabase_insert_return("points_history", hist)
        
        logger.info(f"Awarded {delta} pts to user {user_id} ({old_points} -> {new_points}) for {reason}")
        
        # referral bonus for booking_verified
        if reason == "booking_verified":
            referred_by = user.get("referred_by")
            if referred_by:
                try:
                    await award_points(referred_by, POINTS_REFERRAL_VERIFIED, "referral_booking_verified", booking_id)
                except Exception:
                    logger.exception("Failed to award referral bonus")
        
        return {"ok": True, "old_points": old_points, "new_points": new_points, "tier": new_tier}
    except Exception:
        logger.exception("award_points failed")
        return {"ok": False, "error": "award_failed"}

async def generate_discount_code(chat_id: int, business_id: str, discount_id: str) -> (str, str):
    if not business_id or not discount_id:
        raise ValueError("Business ID or discount ID missing")

    # Check if user has already claimed this discount
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
        raise RuntimeError("Failed to save promo code")
    
    # award points for claiming promo
    try:
        user_row = await supabase_find_registered(chat_id)
        if user_row and not await has_history(user_row["id"], "claim_promo"):
            await award_points(user_row["id"], POINTS_CLAIM_PROMO, "claim_promo")
    except Exception:
        logger.exception("Failed to award claim promo points")
    
    logger.info("Generated discount code %s for chat %s", code, chat_id)
    return code, expiry

async def generate_promo_code(chat_id: int, business_id: str, giveaway_id: str, entry_status: str) -> (str, str):
    if not business_id or not giveaway_id:
        raise ValueError("Business ID or giveaway ID missing")

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
        raise RuntimeError("Failed to save giveaway promo code")
    logger.info("Generated giveaway code %s for chat %s", code, chat_id)
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
        logger.error(f"has_redeemed_discount failed for chat_id {chat_id}: {str(e)}")
        return False

# --- Notifications --------------------------------------------------------

async def notify_users(giveaway_id: str):
    try:
        giveaway = await supabase_find_giveaway(giveaway_id)
        if not giveaway:
            logger.error("notify_users: giveaway not found %s", giveaway_id)
            return
        
        def _q_users():
            return supabase.table("central_bot_leads").select("telegram_id").contains("interests", [giveaway["category"]]).execute()
        
        resp = await asyncio.to_thread(_q_users)
        users = resp.data if hasattr(resp, "data") else resp.get("data", [])
        
        for user in users:
            await send_message(
                user["telegram_id"], 
                f"New {giveaway['category']} offer: *{giveaway['name']}* at {giveaway.get('salon_name', 'Unknown')}. Check it out:"
            )
        
        logger.info("Notified %d users for giveaway %s", len(users), giveaway_id)
    except Exception:
        logger.exception("notify_users failed")
        
# --- Bot init ---------------------------------------------------

async def initialize_bot(webhook_url: str, token: str):
    # Set menu button and commands
    await set_menu_button(token)
    
    # Set webhook
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        try:
            response = await client.post(
                f"https://api.telegram.org/bot{token}/setWebhook",
                json={"url": webhook_url, "allowed_updates": ["message", "callback_query"]}
            )
            response.raise_for_status()
            logger.info(f"Webhook set to {webhook_url}")
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to set webhook: HTTP {e.response.status_code} - {e.response.text}")
        except Exception as e:
            logger.error(f"Failed to set webhook: {str(e)}")

# --- Conversation handlers -----------------------------------------------

async def handle_message(chat_id: int, message: Dict[str, Any], token: str):
    text = (message.get("text") or "").strip()
    contact = message.get("contact")
    state = get_state(chat_id) or {}

    # Handle /myid
    if text.lower() == "/myid":
        await send_message(chat_id, f"Your Telegram ID: {chat_id}", token=token)
        return

    # Handle /menu
    if text.lower() == "/menu":
        await send_message(chat_id, "Choose an option:", reply_markup=create_menu_options_keyboard(), token=token)
        return

    # Handle phone number
    if contact and state.get("stage") == "awaiting_phone_profile":
        phone_number = contact.get("phone_number")
        if not phone_number:
            await send_message(chat_id, "Invalid phone number. Please try again:", reply_markup=create_phone_keyboard(), token=token)
            return
        
        state["data"]["phone_number"] = phone_number
        entry_id = state.get("entry_id")
        if entry_id:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"phone_number": phone_number})
        
        registered = await supabase_find_registered(chat_id)
        # If user now has both phone and dob -> award profile-complete points (idempotent)
        try:
            if registered:
                # fetch fresh row
                user_row = await supabase_find_registered(chat_id)
                if user_row and user_row.get("dob") and user_row.get("phone_number"):
                    user_id = user_row["id"]
                    if not await has_history(user_id, "profile_complete"):
                        await award_points(user_id, POINTS_PROFILE_COMPLETE, "profile_complete")
        except Exception:
            logger.exception("Failed during profile completion points flow")

        if registered and not registered.get("dob"):
            await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:", token=token)
            state["stage"] = "awaiting_dob_profile"
        else:
            interests = registered.get("interests", []) or []
            interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
            await send_message(
                chat_id, 
                f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}",
                token=token
            )
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
        
        set_state(chat_id, state)
        return

    # Handle DOB (initial registration)
    if state.get("stage") == "awaiting_dob":
        if text.lower() == "/skip":
            state["data"]["dob"] = None
            entry_id = state.get("entry_id")
            if entry_id:
                await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": None})
            state["stage"] = "awaiting_interests"
            await send_message(chat_id, "Choose exactly 3 interests for this month:", reply_markup=create_interests_keyboard(), token=token)
            set_state(chat_id, state)
            return
        
        try:
            dob_obj = datetime.strptime(text, "%Y-%m-%d").date()
            if dob_obj.year < 1900 or dob_obj > datetime.now().date():
                await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.", token=token)
                return
            
            state["data"]["dob"] = dob_obj.isoformat()
            entry_id = state.get("entry_id")
            if entry_id:
                await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": state["data"]["dob"]})
            
            state["stage"] = "awaiting_interests"
            await send_message(chat_id, "Choose exactly 3 interests for this month:", reply_markup=create_interests_keyboard(), token=token)
            set_state(chat_id, state)
        except ValueError:
            await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.", token=token)
        return

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
            
            await send_message(
                chat_id, 
                f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}",
                token=token
            )
            
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
            return
        
        try:
            dob_obj = datetime.strptime(text, "%Y-%m-%d").date()
            if dob_obj.year < 1900 or dob_obj > datetime.now().date():
                await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.", token=token)
                return
            
            state["data"]["dob"] = dob_obj.isoformat()
            entry_id = state.get("entry_id")
            if entry_id:
                await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": state["data"]["dob"]})
            
            registered = await supabase_find_registered(chat_id)
            # If user now has phone and dob -> award profile complete
            try:
                if registered and registered.get("phone_number") and registered.get("dob"):
                    if not await has_history(registered["id"], "profile_complete"):
                        await award_points(registered["id"], POINTS_PROFILE_COMPLETE, "profile_complete")
            except Exception:
                logger.exception("Failed awarding profile_complete after dob update")

            interests = registered.get("interests", []) or []
            interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
            
            await send_message(
                chat_id, 
                f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}",
                token=token
            )
            
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
            return
        except ValueError:
            await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.", token=token)
        return

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
            await send_message(chat_id, "You're already registered! Explore options:", reply_markup=create_main_menu_keyboard(), token=token)
            return
        
        existing = await supabase_find_draft(chat_id)
        if existing:
            state = {
                "stage": "awaiting_gender",
                "data": {"language": existing.get("language")},
                "entry_id": existing.get("id"),
                "selected_interests": []
            }
            await send_message(chat_id, "What's your gender? (optional, helps target offers)", reply_markup=create_gender_keyboard(), token=token)
        else:
            state = {"stage": "awaiting_language", "data": {}, "entry_id": None, "selected_interests": []}
            await send_message(chat_id, "Welcome! Choose your language:", reply_markup=create_language_keyboard(), token=token)
        
        set_state(chat_id, state)
        return

    # Default response for unhandled messages
    registered = await supabase_find_registered(chat_id)
    if registered:
        await send_message(chat_id, "Please use /menu to interact or provide a valid command.", token=token)
    else:
        await send_message(chat_id, "Please start registration with /start.", token=token)

async def handle_callback(chat_id: int, callback_query: Dict[str, Any], token: str):
    data = callback_query.get("data")
    message_id = callback_query.get("message", {}).get("message_id")
    
    if not data or not message_id:
        await safe_clear_markup(chat_id, message_id, token=token)
        return

    registered = await supabase_find_registered(chat_id)
    state = get_state(chat_id) or {}

    # Handle admin approval/rejection callbacks
    if ADMIN_CHAT_ID is None:
        logger.warning("ADMIN_CHAT_ID is not set; admin functionality disabled")
    if chat_id and ADMIN_CHAT_ID is not None and int(chat_id) == int(ADMIN_CHAT_ID):
        if data.startswith("approve:"):
            business_id = data[len("approve:"):]
            try:
                uuid.UUID(business_id)
                business = await supabase_find_business(business_id)
                if not business:
                    await send_message(chat_id, f"Business with ID {business_id} not found.", token=token)
                    await safe_clear_markup(chat_id, message_id, token=token)
                    return
                
                await supabase_update_by_id_return("businesses", business_id, {"status": "approved", "updated_at": now_iso()})
                await send_message(chat_id, f"Business {business['name']} approved.", token=token)
                await send_message(business["telegram_id"], "Your business has been approved! You can now add discounts and giveaways.", token=token)
                await safe_clear_markup(chat_id, message_id, token=token)
            except ValueError:
                await send_message(chat_id, f"Invalid business ID format: {business_id}", token=token)
            except Exception as e:
                logger.error(f"Failed to approve business {business_id}: {str(e)}")
                await send_message(chat_id, "Failed to approve business. Please try again.", token=token)
            return

        if data.startswith("reject:"):
            business_id = data[len("reject:"):]
            try:
                uuid.UUID(business_id)
                business = await supabase_find_business(business_id)
                if not business:
                    await send_message(chat_id, f"Business with ID {business_id} not found.", token=token)
                    await safe_clear_markup(chat_id, message_id, token=token)
                    return
                
                await supabase_update_by_id_return("businesses", business_id, {"status": "rejected", "updated_at": now_iso()})
                await send_message(chat_id, f"Business {business['name']} rejected.", token=token)
                await send_message(business["telegram_id"], "Your business registration was rejected. Please contact support.", token=token)
                await safe_clear_markup(chat_id, message_id, token=token)
            except ValueError:
                await send_message(chat_id, f"Invalid business ID format: {business_id}", token=token)
            except Exception as e:
                logger.error(f"Failed to reject business {business_id}: {str(e)}")
                await send_message(chat_id, "Failed to reject business. Please try again.", token=token)
            return

        # Handle giveaway approval/rejection
        if data.startswith("giveaway_approve:"):
            giveaway_id = data[len("giveaway_approve:"):]
            try:
                uuid.UUID(giveaway_id)
                giveaway = await supabase_find_giveaway(giveaway_id)
                if not giveaway:
                    await send_message(chat_id, f"Giveaway with ID {giveaway_id} not found.", token=token)
                    await safe_clear_markup(chat_id, message_id, token=token)
                    return
                
                await supabase_update_by_id_return("giveaways", giveaway_id, {"active": True, "updated_at": now_iso()})
                await send_message(chat_id, f"Approved {giveaway['business_type']}: {giveaway['name']}.", token=token)
                
                business = await supabase_find_business(giveaway["business_id"])
                await send_message(business["telegram_id"], f"Your {giveaway['business_type']} '{giveaway['name']}' is approved and live!", token=token)
                
                await notify_users(giveaway_id)
                await safe_clear_markup(chat_id, message_id, token=token)
            except ValueError:
                await send_message(chat_id, f"Invalid giveaway ID: {giveaway_id}", token=token)
            except Exception as e:
                logger.error(f"Failed to approve giveaway {giveaway_id}: {str(e)}")
                await send_message(chat_id, "Failed to approve giveaway. Please try again.", token=token)
            return

        if data.startswith("giveaway_reject:"):
            giveaway_id = data[len("giveaway_reject:"):]
            try:
                uuid.UUID(giveaway_id)
                giveaway = await supabase_find_giveaway(giveaway_id)
                if not giveaway:
                    await send_message(chat_id, f"Giveaway with ID {giveaway_id} not found.", token=token)
                    await safe_clear_markup(chat_id, message_id, token=token)
                    return
                
                await supabase_update_by_id_return("giveaways", giveaway_id, {"active": False, "updated_at": now_iso()})
                await send_message(chat_id, f"Rejected {giveaway['business_type']}: {giveaway['name']}.", token=token)
                
                business = await supabase_find_business(giveaway["business_id"])
                await send_message(business["telegram_id"], f"Your {giveaway['business_type']} '{giveaway['name']}' was rejected. Contact support.", token=token)
                
                await safe_clear_markup(chat_id, message_id, token=token)
            except ValueError:
                await send_message(chat_id, f"Invalid giveaway ID: {giveaway_id}", token=token)
            except Exception as e:
                logger.error(f"Failed to reject giveaway {giveaway_id}: {str(e)}")
                await send_message(chat_id, "Failed to reject giveaway. Please try again.", token=token)
            return

    # Menu options
    if data == "menu:main":
        await safe_clear_markup(chat_id, message_id, token=token)
        await send_message(chat_id, "Explore options:", reply_markup=create_main_menu_keyboard(), token=token)
        return
    
    elif data == "menu:language":
        await safe_clear_markup(chat_id, message_id, token=token)
        await send_message(chat_id, "Choose your language:", reply_markup=create_language_keyboard(), token=token)
        state["stage"] = "awaiting_language_change"
        set_state(chat_id, state)
        return

    # Language selection
    if state.get("stage") in ["awaiting_language", "awaiting_language_change"] and data.startswith("lang:"):
        language = data[len("lang:"):]
        if language not in ["en", "ru"]:
            await send_message(chat_id, "Invalid language:", reply_markup=create_language_keyboard(), token=token)
            return
        
        state["data"]["language"] = language
        entry_id = state.get("entry_id")
        
        if not entry_id:
            created = await supabase_insert_return("central_bot_leads", {"telegram_id": chat_id, "language": language, "is_draft": True})
            state["entry_id"] = created.get("id") if created else None
        else:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"language": language})
        
        await safe_clear_markup(chat_id, message_id, token=token)
        
        if state.get("stage") == "awaiting_language":
            await send_message(chat_id, "What's your gender? (optional, helps target offers)", reply_markup=create_gender_keyboard(), token=token)
            state["stage"] = "awaiting_gender"
        else:
            await send_message(chat_id, "Language updated! Explore options:", reply_markup=create_main_menu_keyboard(), token=token)
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
        
        set_state(chat_id, state)
        return

    # Gender selection
    if state.get("stage") == "awaiting_gender" and data.startswith("gender:"):
        gender = data[len("gender:"):]
        if gender not in ["female", "male"]:
            await send_message(chat_id, "Invalid gender:", reply_markup=create_gender_keyboard(), token=token)
            return
        
        state["data"]["gender"] = gender
        entry_id = state.get("entry_id")
        if entry_id:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"gender": gender})
        
        await safe_clear_markup(chat_id, message_id, token=token)
        await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:", token=token)
        state["stage"] = "awaiting_dob"
        set_state(chat_id, state)
        return

    # Interests selection
    if state.get("stage") == "awaiting_interests":
        if data.startswith("interest:"):
            interest = data[len("interest:"):]
            if interest not in INTERESTS:
                logger.warning(f"Invalid interest selected: {interest}")
                return
            
            selected = state.get("selected_interests", [])
            if interest in selected:
                selected.remove(interest)
            elif len(selected) < 3:
                selected.append(interest)
            
            state["selected_interests"] = selected
            await edit_message_keyboard(chat_id, message_id, create_interests_keyboard(selected), token=token)
            set_state(chat_id, state)
            return
        
        elif data == "interests_done":
            selected = state.get("selected_interests", [])
            if len(selected) != 3:
                await send_message(chat_id, f"Please select exactly 3 interests (currently {len(selected)}):", reply_markup=create_interests_keyboard(selected), token=token)
                return
            
            await send_message(chat_id, "Interests saved! Finalizing registration...", token=token)
            entry_id = state.get("entry_id")
            
            if entry_id:
                # Remove direct points update; use award_points to handle tier & history
                await supabase_update_by_id_return("central_bot_leads", entry_id, {
                    "interests": selected,
                    "is_draft": False
                })
                
                try:
                    # award starter points (idempotent via history check)
                    if not await has_history(entry_id, "signup"):
                        await award_points(entry_id, STARTER_POINTS, "signup")
                    
                    # if referred_by present in state (and looks like uuid), set it and award referrer join points
                    referred = state.get("referred_by")
                    if referred:
                        try:
                            # ensure it's a valid UUID of another user
                            ref_uuid = str(uuid.UUID(referred))
                            # set referred_by column on the new user (best effort)
                            await supabase_update_by_id_return("central_bot_leads", entry_id, {"referred_by": ref_uuid})
                            # award referral join points to referrer (idempotent)
                            if not await has_history(ref_uuid, "referral_join"):
                                await award_points(ref_uuid, POINTS_REFERRAL_JOIN, "referral_join")
                        except Exception:
                            logger.debug("referred_by value is not a user UUID or awarding failed; skipping referral join")
                except Exception:
                    logger.exception("Failed awarding signup or referral points")
            
            await send_message(chat_id, f"Congrats! You've earned {STARTER_POINTS} points. Explore options:", reply_markup=create_main_menu_keyboard(), token=token)
            
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
            return

    # Registered user actions
    if registered:
        if data == "menu:points":
            points = registered.get("points", 0)
            tier = registered.get("tier", "Bronze")
            await send_message(chat_id, f"Your balance: *{points} points*\nYour tier: *{tier}*", token=token)
            return
        
        elif data == "menu:profile":
            if not registered.get("phone_number"):
                await send_message(chat_id, "Please share your phone number to complete your profile:", reply_markup=create_phone_keyboard(), token=token)
                state["stage"] = "awaiting_phone_profile"
                state["data"] = registered
                state["entry_id"] = registered["id"]
                set_state(chat_id, state)
                return
            
            if not registered.get("dob"):
                await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:", token=token)
                state["stage"] = "awaiting_dob_profile"
                state["data"] = registered
                state["entry_id"] = registered["id"]
                set_state(chat_id, state)
                return
            
            interests = registered.get("interests", []) or []
            interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
            
            await send_message(
                chat_id, 
                f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}",
                token=token
            )
            return
        
        elif data == "menu:discounts":
            if not registered.get("phone_number") or not registered.get("dob"):
        await send_message(chat_id, "Complete your profile to access discounts:", reply_markup=create_phone_keyboard(), token=token)
        state["stage"] = "awaiting_phone_profile"
        state["data"] = registered
        state["entry_id"] = registered["id"]
        set_state(chat_id, state)
        return

    interests = registered.get("interests", []) or []
    if not interests:
        await send_message(chat_id, "No interests set. Please update your profile.", token=token)
        return

    await send_message(chat_id, "Choose a category for discounts:", reply_markup=create_categories_keyboard(), token=token)
    return

    if data.startswith("discount_category:"):
        category = data[len("discount_category:"):]
    if category not in CATEGORIES:
        await send_message(chat_id, "Invalid category.", token=token)
        return
    try:
        def _query_discounts():
            return supabase.table("discounts").select(
                "id, name, discount_percentage, category, business_id"
            ).eq("category", category).eq("active", True).execute()

        resp = await asyncio.to_thread(_query_discounts)
        discounts = resp.data if hasattr(resp, "data") else resp.get("data", [])

        if not discounts:
            await send_message(chat_id, f"No discounts available in *{category}*.", token=token)
            return

        for d in discounts:
            business = await supabase_find_business(d["business_id"])
            if not business:
                await send_message(chat_id, f"Business not found for discount {d['name']}.", token=token)
                continue

            def _query_categories():
                return supabase.table("business_categories").select("category").eq(
                    "business_id", d["business_id"]
                ).execute()

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

            web_app_url = f"https://flutter-web-app-3q0r.onrender.com/?business_id={d['business_id']}&action=view_discount&discount_id={d['id']}"

            keyboard = {
                "inline_keyboard": [
                    [
                        {"text": "View in Web App", "web_app": {"url": web_app_url}}
                    ]
                ]
            }

            await send_message(chat_id, message, keyboard, token=token)
    except Exception as e:
        logger.error(f"Failed to fetch discounts for category {category}, chat_id {chat_id}: {str(e)}")
        await send_message(chat_id, "Failed to load discounts. Please try again later.", token=token)
    return

    if data.startswith("profile:"):
        business_id = data[len("profile:"):]
        try:
            business = await supabase_find_business(business_id)
            if not business:
                await send_message(chat_id, "Business not found.", token=token)
                return

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

            await send_message(chat_id, msg, token=token)
        except Exception as e:
            logger.error(f"Failed to fetch business profile {business_id}: {str(e)}")
            await send_message(chat_id, "Failed to load profile.", token=token)
        return

    if data.startswith("services:"):
        business_id = data[len("services:"):]
        try:
            business = await supabase_find_business(business_id)
            if not business:
                await send_message(chat_id, "Business not found.", token=token)
                return

            prices = business.get("prices", {})
            msg = "Services:\n" + "\n".join(f"{k}: {v}" for k, v in prices.items()) if prices else "No services listed."
            await send_message(chat_id, msg, token=token)
        except Exception as e:
            logger.error(f"Failed to fetch business services {business_id}: {str(e)}")
            await send_message(chat_id, "Failed to load services.", token=token)
        return

    if data.startswith("book:"):
        business_id = data[len("book:"): ]
        try:
            business = await supabase_find_business(business_id)
            if not business:
                await send_message(chat_id, "Business not found.", token=token)
                return

            # If we have a registered user, create a booking record (pending) and award booking-created points
            if registered:
                try:
                    booking_payload = {
                        "user_id": registered["id"],
                        "business_id": business_id,
                        "booking_date": now_iso(),
                        "status": "pending",
                        "points_awarded": False,
                        "referral_awarded": False
                    }

                    created_booking = await supabase_insert_return("user_bookings", booking_payload)
                    if created_booking:
                        if not await has_history(registered["id"], f"booking_created:{created_booking['id']}"):
                            await award_points(registered["id"], POINTS_BOOKING_CREATED, f"booking_created:{created_booking['id']}", created_booking["id"])

                        await send_message(chat_id, f"Booking request created (ref: {created_booking['id']}). To confirm, contact {business['name']} at {business.get('phone_number', 'Not set')}.", token=token)
                    else:
                        await send_message(chat_id, f"Booking: Please contact {business['name']} at {business.get('phone_number', 'Not set')}.", token=token)
                except Exception:
                    logger.exception("Failed to create booking in DB")
                    await send_message(chat_id, f"Booking: Please contact {business['name']} at {business.get('phone_number', 'Not set')}.", token=token)
            else:
                await send_message(chat_id, f"To book, please contact {business['name']} at {business.get('phone_number', 'Not set')}.", token=token)
        except Exception as e:
            logger.error(f"Failed to fetch book info {business_id}: {str(e)}")
            await send_message(chat_id, "Failed to load booking info.", token=token)
        return

    if data.startswith("get_discount:"):
        discount_id = data[len("get_discount:"): ]
        try:
            uuid.UUID(discount_id)
            discount = await supabase_find_discount(discount_id)
            if not discount or not discount.get("active"):
                await send_message(chat_id, "Discount not found or inactive.", token=token)
                return

            if not discount.get("business_id"):
                logger.error(f"Missing business_id for discount_id: {discount_id}")
                await send_message(chat_id, "Sorry, this discount is unavailable due to a configuration issue. Please try another.", token=token)
                return

            code, expiry = await generate_discount_code(chat_id, discount["business_id"], discount_id)
            await send_message(chat_id, f"Your promo code: *{code}* for {discount['name']}. Valid until {expiry.split('T')[0]}.", token=token)
        except ValueError as ve:
            await send_message(chat_id, str(ve), token=token)
        except Exception as e:
            logger.error(f"Failed to generate discount code for discount_id: {discount_id}, chat_id: {chat_id}: {str(e)}")
            await send_message(chat_id, "Failed to generate promo code. Please try again later.", token=token)
        return

    if data.startswith("giveaway_points:"):
        giveaway_id = data[len("giveaway_points:"):]
        try:
            uuid.UUID(giveaway_id)

            def _query_giveaway():
                return supabase.table("giveaways").select("*").eq("id", giveaway_id).eq("active", True).limit(1).execute()

            resp = await asyncio.to_thread(_query_giveaway)
            giveaway = resp.data[0] if resp.data else None

            if not giveaway:
                await send_message(chat_id, "Giveaway not found or inactive.", token=token)
                return

            if not giveaway.get("business_id"):
                logger.error(f"Missing business_id for giveaway_id: {giveaway_id}")
                await send_message(chat_id, "Sorry, this giveaway is unavailable due to a configuration issue. Please try another.", token=token)
                return

            cost = giveaway.get("cost", 200)
            if registered.get("points", 0) < cost:
                await send_message(chat_id, f"Not enough points (need {cost}).", token=token)
                return

            def _check_existing():
                current_month = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                return supabase.table("user_giveaways").select("id").eq("telegram_id", chat_id).eq("giveaway_id", giveaway_id).gte("joined_at", current_month.isoformat()).execute()

            resp = await asyncio.to_thread(_check_existing)
            existing = resp.data if hasattr(resp, "data") else resp.get("data", [])

            if existing:
                await send_message(chat_id, "You've already joined this giveaway this month.", token=token)
                return

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
            await send_message(chat_id, f"Joined {business_type} {giveaway['name']} with {cost} points. Your 20% loser discount code: *{code}*, valid until {expiry.split('T')[0]}.", token=token)
        except ValueError:
            logger.error(f"Invalid giveaway_id format: {giveaway_id}")
            await send_message(chat_id, "Invalid giveaway ID.", token=token)
        except Exception as e:
            logger.error(f"Failed to process giveaway_points for giveaway_id: {giveaway_id}, chat_id: {chat_id}: {str(e)}")
            await send_message(chat_id, "Failed to join giveaway. Please try again later.", token=token)
        return

    if data.startswith("giveaway_book:"):
        giveaway_id = data[len("giveaway_book:"):]
        try:
            uuid.UUID(giveaway_id)

            def _query_giveaway():
                return supabase.table("giveaways").select("*").eq("id", giveaway_id).eq("active", True).limit(1).execute()

            resp = await asyncio.to_thread(_query_giveaway)
            giveaway = resp.data[0] if resp.data else None

            if not giveaway:
                await send_message(chat_id, "Giveaway not found or inactive.", token=token)
                return

            if not giveaway.get("business_id"):
                logger.error(f"Missing business_id for giveaway_id: {giveaway_id}")
                await send_message(chat_id, "Sorry, this giveaway is unavailable due to a configuration issue. Please try another.", token=token)
                return

            def _check_existing():
                current_month = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                return supabase.table("user_giveaways").select("id").eq("telegram_id", chat_id).eq("giveaway_id", giveaway_id).gte("joined_at", current_month.isoformat()).execute()

            resp = await asyncio.to_thread(_check_existing)
            existing = resp.data if hasattr(resp, "data") else resp.get("data", [])

            if existing:
                await send_message(chat_id, "You've already joined this giveaway this month.", token=token)
                return

            code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "awaiting_booking")

            await supabase_insert_return("user_giveaways", {
                "telegram_id": chat_id,
                "giveaway_id": giveaway_id,
                "business_id": giveaway["business_id"],
                "entry_status": "awaiting_booking",
                "joined_at": now_iso()
            })

            business_type = giveaway.get("business_type", "salon").capitalize()
            await send_message(chat_id, f"Book a service at {business_type} {giveaway.get('salon_name')} with code *{code}* to join {giveaway['name']}. Valid until {expiry.split('T')[0]}.", token=token)
        except ValueError:
            logger.error(f"Invalid giveaway_id format: {giveaway_id}")
            await send_message(chat_id, "Invalid giveaway ID.", token=token)
        except Exception as e:
            logger.error(f"Failed to process giveaway_book for giveaway_id: {giveaway_id}, chat_id: {chat_id}: {str(e)}")
            await send_message(chat_id, "Failed to join giveaway. Please try again later.", token=token)
        return

    await safe_clear_markup(chat_id, message_id, token=token)
'''












'''
# convo_central.py
import os
import asyncio
import logging
import re
import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

from dotenv import load_dotenv
from supabase import create_client, Client

from utils import (
    send_message,
    edit_message_text,
    edit_message_keyboard,
    safe_clear_markup,
    create_menu_options_keyboard,
    create_language_keyboard,
    create_gender_keyboard,
    create_interests_keyboard,
    create_main_menu_keyboard,
    create_categories_keyboard,
    create_phone_keyboard,
)

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Environment / Supabase client
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Constants -------------------------------------------------------------
INTERESTS = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining", "Discounts only", "Giveaways only"]
CATEGORIES = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining"]

STARTER_POINTS = 100
POINTS_PROFILE_COMPLETE = 40
POINTS_CLAIM_PROMO = 10
POINTS_BOOKING_CREATED = 15
POINTS_REFERRAL_JOIN = 10
POINTS_REFERRAL_VERIFIED = 100

USER_STATES: Dict[int, Dict[str, Any]] = {}  # in-memory ephemeral per-chat state

# --- Helpers ---------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# -------------------------
# Supabase helpers (sync client executed in threads)
# -------------------------
async def supabase_find_registered(chat_id: int) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("central_bot_leads").select("*").eq("telegram_id", chat_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = getattr(resp, "data", resp.get("data") if isinstance(resp, dict) else None)
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_registered failed")
        return None

async def supabase_find_business(business_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("businesses").select("*").eq("id", business_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = getattr(resp, "data", resp.get("data") if isinstance(resp, dict) else None)
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_business failed")
        return None

async def supabase_find_discount(discount_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("discounts").select("*").eq("id", discount_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = getattr(resp, "data", resp.get("data") if isinstance(resp, dict) else None)
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_discount failed")
        return None

async def supabase_find_giveaway(giveaway_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("giveaways").select("*").eq("id", giveaway_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = getattr(resp, "data", resp.get("data") if isinstance(resp, dict) else None)
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_giveaway failed")
        return None

async def supabase_find_user_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("central_bot_leads").select("*").eq("id", user_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = getattr(resp, "data", resp.get("data") if isinstance(resp, dict) else None)
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_user_by_id failed")
        return None

async def supabase_insert_return(table: str, payload: dict) -> Optional[Dict[str, Any]]:
    def _ins():
        return supabase.table(table).insert(payload).execute()
    try:
        resp = await asyncio.to_thread(_ins)
        data = getattr(resp, "data", resp.get("data") if isinstance(resp, dict) else None)
        if not data:
            logger.error("supabase_insert_return: no data")
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_insert_return failed")
        return None

async def supabase_update_by_id_return(table: str, entry_id: str, payload: dict) -> Optional[Dict[str, Any]]:
    def _upd():
        return supabase.table(table).update(payload).eq("id", entry_id).execute()
    try:
        resp = await asyncio.to_thread(_upd)
        data = getattr(resp, "data", resp.get("data") if isinstance(resp, dict) else None)
        if not data:
            logger.error(f"supabase_update_by_id_return: no data for {table} id {entry_id}")
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_update_by_id_return failed")
        return None

# helper: find promo row in user_giveaways or user_discounts
async def find_promo_row(promo_code: str, business_id: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    def _q_ug():
        return supabase.table("user_giveaways").select("*").eq("promo_code", promo_code).eq("business_id", business_id).limit(1).execute()
    def _q_ud():
        return supabase.table("user_discounts").select("*").eq("promo_code", promo_code).eq("business_id", business_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q_ug)
        row = getattr(resp, "data", resp.get("data", []))
        if row:
            return row[0], "user_giveaways"
        resp2 = await asyncio.to_thread(_q_ud)
        row2 = getattr(resp2, "data", resp2.get("data", []))
        if row2:
            return row2[0], "user_discounts"
        return None, None
    except Exception:
        logger.exception("find_promo_row failed")
        return None, None

async def mark_promo_as_winner(table_name: str, row_id: str):
    def _upd():
        return supabase.table(table_name).update({"entry_status": "winner", "updated_at": now_iso()}).eq("id", row_id).execute()
    await asyncio.to_thread(_upd)

# --- Points & idempotency -------------------------------------------------
async def has_history(user_id: str, reason: str) -> bool:
    def _q():
        return supabase.table("points_history").select("id").eq("user_id", user_id).eq("reason", reason).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = getattr(resp, "data", resp.get("data", []))
        return bool(rows)
    except Exception:
        logger.exception("has_history failed")
        return False

async def award_points(user_id: str, delta: int, reason: str, booking_id: Optional[str] = None) -> dict:
    if delta == 0:
        return {"ok": True}
    try:
        user = await supabase_find_user_by_id(user_id)
        if not user:
            return {"ok": False, "error": "user_not_found"}
        old_points = int(user.get("points") or 0)
        new_points = max(0, old_points + delta)
        def _upd_user():
            return supabase.table("central_bot_leads").update({"points": new_points, "last_login": now_iso()}).eq("id", user_id).execute()
        await asyncio.to_thread(_upd_user)
        hist = {"user_id": user_id, "points": delta, "reason": reason, "awarded_at": now_iso()}
        await supabase_insert_return("points_history", hist)
        logger.info(f"Awarded {delta} pts to user {user_id} ({old_points} -> {new_points})")
        # referral: if booking verified, forward reward to referrer (best-effort)
        if reason.startswith("booking_verified:"):
            referred_by = user.get("referred_by")
            if referred_by:
                ref_reason = f"referral_booking_verified:{booking_id or reason.split(':')[-1]}"
                if not await has_history(referred_by, ref_reason):
                    await award_points(referred_by, POINTS_REFERRAL_VERIFIED, ref_reason, booking_id)
        return {"ok": True, "old_points": old_points, "new_points": new_points}
    except Exception:
        logger.exception("award_points failed")
        return {"ok": False, "error": "award_failed"}

# --- Promo code generation (unlimited claims allowed) ---------------------
async def generate_discount_code(chat_id: int, business_id: str, discount_id: str) -> (str, str):
    """Generate unique 4-digit code, insert to user_discounts. Allows unlimited claims."""
    if not business_id or not discount_id:
        raise ValueError("Business ID or discount ID missing")
    # create unique promo code (4 digits)
    for _ in range(50):
        code = f"{random.randint(0, 9999):04d}"
        def _check_existing():
            return supabase.table("user_discounts").select("id").eq("promo_code", code).eq("business_id", business_id).execute()
        existing = await asyncio.to_thread(_check_existing)
        if not getattr(existing, "data", None):
            break
    else:
        raise RuntimeError("Failed to generate unique promo code")

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
        raise RuntimeError("Failed to save promo code")
    logger.info("Generated discount code %s for chat %s", code, chat_id)
    return code, expiry

async def generate_promo_code(chat_id: int, business_id: str, giveaway_id: str, entry_status: str = "awaiting_booking") -> (str, str):
    """Generate promo code for giveaways."""
    if not business_id or not giveaway_id:
        raise ValueError("Business ID or giveaway ID missing")
    for _ in range(50):
        code = f"{random.randint(0, 9999):04d}"
        def _check_existing():
            return supabase.table("user_giveaways").select("id").eq("promo_code", code).eq("business_id", business_id).execute()
        existing = await asyncio.to_thread(_check_existing)
        if not getattr(existing, "data", None):
            break
    else:
        raise RuntimeError("Failed to generate unique promo code")
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
        raise RuntimeError("Failed to save giveaway promo code")
    logger.info("Generated giveaway code %s for chat %s", code, chat_id)
    return code, expiry

# --- Notifications --------------------------------------------------------
async def notify_users(giveaway_id: str):
    try:
        giveaway = await supabase_find_giveaway(giveaway_id)
        if not giveaway:
            logger.error("notify_users: giveaway not found %s", giveaway_id)
            return
        def _q_users():
            return supabase.table("central_bot_leads").select("telegram_id").contains("interests", [giveaway["category"]]).execute()
        resp = await asyncio.to_thread(_q_users)
        users = getattr(resp, "data", resp.get("data", []))
        for user in users:
            await send_message(user["telegram_id"], f"New {giveaway['category']} offer: *{giveaway['name']}* at {giveaway.get('salon_name', 'Unknown')}. Check it out:", create_main_menu_keyboard())
        logger.info("Notified %d users for giveaway %s", len(users), giveaway_id)
    except Exception:
        logger.exception("notify_users failed")

# --- Conversation handlers -----------------------------------------------

async def handle_message(chat_id: int, message: Dict[str, Any]):
    """Handle incoming message object from Telegram (text/contact)."""
    text = (message.get("text") or "").strip()
    lc_text = text.lower()
    user = await supabase_find_registered(chat_id)
    state = USER_STATES.get(chat_id, {})

    # /start - if registered show menu, otherwise start registration asking gender first
    if lc_text.startswith("/start"):
        if user:
            await send_message(chat_id, "Welcome back! Here's the menu:", create_main_menu_keyboard())
            return
        else:
            # start fresh registration: ask gender first
            USER_STATES[chat_id] = {"state": "awaiting_gender", "last_updated": now_iso()}
            await send_message(chat_id, "Welcome! What's your gender? (optional, helps target offers)", create_gender_keyboard())
            return

    if lc_text == "/menu":
        if user:
            await send_message(chat_id, "Here's the menu:", create_main_menu_keyboard())
        else:
            await send_message(chat_id, "Please complete registration first with /start.")
        return

    if lc_text == "/myid":
        await send_message(chat_id, f"Your Telegram ID is: {chat_id}")
        return

    # handle incoming contact for phone (when waiting for phone)
    if state.get("state") == "awaiting_phone" and message.get("contact"):
        phone = message["contact"].get("phone_number")
        if not phone:
            await send_message(chat_id, "Failed to get phone number. Please try again.", create_phone_keyboard())
            return
        # ensure user exists (should exist after interests_done)
        reg = await supabase_find_registered(chat_id)
        if not reg:
            await send_message(chat_id, "Registration record not found. Please /start to register.")
            USER_STATES.pop(chat_id, None)
            return
        updated = await supabase_update_by_id_return("central_bot_leads", reg["id"], {"phone": phone, "updated_at": now_iso()})
        if not updated:
            await send_message(chat_id, "Failed to save phone number. Please try again.", create_phone_keyboard())
            return
        # award profile complete if dob already present and history not awarded
        if updated.get("dob") and not await has_history(updated["id"], "profile_completed"):
            await award_points(updated["id"], POINTS_PROFILE_COMPLETE, "profile_completed")
        # continue post-action if any
        if state.get("action_after"):
            action = state.pop("action_after")
            USER_STATES.pop(chat_id, None)
            await _continue_after_verification(chat_id, action, updated)
        else:
            USER_STATES.pop(chat_id, None)
            await send_message(chat_id, "Phone number saved. Thank you!", create_main_menu_keyboard())
        return

    # handle DOB entered when waiting (format DD.MM.YYYY)
    if state.get("state") == "awaiting_dob" and text:
        dob_match = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", text)
        if not dob_match:
            await send_message(chat_id, "Please enter a valid date of birth (DD.MM.YYYY).")
            return
        day, month, year = map(int, dob_match.groups())
        try:
            dob_iso = datetime(year, month, day, tzinfo=timezone.utc).isoformat()
        except Exception:
            await send_message(chat_id, "Invalid date. Please enter a valid date of birth (DD.MM.YYYY).")
            return
        reg = await supabase_find_registered(chat_id)
        if not reg:
            await send_message(chat_id, "Registration not found. Please /start to register.")
            USER_STATES.pop(chat_id, None)
            return
        updated = await supabase_update_by_id_return("central_bot_leads", reg["id"], {"dob": dob_iso, "updated_at": now_iso()})
        if not updated:
            await send_message(chat_id, "Failed to save date of birth. Please try again.")
            return
        # award profile complete if phone exists
        if updated.get("phone") and not await has_history(updated["id"], "profile_completed"):
            await award_points(updated["id"], POINTS_PROFILE_COMPLETE, "profile_completed")
        if state.get("action_after"):
            action = state.pop("action_after")
            USER_STATES.pop(chat_id, None)
            await _continue_after_verification(chat_id, action, updated)
        else:
            USER_STATES.pop(chat_id, None)
            await send_message(chat_id, "Date of birth saved. Thank you!", create_main_menu_keyboard())
        return

    # if waiting referral id
    if state.get("state") == "awaiting_referral_id" and user:
        referred_by = None
        if text.isdigit():
            ref_user = await supabase_find_registered(int(text))
            if ref_user and ref_user["telegram_id"] != chat_id:
                referred_by = ref_user["id"]
        if referred_by:
            await supabase_update_by_id_return("central_bot_leads", user["id"], {"referred_by": referred_by})
            await award_points(user["id"], POINTS_REFERRAL_JOIN, "referral_joined")
            await award_points(referred_by, POINTS_REFERRAL_JOIN, f"referral_invited:{user['id']}")
            await send_message(chat_id, "Referral linked! You both earned points. Here's the menu:", create_main_menu_keyboard())
        else:
            await send_message(chat_id, "Invalid or self-referral ID. Try again or skip with /menu.")
        USER_STATES.pop(chat_id, None)
        return

    # default fallback
    if user:
        await send_message(chat_id, "Please use /menu to interact or provide a valid command.")
    else:
        await send_message(chat_id, "Please start registration with /start.")

# helper to continue after phone/dob verification
async def _continue_after_verification(chat_id: int, action: str, reg_row: Dict[str, Any]):
    """
    action format examples:
      - claim_discount:<discount_id>
      - claim_giveaway:<giveaway_id>
    """
    try:
        if action.startswith("claim_discount:"):
            discount_id = action.split(":", 1)[1]
            discount = await supabase_find_discount(discount_id)
            if not discount:
                await send_message(chat_id, "Discount not found.", create_main_menu_keyboard())
                return
            code, expiry = await generate_discount_code(chat_id, discount["business_id"], discount_id)
            await award_points(reg_row["id"], POINTS_CLAIM_PROMO, f"discount_claimed:{discount_id}")
            business = await supabase_find_business(discount["business_id"])
            await send_message(chat_id, f"Claimed {discount['name']} at {business.get('name', 'Unknown')}!\nCode: {code}\nExpires: {expiry[:10]}", create_main_menu_keyboard())
            return
        if action.startswith("claim_giveaway:"):
            giveaway_id = action.split(":", 1)[1]
            giveaway = await supabase_find_giveaway(giveaway_id)
            if not giveaway:
                await send_message(chat_id, "Giveaway not found.", create_main_menu_keyboard())
                return
            code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "awaiting_booking")
            await award_points(reg_row["id"], POINTS_BOOKING_CREATED, f"giveaway_joined:{giveaway_id}")
            business = await supabase_find_business(giveaway["business_id"])
            await send_message(chat_id, f"Entered {giveaway['name']} at {business.get('name', 'Unknown')}!\nCode: {code}\nExpires: {expiry[:10]}", create_main_menu_keyboard())
            return
    except Exception:
        logger.exception("Failed resuming action_after verification")
        await send_message(chat_id, "Failed to complete your action. Please try again.", create_main_menu_keyboard())

# callback handler
async def handle_callback(chat_id: int, callback_query: Dict[str, Any]):
    data = callback_query.get("data")
    message_id = callback_query.get("message", {}).get("message_id")
    user = await supabase_find_registered(chat_id)
    state = USER_STATES.get(chat_id, {})

    if data is None or message_id is None:
        await safe_clear_markup(chat_id, message_id)
        return

    # Menu navigation
    if data == "menu:main":
        if user:
            await edit_message_keyboard(chat_id, message_id, create_main_menu_keyboard())
        else:
            await send_message(chat_id, "Please complete registration first with /start.")
        return

    if data == "menu:language":
        await edit_message_keyboard(chat_id, message_id, create_language_keyboard())
        return

    if data.startswith("lang:"):
        lang = data.split(":", 1)[1]
        if user:
            await supabase_update_by_id_return("central_bot_leads", user["id"], {"language": lang})
            await send_message(chat_id, f"Language set to {lang}.", create_main_menu_keyboard())
        else:
            # create draft with language if possible
            created = await supabase_insert_return("central_bot_leads", {"telegram_id": chat_id, "language": lang, "is_draft": True, "created_at": now_iso()})
            await send_message(chat_id, "Language saved. Continue with /start.")
        await safe_clear_markup(chat_id, message_id)
        return

    # gender selection (first step)
    if data.startswith("gender:") and state.get("state") == "awaiting_gender":
        gender = data.split(":", 1)[1]
        state["gender"] = gender
        state["state"] = "awaiting_interests"
        state["interests"] = []
        state["last_updated"] = now_iso()
        USER_STATES[chat_id] = state
        await edit_message_text(chat_id, message_id, "Select up to 5 interests:", create_interests_keyboard(state["interests"], INTERESTS))
        return

    # interest toggles (up to 5)
    if data.startswith("interest:") and state.get("state") == "awaiting_interests":
        interest = data.split(":", 1)[1]
        selected = state.get("interests", [])
        if interest in selected:
            selected.remove(interest)
        elif len(selected) < 5:
            selected.append(interest)
        state["interests"] = selected
        state["last_updated"] = now_iso()
        USER_STATES[chat_id] = state
        await edit_message_keyboard(chat_id, message_id, create_interests_keyboard(selected, INTERESTS))
        return

    # interests done -> create registered user
    if data == "interests_done" and state.get("state") == "awaiting_interests":
        if not state.get("interests"):
            await send_message(chat_id, "Please select at least one interest.")
            return
        payload = {
            "telegram_id": chat_id,
            "gender": state.get("gender"),
            "interests": state.get("interests"),
            "is_draft": False,
            "created_at": now_iso(),
            "points": STARTER_POINTS,
            "language": state.get("language", "en")
        }
        inserted = await supabase_insert_return("central_bot_leads", payload)
        if inserted:
            # award starter points (idempotent via has_history)
            if not await has_history(inserted["id"], "signup"):
                await award_points(inserted["id"], STARTER_POINTS, "signup")
            await send_message(chat_id, "Registration complete! Here's the menu:", create_main_menu_keyboard())
        else:
            await send_message(chat_id, "Failed to complete registration. Please try again.")
        USER_STATES.pop(chat_id, None)
        await safe_clear_markup(chat_id, message_id)
        return

    # menu items
    if data == "menu:points" and user:
        points = user.get("points", 0)
        await send_message(chat_id, f"You have *{points}* points.", create_main_menu_keyboard())
        await safe_clear_markup(chat_id, message_id)
        return

    if data == "menu:profile" and user:
        points = user.get("points", 0)
        dob_str = user.get("dob", "Not set")
        if dob_str and dob_str != "Not set":
            try:
                dob_dt = datetime.fromisoformat(dob_str)
                dob_str = dob_dt.strftime("%d.%m.%Y")
            except Exception:
                pass
        phone = user.get("phone", "Not set")
        profile = (
            f"ID: {chat_id}\n"
            f"Points: {points}\n"
            f"Language: {user.get('language', 'N/A')}\n"
            f"Gender: {user.get('gender', 'N/A')}\n"
            f"DOB: {dob_str}\n"
            f"Phone: {phone}\n"
            f"Interests: {', '.join(user.get('interests', []))}"
        )
        await send_message(chat_id, profile, create_main_menu_keyboard())
        await safe_clear_markup(chat_id, message_id)
        return

    # discounts menu: show only categories that user is interested in
    if data == "menu:discounts" and user:
        user_categories = [i for i in user.get("interests", []) if i in CATEGORIES]
        if not user_categories:
            await send_message(chat_id, "No discount categories selected in your interests.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        await edit_message_keyboard(chat_id, message_id, create_categories_keyboard(user_categories))
        return

    # fetch discounts for category
    if data.startswith("discount_category:") and user:
        category = data.split(":", 1)[1]
        def _q_discounts():
            return supabase.table("discounts").select("*").eq("category", category).eq("active", True).execute()
        resp = await asyncio.to_thread(_q_discounts)
        discounts = getattr(resp, "data", [])
        if not discounts:
            await send_message(chat_id, f"No active {category} discounts available.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        text = f"Available discounts in {category}:\n\n" + "\n".join(f"- {d['name']}: {d.get('description', '')}" for d in discounts)
        inline_keyboard = []
        for d in discounts:
            inline_keyboard.append([
                {"text": "Profile", "callback_data": f"business_profile:{d['business_id']}"},
                {"text": "Book", "callback_data": f"discount_book:{d['id']}"},
                {"text": "Promo Code", "callback_data": f"discount_promo:{d['id']}"}
            ])
        keyboard = {"inline_keyboard": inline_keyboard}
        await edit_message_text(chat_id, message_id, text, keyboard)
        return

    # business profile view
    if data.startswith("business_profile:"):
        business_id = data.split(":", 1)[1]
        business = await supabase_find_business(business_id)
        if not business:
            await send_message(chat_id, "Business not found.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        text = f"Business Profile\nName: {business.get('name', 'N/A')}\nCity: {business.get('city', 'N/A')}\nDescription: {business.get('description', 'N/A')}"
        await send_message(chat_id, text, create_main_menu_keyboard())
        await safe_clear_markup(chat_id, message_id)
        return

    # user pressed Book or Promo for a discount
    if data.startswith("discount_book:") or data.startswith("discount_promo:"):
        discount_id = data.split(":", 1)[1]
        discount = await supabase_find_discount(discount_id)
        if not discount:
            await send_message(chat_id, "Discount not found.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        # if user has dob & phone -> proceed
        if user and user.get("dob") and user.get("phone"):
            try:
                code, expiry = await generate_discount_code(chat_id, discount["business_id"], discount_id)
                await award_points(user["id"], POINTS_CLAIM_PROMO, f"discount_claimed:{discount_id}")
                business = await supabase_find_business(discount["business_id"])
                await send_message(chat_id, f"Claimed {discount['name']} at {business.get('name', 'Unknown')}!\nCode: {code}\nExpires: {expiry[:10]}", create_main_menu_keyboard())
            except Exception as e:
                logger.exception("Error generating discount code")
                await send_message(chat_id, "Failed to generate discount. Please try again later.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        # otherwise ask for missing info, set action_after and continue later
        await safe_clear_markup(chat_id, message_id)
        action = f"claim_discount:{discount_id}"
        # prepare state: ask DOB if missing, otherwise ask phone
        if not user or not user.get("dob"):
            USER_STATES[chat_id] = {"state": "awaiting_dob", "action_after": action, "last_updated": now_iso()}
            await send_message(chat_id, "To claim this discount, please provide your date of birth (DD.MM.YYYY).")
        else:
            USER_STATES[chat_id] = {"state": "awaiting_phone", "action_after": action, "last_updated": now_iso()}
            await send_message(chat_id, "To claim this discount, please share your phone number:", create_phone_keyboard())
        return

    # giveaways list
    if data == "menu:giveaways" and user:
        def _q_giveaways():
            return supabase.table("giveaways").select("*").eq("active", True).execute()
        resp = await asyncio.to_thread(_q_giveaways)
        giveaways = getattr(resp, "data", [])
        if not giveaways:
            await send_message(chat_id, "No active giveaways available.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        keyboard = {"inline_keyboard": [[{"text": g["name"], "callback_data": f"giveaway:{g['id']}"}] for g in giveaways]}
        await edit_message_keyboard(chat_id, message_id, keyboard)
        return

    # user selects specific giveaway
    if data.startswith("giveaway:"):
        giveaway_id = data.split(":", 1)[1]
        giveaway = await supabase_find_giveaway(giveaway_id)
        if not giveaway:
            await send_message(chat_id, "Giveaway not found.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        # if user profile complete -> generate promo and enter
        if user and user.get("dob") and user.get("phone"):
            try:
                code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "awaiting_booking")
                await award_points(user["id"], POINTS_BOOKING_CREATED, f"giveaway_joined:{giveaway_id}")
                business = await supabase_find_business(giveaway["business_id"])
                await send_message(chat_id, f"Entered {giveaway['name']} at {business.get('name', 'Unknown')}!\nCode: {code}\nExpires: {expiry[:10]}", create_main_menu_keyboard())
            except Exception:
                logger.exception("Error entering giveaway")
                await send_message(chat_id, "Failed to enter giveaway. Please try again later.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        # otherwise ask for missing info and resume
        await safe_clear_markup(chat_id, message_id)
        action = f"claim_giveaway:{giveaway_id}"
        if not user or not user.get("dob"):
            USER_STATES[chat_id] = {"state": "awaiting_dob", "action_after": action, "last_updated": now_iso()}
            await send_message(chat_id, "To enter this giveaway, please provide your date of birth (DD.MM.YYYY).")
        else:
            USER_STATES[chat_id] = {"state": "awaiting_phone", "action_after": action, "last_updated": now_iso()}
            await send_message(chat_id, "To enter this giveaway, please share your phone number:", create_phone_keyboard())
        return

    if data == "menu:refer" and user:
        await send_message(chat_id, f"Share your ID to refer friends: {chat_id}\nEnter a friend's ID to link a referral:", create_main_menu_keyboard())
        USER_STATES[chat_id] = {"state": "awaiting_referral_id", "last_updated": now_iso()}
        await safe_clear_markup(chat_id, message_id)
        return

    # admin approve/reject callbacks handled elsewhere (central webhook) - just clear markup here
    await safe_clear_markup(chat_id, message_id)
'''
