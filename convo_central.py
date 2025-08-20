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
