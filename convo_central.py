# convo_central.py
import os
import asyncio
import logging
import re
import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

from dotenv import load_dotenv
from supabase import create_client, Client

from utils import (
    send_message,
    edit_message_keyboard,
    safe_clear_markup,
    create_menu_options_keyboard,
    create_language_keyboard,
    create_gender_keyboard,
    create_interests_keyboard,
    create_main_menu_keyboard,
    create_categories_keyboard,
    create_phone_keyboard,
    create_discount_card_keyboard,
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
STATE_TTL_SECONDS = 30 * 60

USER_STATES: Dict[int, Dict[str, Any]] = {}

# --- Utilities -------------------------------------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# --- Supabase helpers -----------------------------------------------------

async def supabase_find_draft(chat_id: int) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("central_bot_leads").select("*").eq("telegram_id", chat_id).eq("is_draft", True).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = getattr(resp, "data", resp.get("data") if isinstance(resp, dict) else None)
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_draft failed")
        return None

async def supabase_find_registered(chat_id: int) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("central_bot_leads").select("*").eq("telegram_id", chat_id).eq("is_draft", False).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = getattr(resp, "data", resp.get("data") if isinstance(resp, dict) else None)
        if not data:
            return None
        return data[0]
    except Exception:
        logger.exception("supabase_find_registered failed")
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

# --- Points / promos ------------------------------------------------------

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

async def generate_discount_code(chat_id: int, business_id: str, discount_id: str, entry_status: str = "standard") -> (str, str):
    """
    Generate a 4-digit promo code for a discount and persist to user_discounts.
    Note: unlimited claims allowed (no claimed-check).
    entry_status can be 'standard' or 'awaiting_booking' etc.
    """
    if not business_id or not discount_id:
        raise ValueError("Business ID or discount ID missing")

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
        "entry_status": entry_status,
        "joined_at": now_iso()
    }
    inserted = await supabase_insert_return("user_discounts", payload)
    if not inserted:
        raise RuntimeError("Failed to save promo code")
    logger.info("Generated discount promo code %s for chat %s", code, chat_id)
    return code, expiry

# --- Notifications --------------------------------------------------------

async def notify_users(giveaway_id: str):
    try:
        giveaway = await supabase_find_giveaway(giveaway_id)
        if not giveaway:
            logger.error("notify_users: giveaway not found %s", giveaway_id)
            return
        def _q_users():
            return supabase.table("central_bot_leads").select("telegram_id").eq("is_draft", False).contains("interests", [giveaway["category"]]).execute()
        resp = await asyncio.to_thread(_q_users)
        users = getattr(resp, "data", resp.get("data", []))
        for user in users:
            await send_message(user["telegram_id"], f"New {giveaway['category']} offer: *{giveaway['name']}* at {giveaway.get('salon_name', 'Unknown')}. Check it out:", create_main_menu_keyboard())
        logger.info("Notified %d users for giveaway %s", len(users), giveaway_id)
    except Exception:
        logger.exception("notify_users failed")

# --- Conversation handlers -----------------------------------------------

async def handle_message(chat_id: int, message: Dict[str, Any]):
    text = (message.get("text") or "").strip()
    lc_text = text.lower()
    user = await supabase_find_registered(chat_id)
    draft = None if user else await supabase_find_draft(chat_id)
    state = USER_STATES.get(chat_id, {})
    state_updated = False

    # ---- START flow now asks gender first, then interests (up to 5) ----
    if lc_text == "/start":
        if user:
            await send_message(chat_id, "Welcome back! Here's the menu:", create_main_menu_keyboard())
            return
        # new user or draft: ask gender first
        USER_STATES[chat_id] = {"state": "awaiting_gender", "last_updated": now_iso()}
        await send_message(chat_id, "Welcome! Please choose your gender:", create_gender_keyboard())
        return

    if lc_text == "/menu":
        if user:
            await send_message(chat_id, "Here's the menu:", create_main_menu_keyboard())
        else:
            await send_message(chat_id, "Please /start to begin and set your preferences.")
        return

    if lc_text == "/myid":
        await send_message(chat_id, f"Your Telegram ID is: {chat_id}")
        return

    # DOB collection can appear during post-action flows (booking/promo)
    if state.get("state") == "awaiting_dob":
        dob_match = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", text)
        if not dob_match:
            await send_message(chat_id, "Please enter a valid date of birth (DD.MM.YYYY).")
            return
        day, month, year = map(int, dob_match.groups())
        try:
            dob = datetime(year, month, day, tzinfo=timezone.utc).isoformat()
            # update existing user or draft, or create draft with dob
            if user:
                await supabase_update_by_id_return("central_bot_leads", user["id"], {"dob": dob})
                user = await supabase_find_registered(chat_id)
            elif state.get("draft_id"):
                await supabase_update_by_id_return("central_bot_leads", state["draft_id"], {"dob": dob})
                draft = await supabase_find_draft(chat_id)
                state["draft_id"] = draft["id"] if draft else state.get("draft_id")
            else:
                payload = {
                    "telegram_id": chat_id,
                    "is_draft": True,
                    "dob": dob,
                    "created_at": now_iso(),
                    "points": STARTER_POINTS
                }
                created = await supabase_insert_return("central_bot_leads", payload)
                if created:
                    state["draft_id"] = created["id"]

            # proceed to phone step
            state["state"] = "awaiting_phone"
            state["last_updated"] = now_iso()
            USER_STATES[chat_id] = state
            await send_message(chat_id, "Thanks — now please share your phone number:", create_phone_keyboard())
        except ValueError:
            await send_message(chat_id, "Invalid date. Please enter a valid date of birth (DD.MM.YYYY).")
        return

    # phone share handling (either registration or post-action)
    if state.get("state") == "awaiting_phone" and message.get("contact"):
        phone = message["contact"].get("phone_number")
        if not phone:
            await send_message(chat_id, "Failed to get phone number. Please try again.", create_phone_keyboard())
            return
        # update DB (user or draft)
        if user:
            updated = await supabase_update_by_id_return("central_bot_leads", user["id"], {"phone": phone, "is_draft": False, "updated_at": now_iso()})
            if updated:
                await award_points(updated["id"], POINTS_PROFILE_COMPLETE, "profile_completed")
                await send_message(chat_id, f"Profile updated! You earned {POINTS_PROFILE_COMPLETE} points. Here's the menu:", create_main_menu_keyboard())
                USER_STATES.pop(chat_id, None)
                # if there was a post_action attached, continue it
                post = state.get("post_action")
                if post:
                    await _continue_post_action(chat_id, post)
            else:
                await send_message(chat_id, "Failed to save phone number. Please try again.", create_phone_keyboard())
        else:
            # draft path
            if state.get("draft_id"):
                updated = await supabase_update_by_id_return("central_bot_leads", state["draft_id"], {"phone": phone, "is_draft": False, "updated_at": now_iso()})
                if updated:
                    await award_points(updated["id"], POINTS_PROFILE_COMPLETE, "profile_completed")
                    await send_message(chat_id, f"Registration complete! You earned {POINTS_PROFILE_COMPLETE} points. Here's the menu:", create_main_menu_keyboard())
                    USER_STATES.pop(chat_id, None)
                    post = state.get("post_action")
                    if post:
                        await _continue_post_action(chat_id, post)
                else:
                    await send_message(chat_id, "Failed to save phone number. Please try again.", create_phone_keyboard())
            else:
                # no draft and no user — create a record
                payload = {"telegram_id": chat_id, "phone": phone, "is_draft": False, "created_at": now_iso(), "points": STARTER_POINTS}
                created = await supabase_insert_return("central_bot_leads", payload)
                if created:
                    await award_points(created["id"], POINTS_PROFILE_COMPLETE, "profile_completed")
                    await send_message(chat_id, f"Registration complete! You earned {POINTS_PROFILE_COMPLETE} points. Here's the menu:", create_main_menu_keyboard())
                    USER_STATES.pop(chat_id, None)
                    post = state.get("post_action")
                    if post:
                        await _continue_post_action(chat_id, post)
                else:
                    await send_message(chat_id, "Failed to create profile. Please try again.", create_phone_keyboard())
        return

    # standard registration has changed: gender -> interests -> create draft -> show menu
    if state.get("state") == "awaiting_gender":
        # handled in callback flow (via callback). fallback: accept "male/female/other" typed
        if lc_text in ("male", "female", "other"):
            gender = lc_text
            state["gender"] = gender
            state["state"] = "awaiting_interests"
            state["interests"] = []
            state["last_updated"] = now_iso()
            USER_STATES[chat_id] = state
            await send_message(chat_id, "Great — now choose up to 5 interests:", create_interests_keyboard([], interests=INTERESTS, max_select=5))
            return
        await send_message(chat_id, "Please select your gender using the buttons.", create_gender_keyboard())
        return

    # if state flow is awaiting_interests we expect callback events, but keep a guard
    if state.get("state") == "awaiting_interests":
        await send_message(chat_id, "Please choose your interests using the buttons.", create_interests_keyboard(state.get("interests", []), interests=INTERESTS, max_select=5))
        return

    # default messages when nothing matched
    if user:
        await send_message(chat_id, "Please use /menu to interact or press buttons from the menu.")
    else:
        await send_message(chat_id, "Please use /start to begin the registration flow.")
    return

# helper to continue post-action after verification
async def _continue_post_action(chat_id: int, post: Dict[str, Any]):
    """
    post: {type: 'discount_book'|'discount_code', discount_id: str}
    """
    try:
        typ = post.get("type")
        discount_id = post.get("discount_id")
        discount = await supabase_find_discount(discount_id)
        if not discount:
            await send_message(chat_id, "Discount not found.")
            return
        business_id = discount.get("business_id")
        if typ == "discount_book":
            # generate code with awaiting_booking
            code, expiry = await generate_discount_code(chat_id, business_id, discount_id, entry_status="awaiting_booking")
            await send_message(chat_id, f"Booking created for *{discount.get('name')}*.\nCode: `{code}`\nExpires: {expiry[:10]}\nWe'll notify the business.", create_main_menu_keyboard())
            # award points for booking created if user exists
            user = await supabase_find_registered(chat_id)
            if user:
                await award_points(user["id"], POINTS_BOOKING_CREATED, f"giveaway_joined:{discount_id}")
        elif typ == "discount_code":
            code, expiry = await generate_discount_code(chat_id, business_id, discount_id, entry_status="standard")
            await send_message(chat_id, f"Promo code for *{discount.get('name')}*:\nCode: `{code}`\nExpires: {expiry[:10]}", create_main_menu_keyboard())
            user = await supabase_find_registered(chat_id)
            if user:
                await award_points(user["id"], POINTS_CLAIM_PROMO, f"discount_claimed:{discount_id}")
        else:
            await send_message(chat_id, "Unknown post action.", create_main_menu_keyboard())
    except Exception:
        logger.exception("Failed to continue post action")
        await send_message(chat_id, "Failed to complete action. Try again later.", create_main_menu_keyboard())

# --- Callback handlers ---------------------------------------------------

async def handle_callback(chat_id: int, callback_query: Dict[str, Any]):
    data = callback_query.get("data")
    message_id = callback_query.get("message", {}).get("message_id")
    user = await supabase_find_registered(chat_id)
    draft = None if user else await supabase_find_draft(chat_id)
    state = USER_STATES.get(chat_id, {})

    if data is None or message_id is None:
        await safe_clear_markup(chat_id, message_id)
        return

    # MAIN MENU
    if data == "menu:main":
        if user:
            await edit_message_keyboard(chat_id, message_id, create_main_menu_keyboard())
        else:
            # If user not yet registered, show menu but ask to /start
            await send_message(chat_id, "Please /start to set up your profile and preferences.")
        return

    if data == "menu:language":
        await edit_message_keyboard(chat_id, message_id, create_language_keyboard())
        return

    # GENDER selection during registration
    if data.startswith("gender:") and state.get("state") == "awaiting_gender":
        gender = data[len("gender:"):]
        state["gender"] = gender
        state["state"] = "awaiting_interests"
        state["interests"] = []
        state["last_updated"] = now_iso()
        USER_STATES[chat_id] = state
        await edit_message_keyboard(chat_id, message_id, create_interests_keyboard([], interests=INTERESTS, max_select=5))
        return

    # INTERESTS selection (toggle)
    if data.startswith("interest:") and state.get("state") == "awaiting_interests":
        interest = data[len("interest:"):]
        selected = state.get("interests", [])
        if interest in selected:
            selected.remove(interest)
        else:
            if len(selected) < 5:
                selected.append(interest)
        state["interests"] = selected
        state["last_updated"] = now_iso()
        USER_STATES[chat_id] = state
        await edit_message_keyboard(chat_id, message_id, create_interests_keyboard(selected, interests=INTERESTS, max_select=5))
        return

    # Done with interests -> create draft and show menu
    if data == "interests_done" and state.get("state") == "awaiting_interests":
        selected = state.get("interests", [])
        if not selected:
            await send_message(chat_id, "Please select at least one interest before continuing.")
            return
        # create draft in DB (or update existing draft)
        if draft and draft.get("id"):
            await supabase_update_by_id_return("central_bot_leads", draft["id"], {"gender": state.get("gender"), "interests": selected, "updated_at": now_iso()})
        else:
            payload = {
                "telegram_id": chat_id,
                "is_draft": True,
                "gender": state.get("gender"),
                "interests": selected,
                "created_at": now_iso(),
                "points": STARTER_POINTS
            }
            created = await supabase_insert_return("central_bot_leads", payload)
            if created:
                state["draft_id"] = created["id"]
        # Done: present main menu
        USER_STATES.pop(chat_id, None)
        await send_message(chat_id, "Thanks — preferences saved. Here's the menu:", create_main_menu_keyboard())
        await safe_clear_markup(chat_id, message_id)
        return

    # LANGUAGE selection
    if data.startswith("lang:"):
        lang = data[len("lang:"):]
        if user:
            await supabase_update_by_id_return("central_bot_leads", user["id"], {"language": lang})
            await send_message(chat_id, f"Language set to {lang}.", create_main_menu_keyboard())
        else:
            await send_message(chat_id, "Please /start to set up your profile first.")
        await safe_clear_markup(chat_id, message_id)
        return

    # PROFILE view in the menu
    if data == "menu:profile":
        if user:
            profile = (
                f"ID: {chat_id}\n"
                f"DOB: {user.get('dob','N/A')}\n"
                f"Language: {user.get('language', 'N/A')}\n"
                f"Gender: {user.get('gender', 'N/A')}\n"
                f"Interests: {', '.join(user.get('interests', []) or [])}\n"
                f"Phone: {user.get('phone', 'N/A')}\n"
                f"Points: {user.get('points', 0)}"
            )
            await send_message(chat_id, profile, create_main_menu_keyboard())
        else:
            await send_message(chat_id, "Please /start to set up your profile first.")
        await safe_clear_markup(chat_id, message_id)
        return

    # POINTS view
    if data == "menu:points":
        if user:
            points = user.get("points", 0)
            await send_message(chat_id, f"You have {points} points.", create_main_menu_keyboard())
        else:
            await send_message(chat_id, "Please /start to set up your profile first.")
        await safe_clear_markup(chat_id, message_id)
        return

    # DISCOUNTS flow: categories
    if data == "menu:discounts":
        await edit_message_keyboard(chat_id, message_id, create_categories_keyboard(CATEGORIES))
        return

    if data.startswith("discount_category:"):
        category = data[len("discount_category:"):]
        # list discounts of that category
        def _q_discounts():
            return supabase.table("discounts").select("*").eq("category", category).eq("active", True).execute()
        resp = await asyncio.to_thread(_q_discounts)
        discounts = getattr(resp, "data", resp.get("data", []))
        if not discounts:
            await send_message(chat_id, f"No active {category} discounts available.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        # Build keyboard listing discounts. Each button opens discount details
        keyboard = {"inline_keyboard": [[{"text": d["name"], "callback_data": f"discount:{d['id']}"}] for d in discounts]}
        await edit_message_keyboard(chat_id, message_id, keyboard)
        return

    # Show discount details card with Profile | Book | Promo Code
    if data.startswith("discount:"):
        discount_id = data[len("discount:"):]
        discount = await supabase_find_discount(discount_id)
        if not discount:
            await send_message(chat_id, "Discount not found.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        # Build message and keyboard
        business = await supabase_find_business(discount.get("business_id"))
        text = f"*{discount.get('name')}*\n{discount.get('description','')}\nAt: {business.get('name','Unknown') if business else 'Unknown'}"
        kb = create_discount_card_keyboard(discount_id)
        await send_message(chat_id, text, kb)
        await safe_clear_markup(chat_id, message_id)
        return

    # Discount card actions
    if data.startswith("discount_profile:"):
        discount_id = data[len("discount_profile:"):]
        # show profile (reuse above)
        if user:
            profile = (
                f"ID: {chat_id}\n"
                f"DOB: {user.get('dob','N/A')}\n"
                f"Language: {user.get('language', 'N/A')}\n"
                f"Gender: {user.get('gender', 'N/A')}\n"
                f"Interests: {', '.join(user.get('interests', []) or [])}\n"
                f"Phone: {user.get('phone', 'N/A')}\n"
                f"Points: {user.get('points', 0)}"
            )
            await send_message(chat_id, profile, create_main_menu_keyboard())
        else:
            await send_message(chat_id, "Please /start to set up your profile first.")
        await safe_clear_markup(chat_id, message_id)
        return

    # Book flow: requires DOB + phone if missing; otherwise create awaiting_booking entry and send code
    if data.startswith("discount_book:") or data.startswith("discount_code:"):
        is_book = data.startswith("discount_book:")
        discount_id = data.split(":", 1)[1]
        discount = await supabase_find_discount(discount_id)
        if not discount:
            await send_message(chat_id, "Discount not found.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        # Check if user has dob & phone
        if user and user.get("dob") and user.get("phone"):
            # ready: generate code immediately
            entry_status = "awaiting_booking" if is_book else "standard"
            code, expiry = await generate_discount_code(chat_id, discount.get("business_id"), discount_id, entry_status=entry_status)
            if is_book:
                await send_message(chat_id, f"Booking created for *{discount.get('name')}*.\nCode: `{code}`\nExpires: {expiry[:10]}", create_main_menu_keyboard())
                await award_points(user["id"], POINTS_BOOKING_CREATED, f"giveaway_joined:{discount_id}")
            else:
                await send_message(chat_id, f"Promo code for *{discount.get('name')}*:\nCode: `{code}`\nExpires: {expiry[:10]}", create_main_menu_keyboard())
                await award_points(user["id"], POINTS_CLAIM_PROMO, f"discount_claimed:{discount_id}")
            await safe_clear_markup(chat_id, message_id)
            return
        # else: ask for DOB then phone, store a post_action in state so we can continue afterwards
        USER_STATES[chat_id] = {
            "state": "awaiting_dob",
            "post_action": {"type": "discount_book" if is_book else "discount_code", "discount_id": discount_id},
            "last_updated": now_iso(),
        }
        await send_message(chat_id, "To proceed we need your date of birth. Please enter DOB in DD.MM.YYYY format.")
        await safe_clear_markup(chat_id, message_id)
        return

    # GIVEAWAYS, REFER, etc: reuse existing behavior
    if data == "menu:giveaways":
        def _q_giveaways():
            return supabase.table("giveaways").select("*").eq("active", True).execute()
        giveaways = (await asyncio.to_thread(_q_giveaways)).data
        if not giveaways:
            await send_message(chat_id, "No active giveaways available.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        keyboard = {
            "inline_keyboard": [[{"text": g["name"], "callback_data": f"giveaway:{g['id']}"}] for g in giveaways]
        }
        await edit_message_keyboard(chat_id, message_id, keyboard)
        return

    if data.startswith("giveaway:"):
        giveaway_id = data[len("giveaway:"):]
        giveaway = await supabase_find_giveaway(giveaway_id)
        if not giveaway:
            await send_message(chat_id, "Giveaway not found.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        # same logic as previously for giveaways
        code, expiry = await generate_discount_code(chat_id, giveaway["business_id"], giveaway_id, entry_status="awaiting_booking")
        await award_points(user["id"], POINTS_BOOKING_CREATED, f"giveaway_joined:{giveaway_id}") if user else None
        business = await supabase_find_business(giveaway["business_id"])
        await send_message(chat_id, f"Entered {giveaway['name']} at {business.get('name', 'Unknown')}!\nCode: {code}\nExpires: {expiry[:10]}", create_main_menu_keyboard())
        await safe_clear_markup(chat_id, message_id)
        return

    if data == "menu:refer":
        await send_message(chat_id, f"Share your ID to refer friends: {chat_id}\nEnter a friend's ID to link a referral:", create_main_menu_keyboard())
        USER_STATES[chat_id] = {"state": "awaiting_referral_id", "last_updated": now_iso()}
        await safe_clear_markup(chat_id, message_id)
        return

    # fallback: clear markup
    await safe_clear_markup(chat_id, message_id)
