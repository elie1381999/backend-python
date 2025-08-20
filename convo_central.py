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
STATE_TTL_SECONDS = 30 * 60

USER_STATES: Dict[int, Dict[str, Any]] = {}

# --- Utilities -------------------------------------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# --- Supabase helpers (all executed in threads because supabase client is sync) ----

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

async def supabase_find_user_by_telegram_id(telegram_id: int) -> Optional[Dict[str, Any]]:
    return await supabase_find_registered(telegram_id)

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

async def generate_discount_code(chat_id: int, business_id: str, discount_id: str) -> (str, str):
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
        "entry_status": "standard",
        "joined_at": now_iso()
    }
    inserted = await supabase_insert_return("user_discounts", payload)
    if not inserted:
        raise RuntimeError("Failed to save promo code")
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

# --- Bot init (optional) ---------------------------------------------------

async def initialize_bot(webhook_url: str, token: Optional[str] = None):
    # set menu & webhook for central bot
    await safe_clear_markup  # reference to avoid lint warning for import
    await set_menu_button if False else None  # noop placeholder if not calling here
    # We do not call set webhook here because central_bot may handle it (optional)

# --- Conversation handlers -----------------------------------------------

async def handle_message(chat_id: int, message: Dict[str, Any]):
    text = (message.get("text") or "").strip()
    lc_text = text.lower()
    user = await supabase_find_registered(chat_id)
    state = USER_STATES.get(chat_id, {})
    state_updated = False

    if lc_text == "/start":
        if user:
            await send_message(chat_id, "Welcome back! Here's the menu:", create_main_menu_keyboard())
            return
        else:
            await send_message(chat_id, "Welcome! What's your gender?", create_gender_keyboard())
            USER_STATES[chat_id] = {"state": "awaiting_gender", "last_updated": now_iso()}
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

    # states
    if state.get("state") == "awaiting_dob":
        dob_match = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", text)
        if not dob_match:
            await send_message(chat_id, "Please enter a valid date of birth (DD.MM.YYYY).")
            return
        day, month, year = map(int, dob_match.groups())
        try:
            dob = datetime(year, month, day, tzinfo=timezone.utc).isoformat()
            updated = await supabase_update_by_id_return("central_bot_leads", user["id"], {"dob": dob, "updated_at": now_iso()})
            if not updated:
                await send_message(chat_id, "Failed to save date of birth. Please try again.")
                return
            user = updated
            if user.get("phone") and not await has_history(user["id"], "profile_completed"):
                await award_points(user["id"], POINTS_PROFILE_COMPLETE, "profile_completed")
            if user.get("phone"):
                if "action_after" in state:
                    action = state.pop("action_after")
                    if action.startswith("claim_discount:"):
                        discount_id = action[14:]
                        discount = await supabase_find_discount(discount_id)
                        if discount:
                            try:
                                code, expiry = await generate_discount_code(chat_id, discount["business_id"], discount_id)
                                await award_points(user["id"], POINTS_CLAIM_PROMO, f"discount_claimed:{discount_id}")
                                business = await supabase_find_business(discount["business_id"])
                                await send_message(chat_id, f"Claimed {discount['name']} at {business.get('name', 'Unknown')}!\nCode: {code}\nExpires: {expiry[:10]}", create_main_menu_keyboard())
                            except Exception as e:
                                await send_message(chat_id, str(e))
                    elif action.startswith("claim_giveaway:"):
                        giveaway_id = action[14:]
                        giveaway = await supabase_find_giveaway(giveaway_id)
                        if giveaway:
                            try:
                                code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "awaiting_booking")
                                await award_points(user["id"], POINTS_BOOKING_CREATED, f"giveaway_joined:{giveaway_id}")
                                business = await supabase_find_business(giveaway["business_id"])
                                await send_message(chat_id, f"Entered {giveaway['name']} at {business.get('name', 'Unknown')}!\nCode: {code}\nExpires: {expiry[:10]}", create_main_menu_keyboard())
                            except Exception as e:
                                await send_message(chat_id, str(e))
                    USER_STATES.pop(chat_id, None)
                else:
                    await send_message(chat_id, "Date of birth set.", create_main_menu_keyboard())
            else:
                state["state"] = "awaiting_phone"
                state["last_updated"] = now_iso()
                USER_STATES[chat_id] = state
                await send_message(chat_id, "Please share your phone number:", create_phone_keyboard())
        except ValueError:
            await send_message(chat_id, "Invalid date. Please enter a valid date of birth (DD.MM.YYYY).")
        return

    if state.get("state") == "awaiting_phone" and message.get("contact"):
        phone = message["contact"].get("phone_number")
        if not phone:
            await send_message(chat_id, "Failed to get phone number. Please try again.", create_phone_keyboard())
            return
        updated = await supabase_update_by_id_return("central_bot_leads", user["id"], {"phone": phone, "updated_at": now_iso()})
        if not updated:
            await send_message(chat_id, "Failed to save phone number. Please try again.", create_phone_keyboard())
            return
        user = updated
        if user.get("dob") and not await has_history(user["id"], "profile_completed"):
            await award_points(user["id"], POINTS_PROFILE_COMPLETE, "profile_completed")
        if "action_after" in state:
            action = state.pop("action_after")
            if action.startswith("claim_discount:"):
                discount_id = action[14:]
                discount = await supabase_find_discount(discount_id)
                if discount:
                    try:
                        code, expiry = await generate_discount_code(chat_id, discount["business_id"], discount_id)
                        await award_points(user["id"], POINTS_CLAIM_PROMO, f"discount_claimed:{discount_id}")
                        business = await supabase_find_business(discount["business_id"])
                        await send_message(chat_id, f"Claimed {discount['name']} at {business.get('name', 'Unknown')}!\nCode: {code}\nExpires: {expiry[:10]}", create_main_menu_keyboard())
                    except Exception as e:
                        await send_message(chat_id, str(e))
            elif action.startswith("claim_giveaway:"):
                giveaway_id = action[14:]
                giveaway = await supabase_find_giveaway(giveaway_id)
                if giveaway:
                    try:
                        code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "awaiting_booking")
                        await award_points(user["id"], POINTS_BOOKING_CREATED, f"giveaway_joined:{giveaway_id}")
                        business = await supabase_find_business(giveaway["business_id"])
                        await send_message(chat_id, f"Entered {giveaway['name']} at {business.get('name', 'Unknown')}!\nCode: {code}\nExpires: {expiry[:10]}", create_main_menu_keyboard())
                    except Exception as e:
                        await send_message(chat_id, str(e))
        else:
            await send_message(chat_id, "Phone number set.", create_main_menu_keyboard())
        USER_STATES.pop(chat_id, None)
        return

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

    # default
    if user:
        await send_message(chat_id, "Please use /menu to interact or provide a valid command.")
    else:
        await send_message(chat_id, "Please start registration with /start.")

async def handle_callback(chat_id: int, callback_query: Dict[str, Any]):
    data = callback_query.get("data")
    message_id = callback_query.get("message", {}).get("message_id")
    user = await supabase_find_registered(chat_id)
    state = USER_STATES.get(chat_id, {})

    if data is None or message_id is None:
        await safe_clear_markup(chat_id, message_id)
        return

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
        lang = data[len("lang:"):]
        if user:
            await supabase_update_by_id_return("central_bot_leads", user["id"], {"language": lang})
            await send_message(chat_id, f"Language set to {lang}.", create_main_menu_keyboard())
        else:
            await send_message(chat_id, "Please complete registration first with /start.")
        await safe_clear_markup(chat_id, message_id)
        return

    if data.startswith("gender:") and state.get("state") == "awaiting_gender":
        gender = data[len("gender:"):]
        state["gender"] = gender
        state["state"] = "awaiting_interests"
        state["interests"] = []
        state["last_updated"] = now_iso()
        USER_STATES[chat_id] = state
        await edit_message_text(chat_id, message_id, "Select up to 5 interests:", create_interests_keyboard(state["interests"]))
        return

    if data.startswith("interest:") and state.get("state") == "awaiting_interests":
        interest = data[len("interest:"):]
        interests = state.get("interests", [])
        if interest in interests:
            interests.remove(interest)
        elif len(interests) < 5:
            interests.append(interest)
        state["interests"] = interests
        state["last_updated"] = now_iso()
        USER_STATES[chat_id] = state
        await edit_message_keyboard(chat_id, message_id, create_interests_keyboard(state["interests"]))
        return

    if data == "interests_done" and state.get("state") == "awaiting_interests":
        if not state.get("interests"):
            await send_message(chat_id, "Please select at least one interest.")
            return
        payload = {
            "telegram_id": chat_id,
            "gender": state["gender"],
            "interests": state["interests"],
            "is_draft": False,
            "created_at": now_iso(),
            "points": STARTER_POINTS,
            "language": "en"
        }
        inserted = await supabase_insert_return("central_bot_leads", payload)
        if inserted:
            await send_message(chat_id, "Registration complete! Here's the menu:", create_main_menu_keyboard())
        else:
            await send_message(chat_id, "Failed to complete registration. Please try again.")
        USER_STATES.pop(chat_id, None)
        await safe_clear_markup(chat_id, message_id)
        return

    if data == "menu:points" and user:
        points = user.get("points", 0)
        await send_message(chat_id, f"You have {points} points.", create_main_menu_keyboard())
        await safe_clear_markup(chat_id, message_id)
        return

    if data == "menu:profile" and user:
        points = user.get("points", 0)
        dob_str = user.get("dob", "Not set")
        if dob_str != "Not set":
            dob_dt = datetime.fromisoformat(dob_str)
            dob_str = dob_dt.strftime("%d.%m.%Y")
        phone = user.get("phone", "Not set")
        profile = f"ID: {chat_id}\nPoints: {points}\nLanguage: {user.get('language', 'N/A')}\nGender: {user.get('gender', 'N/A')}\nDOB: {dob_str}\nPhone: {phone}\nInterests: {', '.join(user.get('interests', []))}"
        await send_message(chat_id, profile, create_main_menu_keyboard())
        await safe_clear_markup(chat_id, message_id)
        return

    if data == "menu:discounts" and user:
        user_categories = [i for i in user.get("interests", []) if i in CATEGORIES]
        if not user_categories:
            await send_message(chat_id, "No discount categories selected in your interests.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        await edit_message_keyboard(chat_id, message_id, create_categories_keyboard(user_categories))
        return

    if data.startswith("discount_category:") and user:
        category = data[len("discount_category:"):]
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

    if data.startswith("business_profile:"):
        business_id = data[17:]
        business = await supabase_find_business(business_id)
        if not business:
            await send_message(chat_id, "Business not found.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        text = f"Business Profile\nName: {business.get('name', 'N/A')}\nCity: {business.get('city', 'N/A')}\nDescription: {business.get('description', 'N/A')}"
        await send_message(chat_id, text, create_main_menu_keyboard())
        await safe_clear_markup(chat_id, message_id)
        return

    if data.startswith("discount_book:") or data.startswith("discount_promo:"):
        discount_id = data.split(":")[1]
        discount = await supabase_find_discount(discount_id)
        if not discount:
            await send_message(chat_id, "Discount not found.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        if user["dob"] and user["phone"]:
            try:
                code, expiry = await generate_discount_code(chat_id, discount["business_id"], discount_id)
                await award_points(user["id"], POINTS_CLAIM_PROMO, f"discount_claimed:{discount_id}")
                business = await supabase_find_business(discount["business_id"])
                await send_message(chat_id, f"Claimed {discount['name']} at {business.get('name', 'Unknown')}!\nCode: {code}\nExpires: {expiry[:10]}", create_main_menu_keyboard())
            except Exception as e:
                await send_message(chat_id, str(e), create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
        else:
            await safe_clear_markup(chat_id, message_id)
            state = {"last_updated": now_iso(), "action_after": f"claim_discount:{discount_id}"}
            if not user["dob"]:
                state["state"] = "awaiting_dob"
                await send_message(chat_id, "To claim this discount, please provide your date of birth (DD.MM.YYYY).")
            else:
                state["state"] = "awaiting_phone"
                await send_message(chat_id, "To claim this discount, please share your phone number:", create_phone_keyboard())
            USER_STATES[chat_id] = state
        return

    if data == "menu:giveaways" and user:
        def _q_giveaways():
            return supabase.table("giveaways").select("*").eq("active", True).execute()
        resp = await asyncio.to_thread(_q_giveaways)
        giveaways = getattr(resp, "data", [])
        if not giveaways:
            await send_message(chat_id, "No active giveaways available.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        keyboard = {
            "inline_keyboard": [[{"text": g["name"], "callback_data": f"giveaway:{g['id']}"}] for g in giveaways]
        }
        await edit_message_keyboard(chat_id, message_id, keyboard)
        return

    if data.startswith("giveaway:") and user:
        giveaway_id = data[len("giveaway:"):]
        giveaway = await supabase_find_giveaway(giveaway_id)
        if not giveaway:
            await send_message(chat_id, "Giveaway not found.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        if user["dob"] and user["phone"]:
            try:
                code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "awaiting_booking")
                await award_points(user["id"], POINTS_BOOKING_CREATED, f"giveaway_joined:{giveaway_id}")
                business = await supabase_find_business(giveaway["business_id"])
                await send_message(chat_id, f"Entered {giveaway['name']} at {business.get('name', 'Unknown')}!\nCode: {code}\nExpires: {expiry[:10]}", create_main_menu_keyboard())
            except Exception as e:
                await send_message(chat_id, str(e), create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
        else:
            await safe_clear_markup(chat_id, message_id)
            state = {"last_updated": now_iso(), "action_after": f"claim_giveaway:{giveaway_id}"}
            if not user["dob"]:
                state["state"] = "awaiting_dob"
                await send_message(chat_id, "To enter this giveaway, please provide your date of birth (DD.MM.YYYY).")
            else:
                state["state"] = "awaiting_phone"
                await send_message(chat_id, "To enter this giveaway, please share your phone number:", create_phone_keyboard())
            USER_STATES[chat_id] = state
        return

    if data == "menu:refer" and user:
        await send_message(chat_id, f"Share your ID to refer friends: {chat_id}\nEnter a friend's ID to link a referral:", create_main_menu_keyboard())
        USER_STATES[chat_id] = {"state": "awaiting_referral_id", "last_updated": now_iso()}
        await safe_clear_markup(chat_id, message_id)
        return

    # Handle admin approve/reject callbacks if you want - central_bot handles admin flows in webhook handler typically
    await safe_clear_markup(chat_id, message_id)
