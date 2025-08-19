import re
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
import logging
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
    create_phone_keyboard
)
from central_bot import (
    now_iso,
    supabase_find_draft,
    supabase_find_registered,
    supabase_insert_return,
    supabase_update_by_id_return,
    award_points,
    generate_discount_code,
    generate_promo_code,
    has_redeemed_discount,
    supabase_find_discount,
    supabase_find_giveaway,
    supabase_find_business,
    STARTER_POINTS,
    POINTS_PROFILE_COMPLETE,
    POINTS_CLAIM_PROMO,
    POINTS_BOOKING_CREATED,
    POINTS_REFERRAL_JOIN,
    INTERESTS,
    CATEGORIES,
    EMOJIS,
    USER_STATES,
    STATE_TTL_SECONDS
)

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def handle_message(chat_id: int, message: Dict[str, Any]):
    text = message.get("text", "").lower()
    user = await supabase_find_registered(chat_id)
    draft = await supabase_find_draft(chat_id) if not user else None
    state = USER_STATES.get(chat_id, {})
    state_updated = False

    if text == "/start":
        if user:
            await send_message(chat_id, "Welcome back! Here's the menu:", create_main_menu_keyboard())
            return
        elif draft:
            await send_message(chat_id, "Let's continue your registration. What's your date of birth (DD.MM.YYYY)?")
            USER_STATES[chat_id] = {"state": "awaiting_dob", "draft_id": draft["id"], "last_updated": now_iso()}
            return
        else:
            await send_message(chat_id, "Welcome! Let's get started. What's your date of birth (DD.MM.YYYY)?")
            USER_STATES[chat_id] = {"state": "awaiting_dob", "last_updated": now_iso()}
            return

    if text == "/menu":
        if user:
            await send_message(chat_id, "Here's the menu:", create_main_menu_keyboard())
        else:
            await send_message(chat_id, "Please complete registration first with /start.")
        return

    if text == "/myid":
        await send_message(chat_id, f"Your Telegram ID is: {chat_id}")
        return

    if state.get("state") == "awaiting_dob":
        dob_match = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", text)
        if not dob_match:
            await send_message(chat_id, "Please enter a valid date of birth (DD.MM.YYYY).")
            return
        day, month, year = map(int, dob_match.groups())
        try:
            dob = datetime(year, month, day, tzinfo=timezone.utc).isoformat()
            if state.get("draft_id"):
                await supabase_update_by_id_return("central_bot_leads", state["draft_id"], {"dob": dob})
            else:
                payload = {
                    "telegram_id": chat_id,
                    "is_draft": True,
                    "dob": dob,
                    "created_at": now_iso(),
                    "points": STARTER_POINTS
                }
                draft = await supabase_insert_return("central_bot_leads", payload)
                if draft:
                    state["draft_id"] = draft["id"]
            state["state"] = "awaiting_gender"
            state["last_updated"] = now_iso()
            state_updated = True
            await send_message(chat_id, "Got it! What's your gender?", create_gender_keyboard())
        except ValueError:
            await send_message(chat_id, "Invalid date. Please enter a valid date of birth (DD.MM.YYYY).")
        return

    if state.get("state") == "awaiting_phone" and message.get("contact"):
        phone = message["contact"].get("phone_number")
        if not phone:
            await send_message(chat_id, "Failed to get phone number. Please try again.", create_phone_keyboard())
            return
        if state.get("draft_id"):
            draft = await supabase_update_by_id_return("central_bot_leads", state["draft_id"], {
                "phone": phone,
                "is_draft": False,
                "updated_at": now_iso()
            })
            if draft:
                await award_points(draft["id"], POINTS_PROFILE_COMPLETE, "profile_completed")
                await send_message(chat_id, f"Registration complete! You earned {POINTS_PROFILE_COMPLETE} points. Here's the menu:", create_main_menu_keyboard())
                del USER_STATES[chat_id]
            else:
                await send_message(chat_id, "Failed to save phone number. Please try again.", create_phone_keyboard())
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
        del USER_STATES[chat_id]
        return

    if user:
        await send_message(chat_id, "Please use /menu to interact or provide a valid command.")
    else:
        await send_message(chat_id, "Please start registration with /start.")

    if state_updated:
        USER_STATES[chat_id] = state

async def handle_callback(chat_id: int, callback_query: Dict[str, Any]):
    data = callback_query.get("data")
    message_id = callback_query.get("message", {}).get("message_id")
    user = await supabase_find_registered(chat_id)
    draft = await supabase_find_draft(chat_id) if not user else None
    state = USER_STATES.get(chat_id, {})

    if not data or not message_id:
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
        if state.get("draft_id"):
            await supabase_update_by_id_return("central_bot_leads", state["draft_id"], {"gender": gender})
            state["state"] = "awaiting_interests"
            state["interests"] = []
            state["last_updated"] = now_iso()
            USER_STATES[chat_id] = state
            await edit_message_keyboard(chat_id, message_id, create_interests_keyboard())
        return

    if data.startswith("interest:") and state.get("state") == "awaiting_interests":
        interest = data[len("interest:"):]
        if interest in state.get("interests", []):
            state["interests"].remove(interest)
        else:
            if len(state.get("interests", [])) < 3:
                state["interests"].append(interest)
        state["last_updated"] = now_iso()
        USER_STATES[chat_id] = state
        await edit_message_keyboard(chat_id, message_id, create_interests_keyboard(state["interests"]))
        return

    if data == "interests_done" and state.get("state") == "awaiting_interests":
        if state.get("interests"):
            if state.get("draft_id"):
                await supabase_update_by_id_return("central_bot_leads", state["draft_id"], {"interests": state["interests"]})
                state["state"] = "awaiting_phone"
                state["last_updated"] = now_iso()
                USER_STATES[chat_id] = state
                await send_message(chat_id, "Please share your phone number:", create_phone_keyboard())
                await safe_clear_markup(chat_id, message_id)
        else:
            await send_message(chat_id, "Please select at least one interest.")
        return

    if data == "menu:points" and user:
        points = user.get("points", 0)
        await send_message(chat_id, f"You have {points} points.", create_main_menu_keyboard())
        await safe_clear_markup(chat_id, message_id)
        return

    if data == "menu:profile" and user:
        profile = f"ID: {chat_id}\nLanguage: {user.get('language', 'N/A')}\nGender: {user.get('gender', 'N/A')}\nInterests: {', '.join(user.get('interests', []))}\nPhone: {user.get('phone', 'N/A')}"
        await send_message(chat_id, profile, create_main_menu_keyboard())
        await safe_clear_markup(chat_id, message_id)
        return

    if data == "menu:discounts" and user:
        await edit_message_keyboard(chat_id, message_id, create_categories_keyboard())
        return

    if data.startswith("discount_category:") and user:
        category = data[len("discount_category:"):]
        has_redeemed = await has_redeemed_discount(chat_id)
        if has_redeemed:
            await send_message(chat_id, "You've already claimed a discount this month.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        def _q_discounts():
            return supabase.table("discounts").select("*").eq("category", category).eq("active", True).execute()
        discounts = (await asyncio.to_thread(_q_discounts)).data
        if not discounts:
            await send_message(chat_id, f"No active {category} discounts available.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        keyboard = {
            "inline_keyboard": [[{"text": d["name"], "callback_data": f"discount:{d['id']}"}] for d in discounts]
        }
        await edit_message_keyboard(chat_id, message_id, keyboard)
        return

    if data.startswith("discount:") and user:
        discount_id = data[len("discount:"):]
        discount = await supabase_find_discount(discount_id)
        if not discount:
            await send_message(chat_id, "Discount not found.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        try:
            code, expiry = await generate_discount_code(chat_id, discount["business_id"], discount_id)
            await award_points(user["id"], POINTS_CLAIM_PROMO, f"discount_claimed:{discount_id}")
            business = await supabase_find_business(discount["business_id"])
            await send_message(
                chat_id,
                f"Claimed {discount['name']} at {business.get('name', 'Unknown')}!\nCode: {code}\nExpires: {expiry[:10]}",
                create_main_menu_keyboard()
            )
            await safe_clear_markup(chat_id, message_id)
        except ValueError as e:
            await send_message(chat_id, str(e), create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
        return

    if data == "menu:giveaways" and user:
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

    if data.startswith("giveaway:") and user:
        giveaway_id = data[len("giveaway:"):]
        giveaway = await supabase_find_giveaway(giveaway_id)
        if not giveaway:
            await send_message(chat_id, "Giveaway not found.", create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
            return
        try:
            code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "awaiting_booking")
            await award_points(user["id"], POINTS_BOOKING_CREATED, f"giveaway_joined:{giveaway_id}")
            business = await supabase_find_business(giveaway["business_id"])
            await send_message(
                chat_id,
                f"Entered {giveaway['name']} at {business.get('name', 'Unknown')}!\nCode: {code}\nExpires: {expiry[:10]}",
                create_main_menu_keyboard()
            )
            await safe_clear_markup(chat_id, message_id)
        except ValueError as e:
            await send_message(chat_id, str(e), create_main_menu_keyboard())
            await safe_clear_markup(chat_id, message_id)
        return

    if data == "menu:refer" and user:
        await send_message(chat_id, f"Share your ID to refer friends: {chat_id}\nEnter a friend's ID to link a referral:", create_main_menu_keyboard())
        USER_STATES[chat_id] = {"state": "awaiting_referral_id", "last_updated": now_iso()}
        await safe_clear_markup(chat_id, message_id)
        return

    await safe_clear_markup(chat_id, message_id)
