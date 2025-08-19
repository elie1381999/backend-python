import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from central_bot import (
    send_message, create_menu_options_keyboard, create_language_keyboard, create_gender_keyboard,
    create_interests_keyboard, create_main_menu_keyboard, create_categories_keyboard,
    create_phone_keyboard, supabase_find_draft, supabase_find_registered,
    supabase_update_by_id_return, supabase_insert_return, award_points, has_history,
    generate_discount_code, generate_promo_code, has_redeemed_discount,
    supabase_find_business, supabase_find_discount, supabase_find_giveaway,
    now_iso, STARTER_POINTS, POINTS_PROFILE_COMPLETE, POINTS_CLAIM_PROMO,
    POINTS_BOOKING_CREATED, POINTS_REFERRAL_JOIN, EMOJIS, INTERESTS
)

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# In-memory state for conversation
CONVO_STATES: Dict[int, Dict[str, Any]] = {}
STATE_TTL_SECONDS = 30 * 60  # 30 minutes

def set_convo_state(chat_id: int, state: Dict[str, Any]):
    """Store conversation state with timestamp."""
    state["updated_at"] = now_iso()
    CONVO_STATES[chat_id] = state

def get_convo_state(chat_id: int) -> Optional[Dict[str, Any]]:
    """Retrieve conversation state, expiring after TTL."""
    st = CONVO_STATES.get(chat_id)
    if not st:
        return None
    try:
        updated = datetime.fromisoformat(st.get("updated_at"))
        if (datetime.now(timezone.utc) - updated).total_seconds() > STATE_TTL_SECONDS:
            CONVO_STATES.pop(chat_id, None)
            return None
    except Exception:
        CONVO_STATES.pop(chat_id, None)
        return None
    return st

async def render_welcome_message(chat_id: int, referral_id: Optional[str] = None):
    """Render the welcome message and language selection."""
    registered = await supabase_find_registered(chat_id)
    if registered:
        await send_message(chat_id, "You're already registered! Explore options:", reply_markup=create_main_menu_keyboard())
        return
    existing = await supabase_find_draft(chat_id)
    state = {
        "stage": "awaiting_gender" if existing else "awaiting_language",
        "data": {"language": existing.get("language") if existing else None},
        "entry_id": existing.get("id") if existing else None,
        "selected_interests": []
    }
    if referral_id:
        state["referred_by"] = referral_id
    if existing:
        await send_message(chat_id, "What's your gender? (optional, helps target offers)", reply_markup=create_gender_keyboard())
    else:
        await send_message(chat_id, "Welcome! Choose your language:", reply_markup=create_language_keyboard())
    set_convo_state(chat_id, state)

async def handle_language_selection(chat_id: int, message_id: int, language: str, state: Dict[str, Any]):
    """Handle language selection and proceed to next step."""
    if language not in ["en", "ru"]:
        await send_message(chat_id, "Invalid language:", reply_markup=create_language_keyboard())
        return
    state["data"]["language"] = language
    entry_id = state.get("entry_id")
    if not entry_id:
        created = await supabase_insert_return("central_bot_leads", {"telegram_id": chat_id, "language": language, "is_draft": True})
        state["entry_id"] = created.get("id") if created else None
    else:
        await supabase_update_by_id_return("central_bot_leads", entry_id, {"language": language})
    if state.get("stage") == "awaiting_language":
        await send_message(chat_id, "What's your gender? (optional, helps target offers)", reply_markup=create_gender_keyboard())
        state["stage"] = "awaiting_gender"
    else:
        await send_message(chat_id, "Language updated! Explore options:", reply_markup=create_main_menu_keyboard())
        if chat_id in CONVO_STATES:
            del CONVO_STATES[chat_id]
    set_convo_state(chat_id, state)

async def handle_gender_selection(chat_id: int, message_id: int, gender: str, state: Dict[str, Any]):
    """Handle gender selection and proceed to DOB."""
    if gender not in ["female", "male"]:
        await send_message(chat_id, "Invalid gender:", reply_markup=create_gender_keyboard())
        return
    state["data"]["gender"] = gender
    entry_id = state.get("entry_id")
    if entry_id:
        await supabase_update_by_id_return("central_bot_leads", entry_id, {"gender": gender})
    await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:")
    state["stage"] = "awaiting_dob"
    set_convo_state(chat_id, state)

async def handle_dob_input(chat_id: int, text: str, state: Dict[str, Any]):
    """Handle DOB input and proceed to interests."""
    if text.lower() == "/skip":
        state["data"]["dob"] = None
        entry_id = state.get("entry_id")
        if entry_id:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": None})
        state["stage"] = "awaiting_interests"
        await send_message(chat_id, "Choose exactly 3 interests for this month:", reply_markup=create_interests_keyboard())
        set_convo_state(chat_id, state)
        return
    try:
        dob_obj = datetime.strptime(text, "%Y-%m-%d").date()
        if dob_obj.year < 1900 or dob_obj > datetime.now().date():
            await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.")
            return
        state["data"]["dob"] = dob_obj.isoformat()
        entry_id = state.get("entry_id")
        if entry_id:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": state["data"]["dob"]})
        state["stage"] = "awaiting_interests"
        await send_message(chat_id, "Choose exactly 3 interests for this month:", reply_markup=create_interests_keyboard())
        set_convo_state(chat_id, state)
    except ValueError:
        await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.")

async def handle_interests_selection(chat_id: int, message_id: int, interest: str, state: Dict[str, Any]):
    """Handle interest selection and update keyboard."""
    if interest not in INTERESTS:
        logger.warning(f"Invalid interest selected: {interest}")
        return
    selected = state.get("selected_interests", [])
    if interest in selected:
        selected.remove(interest)
    elif len(selected) < 3:
        selected.append(interest)
    state["selected_interests"] = selected
    await edit_message_keyboard(chat_id, message_id, create_interests_keyboard(selected))
    set_convo_state(chat_id, state)

async def finalize_interests(chat_id: int, state: Dict[str, Any]):
    """Finalize interest selection and complete registration."""
    selected = state.get("selected_interests", [])
    if len(selected) != 3:
        await send_message(chat_id, f"Please select exactly 3 interests (currently {len(selected)}):", reply_markup=create_interests_keyboard(selected))
        return
    await send_message(chat_id, "Interests saved! Finalizing registration...")
    entry_id = state.get("entry_id")
    if entry_id:
        await supabase_update_by_id_return("central_bot_leads", entry_id, {
            "interests": selected,
            "is_draft": False
        })
        try:
            signup_reason = "signup"
            if not await has_history(entry_id, signup_reason):
                await award_points(entry_id, STARTER_POINTS, signup_reason)
            referred_by = state.get("referred_by")
            if referred_by:
                def _check_referrer():
                    return supabase.table("central_bot_leads").select("id").eq("id", referred_by).limit(1).execute()
                resp = await asyncio.to_thread(_check_referrer)
                if resp.data:
                    await supabase_update_by_id_return("central_bot_leads", entry_id, {"referred_by": referred_by})
                    ref_join_reason = f"referral_join:{entry_id}"
                    if not await has_history(referred_by, ref_join_reason):
                        await award_points(referred_by, POINTS_REFERRAL_JOIN, ref_join_reason)
                else:
                    logger.debug(f"Referrer {referred_by} does not exist; skipping referral award")
        except Exception:
            logger.exception("Failed awarding signup or referral points")
    await send_message(chat_id, f"Congrats! You've earned {STARTER_POINTS} points. Explore options:", reply_markup=create_main_menu_keyboard())
    if chat_id in CONVO_STATES:
        del CONVO_STATES[chat_id]

async def handle_phone_input(chat_id: int, phone_number: str, state: Dict[str, Any]):
    """Handle phone number input for profile completion."""
    state["data"]["phone_number"] = phone_number
    entry_id = state.get("entry_id")
    if entry_id:
        await supabase_update_by_id_return("central_bot_leads", entry_id, {"phone_number": phone_number})
    registered = await supabase_find_registered(chat_id)
    try:
        if registered and registered.get("dob") and registered.get("phone_number"):
            profile_reason = "profile_complete"
            if not await has_history(registered["id"], profile_reason):
                await award_points(registered["id"], POINTS_PROFILE_COMPLETE, profile_reason)
    except Exception:
        logger.exception("Failed during profile completion points flow")
    if registered and not registered.get("dob"):
        await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:")
        state["stage"] = "awaiting_dob_profile"
    else:
        interests = registered.get("interests", []) or []
        interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
        await send_message(chat_id, f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}")
        if chat_id in CONVO_STATES:
            del CONVO_STATES[chat_id]
    set_convo_state(chat_id, state)

async def handle_profile_dob(chat_id: int, text: str, state: Dict[str, Any]):
    """Handle DOB input during profile update."""
    if text.lower() == "/skip":
        state["data"]["dob"] = None
        entry_id = state.get("entry_id")
        if entry_id:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": None})
        registered = await supabase_find_registered(chat_id)
        interests = registered.get("interests", []) or []
        interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
        await send_message(chat_id, f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}")
        if chat_id in CONVO_STATES:
            del CONVO_STATES[chat_id]
        return
    try:
        dob_obj = datetime.strptime(text, "%Y-%m-%d").date()
        if dob_obj.year < 1900 or dob_obj > datetime.now().date():
            await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.")
            return
        state["data"]["dob"] = dob_obj.isoformat()
        entry_id = state.get("entry_id")
        if entry_id:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": state["data"]["dob"]})
        registered = await supabase_find_registered(chat_id)
        try:
            if registered and registered.get("phone_number") and registered.get("dob"):
                profile_reason = "profile_complete"
                if not await has_history(registered["id"], profile_reason):
                    await award_points(registered["id"], POINTS_PROFILE_COMPLETE, profile_reason)
        except Exception:
            logger.exception("Failed awarding profile_complete after dob update")
        interests = registered.get("interests", []) or []
        interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
        await send_message(chat_id, f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}")
        if chat_id in CONVO_STATES:
            del CONVO_STATES[chat_id]
    except ValueError:
        await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.")

async def handle_message(chat_id: int, message: Dict[str, Any]):
    """Process incoming messages for the conversation UI."""
    text = (message.get("text") or "").strip()
    contact = message.get("contact")
    state = get_convo_state(chat_id) or {}

    if text.lower() == "/myid":
        await send_message(chat_id, f"Your Telegram ID: {chat_id}")
        return

    if text.lower() == "/menu":
        await send_message(chat_id, "Choose an option:", reply_markup=create_menu_options_keyboard())
        return

    if text.lower().startswith("/start"):
        referral_id = None
        if text.lower() != "/start":
            try:
                from uuid import UUID
                referral_id = str(UUID(text[len("/start "):]))
            except ValueError:
                logger.error(f"Invalid referral ID: {text[len('/start '):]}")
        await render_welcome_message(chat_id, referral_id)
        return

    if contact and state.get("stage") == "awaiting_phone_profile":
        phone_number = contact.get("phone_number")
        if not phone_number:
            await send_message(chat_id, "Invalid phone number. Please try again:", reply_markup=create_phone_keyboard())
            return
        await handle_phone_input(chat_id, phone_number, state)
        return

    if state.get("stage") == "awaiting_dob":
        await handle_dob_input(chat_id, text, state)
        return

    if state.get("stage") == "awaiting_dob_profile":
        await handle_profile_dob(chat_id, text, state)
        return

async def handle_callback(chat_id: int, callback_query: Dict[str, Any]):
    """Process callback queries for the conversation UI."""
    callback_data = callback_query.get("data")
    message_id = callback_query.get("message", {}).get("message_id")
    if not callback_data or not message_id:
        logger.error(f"Invalid callback query: callback_data={callback_data}, message_id={message_id}")
        return

    state = get_convo_state(chat_id) or {}
    registered = await supabase_find_registered(chat_id)

    if callback_data == "menu:main":
        await send_message(chat_id, "Explore options:", reply_markup=create_main_menu_keyboard())
        return

    if callback_data == "menu:language":
        await send_message(chat_id, "Choose your language:", reply_markup=create_language_keyboard())
        state["stage"] = "awaiting_language_change"
        set_convo_state(chat_id, state)
        return

    if callback_data == "menu:refer" and registered:
        from os import getenv
        user_id = registered["id"]
        referral_link = f"t.me/{getenv('BOT_USERNAME')}?start={user_id}"
        await send_message(chat_id, f"Share this link to refer friends: {referral_link}\nThey'll get starter points, and you'll get {POINTS_REFERRAL_JOIN} points when they join, plus more when they complete a verified booking!")
        return

    if callback_data.startswith("lang:") and state.get("stage") in ["awaiting_language", "awaiting_language_change"]:
        language = callback_data[len("lang:"):]
        await handle_language_selection(chat_id, message_id, language, state)
        return

    if callback_data.startswith("gender:") and state.get("stage") == "awaiting_gender":
        gender = callback_data[len("gender:"):]
        await handle_gender_selection(chat_id, message_id, gender, state)
        return

    if callback_data.startswith("interest:") and state.get("stage") == "awaiting_interests":
        interest = callback_data[len("interest:"):]
        await handle_interests_selection(chat_id, message_id, interest, state)
        return

    if callback_data == "interests_done" and state.get("stage") == "awaiting_interests":
        await finalize_interests(chat_id, state)
        return

    if registered:
        if callback_data == "menu:points":
            points = registered.get("points", 0)
            await send_message(chat_id, f"Your balance: *{points} points*")
            return

        if callback_data == "menu:profile":
            if not registered.get("phone_number"):
                await send_message(chat_id, "Please share your phone number to complete your profile:", reply_markup=create_phone_keyboard())
                state["stage"] = "awaiting_phone_profile"
                state["data"] = registered
                state["entry_id"] = registered["id"]
                set_convo_state(chat_id, state)
                return
            if not registered.get("dob"):
                await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:")
                state["stage"] = "awaiting_dob_profile"
                state["data"] = registered
                state["entry_id"] = registered["id"]
                set_convo_state(chat_id, state)
                return
            interests = registered.get("interests", []) or []
            interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
            await send_message(chat_id, f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}")
            return

        if callback_data == "menu:discounts":
            if not registered.get("phone_number") or not registered.get("dob"):
                await send_message(chat_id, "Complete your profile to access discounts:", reply_markup=create_phone_keyboard())
                state["stage"] = "awaiting_phone_profile"
                state["data"] = registered
                state["entry_id"] = registered["id"]
                set_convo_state(chat_id, state)
                return
            interests = registered.get("interests", []) or []
            if not interests:
                await send_message(chat_id, "No interests set. Please update your profile.")
                return
            await send_message(chat_id, "Choose a category for discounts:", reply_markup=create_categories_keyboard())
            return

        if callback_data == "menu:giveaways":
            if not await has_redeemed_discount(chat_id):
                await send_message(chat_id, "Claim a discount first to unlock giveaways. Check Discounts:", reply_markup=create_main_menu_keyboard())
                return
            interests = registered.get("interests", []) or []
            if not interests:
                await send_message(chat_id, "No interests set. Please update your profile.")
                return
            def _query_giveaways():
                return supabase.table("giveaways").select("*").in_("category", interests).eq("active", True).eq("business_type", "giveaway").execute()
            try:
                resp = await asyncio.to_thread(_query_giveaways)
                giveaways = resp.data if hasattr(resp, "data") else resp.get("data", [])
                if not giveaways:
                    await send_message(chat_id, "No giveaways available for your interests. Check Discover Offers:", reply_markup=create_main_menu_keyboard())
                    return
                for g in giveaways:
                    business_type = g.get("business_type", "salon").capitalize()
                    cost = g.get("cost", 200)
                    message = f"{business_type}: *{g['name']}* at {g.get('salon_name', 'Unknown')} ({g.get('category')})"
                    keyboard = {"inline_keyboard": [
                        [{"text": f"Join ({cost} pts)", "callback_data": f"giveaway_points:{g['id']}"}],
                        [{"text": "Join via Booking", "callback_data": f"giveaway_book:{g['id']}"}]
                    ]}
                    await send_message(chat_id, message, keyboard)
            except Exception as e:
                logger.error(f"Failed to fetch giveaways for chat_id {chat_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to load giveaways. Please try again later.")
            return

        if callback_data.startswith("discount_category:"):
            from central_bot import CATEGORIES
            category = callback_data[len("discount_category:"):]
            if category not in CATEGORIES:
                await send_message(chat_id, "Invalid category.")
                return
            try:
                def _query_discounts():
                    return supabase.table("discounts").select("id, name, discount_percentage, category, business_id").eq("category", category).eq("active", True).execute()
                resp = await asyncio.to_thread(_query_discounts)
                discounts = resp.data if hasattr(resp, "data") else resp.get("data", [])
                if not discounts:
                    await send_message(chat_id, f"No discounts available in *{category}*.")
                    return
                for d in discounts:
                    business = await supabase_find_business(d["business_id"])
                    if not business:
                        await send_message(chat_id, f"Business not found for discount {d['name']}.")
                        continue
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
                    view_reason = f"view_discount:{d['id']}"
                    if not await has_history(registered["id"], view_reason):
                        await award_points(registered["id"], 5, view_reason)
            except Exception as e:
                logger.error(f"Failed to fetch discounts for category {category}, chat_id {chat_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to load discounts. Please try again later.")
            return

        if callback_data.startswith("profile:"):
            business_id = callback_data[len("profile:"):]
            try:
                business = await supabase_find_business(business_id)
                if not business:
                    await send_message(chat_id, "Business not found.")
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
                await send_message(chat_id, msg)
            except Exception as e:
                logger.error(f"Failed to fetch business profile {business_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to load profile.")
            return

        if callback_data.startswith("services:"):
            business_id = callback_data[len("services:"):]
            try:
                business = await supabase_find_business(business_id)
                if not business:
                    await send_message(chat_id, "Business not found.")
                    return
                prices = business.get("prices", {})
                msg = "Services:\n" + "\n".join(f"{k}: {v}" for k, v in prices.items()) if prices else "No services listed."
                await send_message(chat_id, msg)
            except Exception as e:
                logger.error(f"Failed to fetch business services {business_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to load services.")
            return

        if callback_data.startswith("book:"):
            business_id = callback_data[len("book:"):]
            try:
                business = await supabase_find_business(business_id)
                if not business:
                    await send_message(chat_id, "Business not found.")
                    return
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
                        booking_created_reason = f"booking_created:{created_booking['id']}"
                        if not await has_history(registered["id"], booking_created_reason):
                            await award_points(registered["id"], POINTS_BOOKING_CREATED, booking_created_reason, created_booking["id"])
                        await send_message(chat_id, f"Booking request created (ref: {created_booking['id']}). To confirm, contact {business['name']} at {business.get('phone_number', 'Not set')}.")
                    else:
                        await send_message(chat_id, f"Booking: Please contact {business['name']} at {business.get('phone_number', 'Not set')}.")
                except Exception:
                    logger.exception("Failed to create booking in DB")
                    await send_message(chat_id, f"Booking: Please contact {business['name']} at {business.get('phone_number', 'Not set')}.")
            except Exception as e:
                logger.error(f"Failed to fetch book info {business_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to load booking info.")
            return

        if callback_data.startswith("get_discount:"):
            from uuid import UUID
            discount_id = callback_data[len("get_discount:"):]
            try:
                UUID(discount_id)
                discount = await supabase_find_discount(discount_id)
                if not discount or not discount["active"]:
                    await send_message(chat_id, "Discount not found or inactive.")
                    return
                if not discount.get("business_id"):
                    logger.error(f"Missing business_id for discount_id: {discount_id}")
                    await send_message(chat_id, "Sorry, this discount is unavailable due to a configuration issue. Please try another.")
                    return
                code, expiry = await generate_discount_code(chat_id, discount["business_id"], discount_id)
                await send_message(chat_id, f"Your promo code: *{code}* for {discount['name']}. Valid until {expiry.split('T')[0]}.")
                try:
                    claim_reason = f"claim_promo:{discount_id}"
                    if not await has_history(registered["id"], claim_reason):
                        await award_points(registered["id"], POINTS_CLAIM_PROMO, claim_reason)
                except Exception:
                    logger.exception("Failed to award claim promo points")
            except ValueError as ve:
                await send_message(chat_id, str(ve))
            except Exception as e:
                logger.error(f"Failed to generate discount code for discount_id: {discount_id}, chat_id: {chat_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to generate promo code. Please try again later.")
            return

        if callback_data.startswith("giveaway_points:"):
            from uuid import UUID
            giveaway_id = callback_data[len("giveaway_points:"):]
            try:
                UUID(giveaway_id)
                def _query_giveaway():
                    return supabase.table("giveaways").select("*").eq("id", giveaway_id).eq("active", True).limit(1).execute()
                resp = await asyncio.to_thread(_query_giveaway)
                giveaway = resp.data[0] if resp.data else None
                if not giveaway:
                    await send_message(chat_id, "Giveaway not found or inactive.")
                    return
                if not giveaway.get("business_id"):
                    logger.error(f"Missing business_id for giveaway_id: {giveaway_id}")
                    await send_message(chat_id, "Sorry, this giveaway is unavailable due to a configuration issue. Please try another.")
                    return
                cost = giveaway.get("cost", 200)
                if registered.get("points", 0) < cost:
                    await send_message(chat_id, f"Not enough points (need {cost}).")
                    return
                def _check_existing():
                    current_month = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    return supabase.table("user_giveaways").select("id").eq("telegram_id", chat_id).eq("giveaway_id", giveaway_id).gte("joined_at", current_month.isoformat()).execute()
                resp = await asyncio.to_thread(_check_existing)
                existing = resp.data if hasattr(resp, "data") else resp.get("data", [])
                if existing:
                    await send_message(chat_id, "You've already joined this giveaway this month.")
                    return
                await award_points(registered["id"], -cost, f"join_giveaway_points:{giveaway_id}")
                code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "loser")
                business_type = giveaway.get("business_type", "salon").capitalize()
                await send_message(chat_id, f"Joined {business_type} {giveaway['name']} with {cost} points. Your 20% loser discount code: *{code}*, valid until {expiry.split('T')[0]}.")
            except ValueError:
                logger.error(f"Invalid giveaway_id format: {giveaway_id}")
                await send_message(chat_id, "Invalid giveaway ID.")
            except Exception as e:
                logger.error(f"Failed to process giveaway_points for giveaway_id: {giveaway_id}, chat_id: {chat_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to join giveaway. Please try again later.")
            return

        if callback_data.startswith("giveaway_book:"):
            from uuid import UUID
            giveaway_id = callback_data[len("giveaway_book:"):]
            try:
                UUID(giveaway_id)
                def _query_giveaway():
                    return supabase.table("giveaways").select("*").eq("id", giveaway_id).eq("active", True).limit(1).execute()
                resp = await asyncio.to_thread(_query_giveaway)
                giveaway = resp.data[0] if resp.data else None
                if not giveaway:
                    await send_message(chat_id, "Giveaway not found or inactive.")
                    return
                if not giveaway.get("business_id"):
                    logger.error(f"Missing business_id for giveaway_id: {giveaway_id}")
                    await send_message(chat_id, "Sorry, this giveaway is unavailable due to a configuration issue. Please try another.")
                    return
                def _check_existing():
                    current_month = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    return supabase.table("user_giveaways").select("id").eq("telegram_id", chat_id).eq("giveaway_id", giveaway_id).gte("joined_at", current_month.isoformat()).execute()
                resp = await asyncio.to_thread(_check_existing)
                existing = resp.data if hasattr(resp, "data") else resp.get("data", [])
                if existing:
                    await send_message(chat_id, "You've already joined this giveaway this month.")
                    return
                code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "awaiting_booking")
                business_type = giveaway.get("business_type", "salon").capitalize()
                await send_message(chat_id, f"Book a service at {business_type} {giveaway.get('salon_name', 'Unknown')} with code *{code}* to join {giveaway['name']}. Valid until {expiry.split('T')[0]}.")
            except ValueError:
                logger.error(f"Invalid giveaway_id format: {giveaway_id}")
                await send_message(chat_id, "Invalid giveaway ID.")
            except Exception as e:
                logger.error(f"Failed to process giveaway_book for giveaway_id: {giveaway_id}, chat_id: {chat_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to join giveaway. Please try again later.")
            return
