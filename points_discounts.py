import uuid
from typing import Dict, Any, Optional
from datetime import datetime, timedelta, timezone
import random
import logging
from utils import (
    send_message, supabase_find_business, supabase_find_discount, supabase_find_giveaway,
    supabase_insert_return, supabase_update_by_id_return, now_iso, create_categories_keyboard,
    CATEGORIES, ADMIN_CHAT_ID
)

logger = logging.getLogger(__name__)

async def has_redeemed_discount(chat_id: int, supabase) -> bool:
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

async def generate_discount_code(chat_id: int, business_id: str, discount_id: str, supabase) -> tuple[str, str]:
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

async def generate_promo_code(chat_id: int, business_id: str, giveaway_id: str, discount_type: str, supabase) -> tuple[str, str]:
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

async def notify_users(giveaway_id: str, supabase):
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

async def handle_points_and_discounts(chat_id: int, callback_data: str, message_id: int, registered: Dict[str, Any]):
    from utils import supabase, create_main_menu_keyboard, safe_clear_markup, edit_message_keyboard
    if callback_data == "menu:points":
        points = registered.get("points", 0)
        await send_message(chat_id, f"Your balance: *{points} points*")
    elif callback_data == "menu:profile":
        if not registered.get("phone_number"):
            await send_message(chat_id, "Please share your phone number to complete your profile:", reply_markup=create_phone_keyboard())
            set_state(chat_id, {"stage": "awaiting_phone_profile", "data": registered, "entry_id": registered["id"]})
            return
        if not registered.get("dob"):
            await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:")
            set_state(chat_id, {"stage": "awaiting_dob_profile", "data": registered, "entry_id": registered["id"]})
            return
        interests = registered.get("interests", []) or []
        interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
        await send_message(chat_id, f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}")
    elif callback_data == "menu:discounts":
        if not registered.get("phone_number") or not registered.get("dob"):
            await send_message(chat_id, "Complete your profile to access discounts:", reply_markup=create_phone_keyboard())
            set_state(chat_id, {"stage": "awaiting_phone_profile", "data": registered, "entry_id": registered["id"]})
            return
        interests = registered.get("interests", []) or []
        if not interests:
            await send_message(chat_id, "No interests set. Please update your profile.")
            return
        await send_message(chat_id, "Choose a category for discounts:", reply_markup=create_categories_keyboard())
    elif callback_data == "menu:giveaways":
        try:
            if not await has_redeemed_discount(chat_id, supabase):
                await send_message(chat_id, "Claim a discount first to unlock giveaways. Check Discounts:", reply_markup=create_main_menu_keyboard())
                return
            interests = registered.get("interests", []) or []
            if not interests:
                await send_message(chat_id, "No interests set. Please update your profile.")
                return
            def _query_giveaways():
                return supabase.table("giveaways").select("*").in_("category", interests).eq("active", True).eq("business_type", "salon").execute()
            resp = await asyncio.to_thread(_query_giveaways)
            giveaways = resp.data if hasattr(resp, "data") else resp.get("data", [])
            if not giveaways:
                await send_message(chat_id, "No giveaways available for your interests. Check Discover Offers:", reply_markup=create_main_menu_keyboard())
                return
            for g in giveaways:
                business_type = g.get("business_type", "salon").capitalize()
                cost = g.get("cost", 200)
                message = f"{business_type}: *{g['name']}* at {g.get('salon_name')} ({g.get('category')})"
                keyboard = {"inline_keyboard": [
                    [{"text": f"Join ({cost} pts)", "callback_data": f"giveaway_points:{g['id']}"}, {"text": "Join via Booking", "callback_data": f"giveaway_book:{g['id']}"}]
                ]}
                await send_message(chat_id, message, keyboard)
        except Exception as e:
            logger.error(f"Failed to fetch giveaways for chat_id {chat_id}: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to load giveaways. Please try again later.")
    elif callback_data.startswith("discount_category:"):
        category = callback_data[len("discount_category:"):]
        if category not in CATEGORIES:
            await send_message(chat_id, "Invalid category.")
            return
        try:
            def _query_discounts():
                return supabase.table("discounts").select("*").eq("category", category).eq("active", True).execute()
            resp = await asyncio.to_thread(_query_discounts)
            discounts = resp.data if hasattr(resp, "data") else resp.get("data", [])
            if not discounts:
                await send_message(chat_id, f"No discounts available in {category}.")
                return
            for d in discounts:
                business = await supabase_find_business(d["business_id"])
                location = business["location"] if business else "Unknown"
                message = f"*{d['name']}*\n{d['discount_percentage']}% off on {d['category']}\nAt {business['name'] if business else 'Unknown'}, {location}"
                keyboard = {"inline_keyboard": [
                    [{"text": "View Profile", "callback_data": f"profile:{d['business_id']}"}, {"text": "View Services", "callback_data": f"services:{d['business_id']}"},],
                    [{"text": "Book", "callback_data": f"book:{d['business_id']}"}, {"text": "Get Discount", "callback_data": f"get_discount:{d['id']}"}]
                ]}
                await send_message(chat_id, message, keyboard)
        except Exception as e:
            logger.error(f"Failed to fetch discounts for category {category}, chat_id {chat_id}: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to load discounts. Please try again later.")
    elif callback_data.startswith("profile:"):
        business_id = callback_data[len("profile:"):]
        try:
            business = await supabase_find_business(business_id)
            if not business:
                await send_message(chat_id, "Business not found.")
                return
            msg = f"Business Profile:\nName: {business['name']}\nCategory: {business['category']}\nLocation: {business['location']}\nPhone: {business['phone_number']}\nWork Days: {', '.join(business['work_days'])}"
            await send_message(chat_id, msg)
        except Exception as e:
            logger.error(f"Failed to fetch business profile {business_id}: {str(e)}")
            await send_message(chat_id, "Failed to load profile.")
    elif callback_data.startswith("services:"):
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
            logger.error(f"Failed to fetch business services {business_id}: {str(e)}")
            await send_message(chat_id, "Failed to load services.")
    elif callback_data.startswith("book:"):
        business_id = callback_data[len("book:"):]
        try:
            business = await supabase_find_business(business_id)
            if not business:
                await send_message(chat_id, "Business not found.")
                return
            msg = f"To book, please contact {business['name']} at {business['phone_number']}."
            await send_message(chat_id, msg)
        except Exception as e:
            logger.error(f"Failed to fetch book info {business_id}: {str(e)}")
            await send_message(chat_id, "Failed to load booking info.")
    elif callback_data.startswith("get_discount:"):
        discount_id = callback_data[len("get_discount:"):]
        try:
            uuid.UUID(discount_id)
            discount = await supabase_find_discount(discount_id)
            if not discount or not discount["active"]:
                await send_message(chat_id, "Discount not found or inactive.")
                return
            if not discount.get("business_id"):
                logger.error(f"Missing business_id for discount_id: {discount_id}")
                await send_message(chat_id, "Sorry, this discount is unavailable due to a configuration issue. Please try another.")
                return
            code, expiry = await generate_discount_code(chat_id, discount["business_id"], discount_id, supabase)
            await send_message(chat_id, f"Your promo code: *{code}* for {discount['name']}. Valid until {expiry.split('T')[0]}.")
        except ValueError as ve:
            await send_message(chat_id, str(ve))
        except Exception as e:
            logger.error(f"Failed to generate discount code for discount_id: {discount_id}, chat_id: {chat_id}: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to generate promo code. Please try again later.")
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
            await supabase_update_by_id_return("central_bot_leads", registered["id"], {"points": registered["points"] - cost})
            code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "loser", supabase)
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
            code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "awaiting_booking", supabase)
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
    elif chat_id == int(ADMIN_CHAT_ID) and callback_data.startswith("giveaway_approve:"):
        giveaway_id = callback_data[len("giveaway_approve:"):]
        try:
            uuid.UUID(giveaway_id)
            giveaway = await supabase_find_giveaway(giveaway_id)
            if not giveaway:
                await send_message(chat_id, f"Giveaway with ID {giveaway_id} not found.")
                await safe_clear_markup(chat_id, message_id)
                return
            def _update_giveaway():
                return supabase.table("giveaways").update({"active": True, "updated_at": now_iso()}).eq("id", giveaway_id).execute()
            await asyncio.to_thread(_update_giveaway)
            await send_message(chat_id, f"Approved {giveaway['business_type']}: {giveaway['name']}.")
            business = await supabase_find_business(giveaway["business_id"])
            await send_message(business["telegram_id"], f"Your {giveaway['business_type']} '{giveaway['name']}' is approved and live!")
            await notify_users(giveaway_id, supabase)
            await safe_clear_markup(chat_id, message_id)
        except ValueError:
            await send_message(chat_id, f"Invalid giveaway ID: {giveaway_id}")
        except Exception as e:
            logger.error(f"Failed to approve giveaway {giveaway_id}: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to approve giveaway. Please try again.")
    elif chat_id == int(ADMIN_CHAT_ID) and callback_data.startswith("giveaway_reject:"):
        giveaway_id = callback_data[len("giveaway_reject:"):]
        try:
            uuid.UUID(giveaway_id)
            giveaway = await supabase_find_giveaway(giveaway_id)
            if not giveaway:
                await send_message(chat_id, f"Giveaway with ID {giveaway_id} not found.")
                await safe_clear_markup(chat_id, message_id)
                return
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
