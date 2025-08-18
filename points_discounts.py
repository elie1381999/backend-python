import logging
import asyncio
from typing import Dict, Any, Optional
from supabase import Client
from utils import send_message, safe_clear_markup, create_categories_keyboard, create_main_menu_keyboard, create_phone_keyboard
import uuid
import random
from datetime import datetime, timedelta, timezone

# Logging setup
logger = logging.getLogger(__name__)

async def generate_discount_code(chat_id: int, business_id: str, discount_id: str, supabase: Client) -> tuple[str, str]:
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
        "joined_at": datetime.now(timezone.utc).isoformat()
    }
    inserted = await supabase_insert_return("user_discounts", payload, supabase)
    if not inserted:
        logger.error(f"Failed to insert discount promo code for chat_id: {chat_id}, discount_id: {discount_id}")
        raise RuntimeError("Failed to save promo code")
    logger.info(f"Generated discount promo code {code} for chat_id {chat_id}, discount_id {discount_id}")
    return code, expiry

async def supabase_insert_return(table: str, payload: dict, supabase: Client) -> Optional[Dict[str, Any]]:
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

async def supabase_find_business(business_id: str, supabase: Client) -> Optional[Dict[str, Any]]:
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

async def supabase_find_discount(discount_id: str, supabase: Client) -> Optional[Dict[str, Any]]:
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

async def handle_points_and_discounts(
    chat_id: int,
    callback_data: str,
    message_id: int,
    registered: Dict[str, Any],
    supabase: Client
):
    try:
        # Handle "My Points" menu
        if callback_data == "menu:points":
            points = registered.get("points", 0)
            await safe_clear_markup(chat_id, message_id)
            await send_message(
                chat_id,
                f"Your balance: *{points} points*",
                reply_markup=create_main_menu_keyboard()
            )
            return {"ok": True}

        # Handle "Profile" menu
        elif callback_data == "menu:profile":
            if not registered.get("phone_number"):
                await safe_clear_markup(chat_id, message_id)
                await send_message(
                    chat_id,
                    "Please share your phone number to complete your profile:",
                    reply_markup=create_phone_keyboard()
                )
                USER_STATES[chat_id] = {
                    "stage": "awaiting_phone_profile",
                    "data": registered,
                    "entry_id": registered["id"],
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }
                return {"ok": True}
            if not registered.get("dob"):
                await safe_clear_markup(chat_id, message_id)
                await send_message(
                    chat_id,
                    "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:",
                )
                USER_STATES[chat_id] = {
                    "stage": "awaiting_dob_profile",
                    "data": registered,
                    "entry_id": registered["id"],
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }
                return {"ok": True}
            interests = registered.get("interests", []) or []
            interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
            await safe_clear_markup(chat_id, message_id)
            await send_message(
                chat_id,
                f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}",
                reply_markup=create_main_menu_keyboard()
            )
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
            return {"ok": True}

        # Handle "Discounts" menu
        elif callback_data == "menu:discounts":
            if not registered.get("phone_number") or not registered.get("dob"):
                await safe_clear_markup(chat_id, message_id)
                await send_message(
                    chat_id,
                    "Complete your profile to access discounts:",
                    reply_markup=create_phone_keyboard()
                )
                USER_STATES[chat_id] = {
                    "stage": "awaiting_phone_profile",
                    "data": registered,
                    "entry_id": registered["id"],
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }
                return {"ok": True}
            interests = registered.get("interests", []) or []
            if not interests:
                await safe_clear_markup(chat_id, message_id)
                await send_message(
                    chat_id,
                    "No interests set. Please update your profile.",
                    reply_markup=create_main_menu_keyboard()
                )
                return {"ok": True}
            await safe_clear_markup(chat_id, message_id)
            await send_message(
                chat_id,
                "Choose a category for discounts:",
                reply_markup=create_categories_keyboard()
            )
            return {"ok": True}

        # Handle discount category selection
        elif callback_data.startswith("discount_category:"):
            category = callback_data[len("discount_category:"):]
            if category not in ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining"]:
                await send_message(chat_id, "Invalid category.", reply_markup=create_main_menu_keyboard())
                return {"ok": True}
            try:
                def _query_discounts():
                    return (
                        supabase.table("discounts")
                        .select("discounts.*, businesses.name AS business_name, businesses.location")
                        .eq("discounts.active", True)
                        .eq("businesses.status", "approved")
                        .eq("discounts.category", category)
                        .join("businesses", "discounts.business_id = businesses.id")
                        .execute()
                    )
                resp = await asyncio.to_thread(_query_discounts)
                discounts = resp.data if hasattr(resp, "data") else resp.get("data", [])
                if not discounts:
                    await safe_clear_markup(chat_id, message_id)
                    await send_message(
                        chat_id,
                        f"No discounts available in {category}.",
                        reply_markup=create_main_menu_keyboard()
                    )
                    return {"ok": True}
                for d in discounts:
                    message = f"*{d['name']}*\n{d['discount_percentage']}% off on {d['category']}\nAt {d['business_name']}, {d['location']}"
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
                    await send_message(chat_id, message, keyboard)
                return {"ok": True}
            except Exception as e:
                logger.error(f"Failed to fetch discounts for category {category}, chat_id {chat_id}: {str(e)}", exc_info=True)
                await safe_clear_markup(chat_id, message_id)
                await send_message(
                    chat_id,
                    "Failed to load discounts. Please try again later.",
                    reply_markup=create_main_menu_keyboard()
                )
                return {"ok": True}

        # Handle business profile
        elif callback_data.startswith("profile:"):
            business_id = callback_data[len("profile:"):]
            try:
                business = await supabase_find_business(business_id, supabase)
                if not business:
                    await safe_clear_markup(chat_id, message_id)
                    await send_message(chat_id, "Business not found.", reply_markup=create_main_menu_keyboard())
                    return {"ok": True}
                msg = f"Business Profile:\nName: {business['name']}\nCategory: {business['category']}\nLocation: {business['location']}\nPhone: {business['phone_number']}\nWork Days: {', '.join(business['work_days'])}"
                await send_message(chat_id, msg, reply_markup=create_main_menu_keyboard())
            except Exception as e:
                logger.error(f"Failed to fetch business profile {business_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to load profile.", reply_markup=create_main_menu_keyboard())
            return {"ok": True}

        # Handle business services
        elif callback_data.startswith("services:"):
            business_id = callback_data[len("services:"):]
            try:
                business = await supabase_find_business(business_id, supabase)
                if not business:
                    await safe_clear_markup(chat_id, message_id)
                    await send_message(chat_id, "Business not found.", reply_markup=create_main_menu_keyboard())
                    return {"ok": True}
                prices = business.get("prices", {})
                msg = "Services:\n" + "\n".join(f"{k}: {v}" for k, v in prices.items()) if prices else "No services listed."
                await send_message(chat_id, msg, reply_markup=create_main_menu_keyboard())
            except Exception as e:
                logger.error(f"Failed to fetch business services {business_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to load services.", reply_markup=create_main_menu_keyboard())
            return {"ok": True}

        # Handle booking
        elif callback_data.startswith("book:"):
            business_id = callback_data[len("book:"):]
            try:
                business = await supabase_find_business(business_id, supabase)
                if not business:
                    await safe_clear_markup(chat_id, message_id)
                    await send_message(chat_id, "Business not found.", reply_markup=create_main_menu_keyboard())
                    return {"ok": True}
                msg = f"To book, please contact {business['name']} at {business['phone_number']}."
                await send_message(chat_id, msg, reply_markup=create_main_menu_keyboard())
            except Exception as e:
                logger.error(f"Failed to fetch book info {business_id}: {str(e)}", exc_info=True)
                await send_message(chat_id, "Failed to load booking info.", reply_markup=create_main_menu_keyboard())
            return {"ok": True}

        # Handle get discount
        elif callback_data.startswith("get_discount:"):
            discount_id = callback_data[len("get_discount:"):]
            try:
                uuid.UUID(discount_id)
                discount = await supabase_find_discount(discount_id, supabase)
                if not discount or not discount["active"]:
                    await safe_clear_markup(chat_id, message_id)
                    await send_message(chat_id, "Discount not found or inactive.", reply_markup=create_main_menu_keyboard())
                    return {"ok": True}
                if not discount.get("business_id"):
                    logger.error(f"Missing business_id for discount_id: {discount_id}")
                    await safe_clear_markup(chat_id, message_id)
                    await send_message(
                        chat_id,
                        "Sorry, this discount is unavailable due to a configuration issue. Please try another.",
                        reply_markup=create_main_menu_keyboard()
                    )
                    return {"ok": True}
                code, expiry = await generate_discount_code(chat_id, discount["business_id"], discount_id, supabase)
                await safe_clear_markup(chat_id, message_id)
                await send_message(
                    chat_id,
                    f"Your promo code: *{code}* for {discount['name']}. Valid until {expiry.split('T')[0]}.",
                    reply_markup=create_main_menu_keyboard()
                )
            except ValueError as ve:
                await safe_clear_markup(chat_id, message_id)
                await send_message(chat_id, str(ve), reply_markup=create_main_menu_keyboard())
            except Exception as e:
                logger.error(f"Failed to generate discount code for discount_id: {discount_id}, chat_id: {chat_id}: {str(e)}", exc_info=True)
                await safe_clear_markup(chat_id, message_id)
                await send_message(
                    chat_id,
                    "Failed to generate promo code. Please try again later.",
                    reply_markup=create_main_menu_keyboard()
                )
            return {"ok": True}

        return {"ok": True}

    except Exception as e:
        logger.error(f"Error in handle_points_and_discounts for chat_id {chat_id}: {str(e)}", exc_info=True)
        await safe_clear_markup(chat_id, message_id)
        await send_message(
            chat_id,
            "An error occurred. Please try again.",
            reply_markup=create_main_menu_keyboard()
        )
        return {"ok": True}
