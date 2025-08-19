from typing import Dict, Any
import asyncio
from utils import send_message, create_categories_keyboard, supabase_find_business, generate_discount_code, create_phone_keyboard, award_points, has_history, POINTS_BOOKING_CREATED, supabase_insert_return, CATEGORIES, logger, uuid

async def handle_discounts(callback_query: Dict[str, Any], registered: Dict[str, Any], chat_id: int):
    if not registered.get("phone_number") or not registered.get("dob"):
        await send_message(chat_id, "Complete your profile to access discounts:", reply_markup=create_phone_keyboard())
        state = get_state(chat_id) or {}
        state["stage"] = "awaiting_phone_profile"
        state["data"] = registered
        state["entry_id"] = registered["id"]
        set_state(chat_id, state)
        return {"ok": True}
    interests = registered.get("interests", []) or []
    if not interests:
        await send_message(chat_id, "No interests set. Please update your profile.")
        return {"ok": True}
    await send_message(chat_id, "Choose a category for discounts:", reply_markup=create_categories_keyboard())
    return {"ok": True}

async def handle_discount_callback(callback_data: str, chat_id: int, registered: Dict[str, Any]):
    if callback_data.startswith("discount_category:"):
        category = callback_data[len("discount_category:"):]
        if category not in CATEGORIES:
            await send_message(chat_id, "Invalid category.")
            return {"ok": True}
        try:
            def _query_discounts():
                return supabase.table("discounts").select("id, name, discount_percentage, category, business_id").eq("category", category).eq("active", True).execute()
            resp = await asyncio.to_thread(_query_discounts)
            discounts = resp.data if hasattr(resp, "data") else resp.get("data", [])
            if not discounts:
                await send_message(chat_id, f"No discounts in *{category}*.")
                return {"ok": True}
            for d in discounts:
                business = await supabase_find_business(d["business_id"])
                if not business:
                    await send_message(chat_id, f"Business not found for {d['name']}.")
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
        except Exception as e:
            logger.error(f"Fetch discounts failed: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to load discounts.")
        return {"ok": True}
    elif callback_data.startswith("profile:"):
        business_id = callback_data[len("profile:"):]
        try:
            business = await supabase_find_business(business_id)
            if not business:
                await send_message(chat_id, "Business not found.")
                return {"ok": True}
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
            logger.error(f"Fetch profile failed: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to load profile.")
        return {"ok": True}
    elif callback_data.startswith("services:"):
        business_id = callback_data[len("services:"):]
        try:
            business = await supabase_find_business(business_id)
            if not business:
                await send_message(chat_id, "Business not found.")
                return {"ok": True}
            prices = business.get("prices", {})
            msg = "Services:\n" + "\n".join(f"{k}: {v}" for k, v in prices.items()) if prices else "No services listed."
            await send_message(chat_id, msg)
        except Exception as e:
            logger.error(f"Fetch services failed: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to load services.")
        return {"ok": True}
    elif callback_data.startswith("book:"):
        business_id = callback_data[len("book:"):]
        try:
            business = await supabase_find_business(business_id)
            if not business:
                await send_message(chat_id, "Business not found.")
                return {"ok": True}
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
                        await send_message(chat_id, f"Booking request created (ref: {created_booking['id']}). Contact {business['name']} at {business.get('phone_number', 'Not set')}.")
                    else:
                        await send_message(chat_id, f"Contact {business['name']} at {business.get('phone_number', 'Not set')}.")
                except Exception:
                    logger.exception("Create booking failed")
                    await send_message(chat_id, f"Contact {business['name']} at {business.get('phone_number', 'Not set')}.")
            else:
                await send_message(chat_id, f"To book, contact {business['name']} at {business.get('phone_number', 'Not set')}.")
        except Exception as e:
            logger.error(f"Book info failed: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to load booking info.")
        return {"ok": True}
    elif callback_data.startswith("get_discount:"):
        discount_id = callback_data[len("get_discount:"):]
        try:
            uuid.UUID(discount_id)
            discount = await supabase_find_discount(discount_id)
            if not discount or not discount["active"]:
                await send_message(chat_id, "Discount not found or inactive.")
                return {"ok": True}
            if not discount.get("business_id"):
                await send_message(chat_id, "Discount unavailable due to config issue.")
                return {"ok": True}
            code, expiry = await generate_discount_code(chat_id, discount["business_id"], discount_id)
            await send_message(chat_id, f"Your promo code: *{code}* for {discount['name']}. Valid until {expiry.split('T')[0]}.")
        except ValueError as ve:
            await send_message(chat_id, str(ve))
        except Exception as e:
            logger.error(f"Generate discount failed: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to generate promo.")
        return {"ok": True}
