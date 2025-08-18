import random
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
import asyncio
from supabase import Client

# Logging setup
logger = logging.getLogger(__name__)

# Constants
CATEGORIES = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining"]

def create_categories_keyboard():
    buttons = []
    for cat in CATEGORIES:
        buttons.append([{"text": cat, "callback_data": f"discount_category:{cat}"}])
    return {"inline_keyboard": buttons}

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

async def has_redeemed_discount(chat_id: int, supabase: Client) -> bool:
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

async def handle_discount_callback(chat_id: int, callback_data: str, message_id: int, registered: Dict[str, Any], state: Dict[str, Any], supabase: Client) -> None:
    async def send_message(text: str, reply_markup: Optional[dict] = None):
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
            payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
            if reply_markup:
                payload["reply_markup"] = reply_markup
            try:
                response = await client.post(
                    f"https://api.telegram.org/bot{os.getenv('CENTRAL_BOT_TOKEN')}/sendMessage",
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Sent message to chat_id {chat_id}: {text}")
            except Exception as e:
                logger.error(f"Failed to send message to chat_id {chat_id}: {str(e)}", exc_info=True)

    async def safe_clear_markup():
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
                await client.post(
                    f"https://api.telegram.org/bot{os.getenv('CENTRAL_BOT_TOKEN')}/editMessageReplyMarkup",
                    json={"chat_id": chat_id, "message_id": message_id, "reply_markup": {}}
                )
                logger.info(f"Cleared keyboard for chat_id {chat_id}, message_id {message_id}")
        except Exception:
            logger.debug(f"Ignored error while clearing keyboard for chat_id {chat_id}")

    if callback_data == "menu:discounts":
        if not registered.get("phone_number") or not registered.get("dob"):
            await send_message("Complete your profile to access discounts:", reply_markup={
                "keyboard": [[{"text": "Share phone", "request_contact": True}]],
                "resize_keyboard": True,
                "one_time_keyboard": True
            })
            state["stage"] = "awaiting_phone_profile"
            state["data"] = registered
            state["entry_id"] = registered["id"]
            supabase.table("user_states").upsert({"chat_id": chat_id, "state": state, "updated_at": datetime.now(timezone.utc).isoformat()}).execute()
            return
        interests = registered.get("interests", []) or []
        if not interests:
            await send_message("No interests set. Please update your profile.")
            return
        await send_message("Choose a category for discounts:", reply_markup=create_categories_keyboard())
        return

    elif callback_data.startswith("discount_category:"):
        category = callback_data[len("discount_category:"):]
        if category not in CATEGORIES:
            await send_message("Invalid category.")
            return
        try:
            def _query_discounts():
                return supabase.table("discounts").select("id, name, discount_percentage, category, business_id").eq("category", category).eq("active", True).execute()
            resp = await asyncio.to_thread(_query_discounts)
            discounts = resp.data if hasattr(resp, "data") else resp.get("data", [])
            if not discounts:
                await send_message(f"No discounts available in *{category}*.")
                return
            for d in discounts:
                business = await supabase_find_business(d["business_id"], supabase)
                if not business:
                    await send_message(f"Business not found for discount {d['name']}.")
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
                await send_message(message, keyboard)
        except Exception as e:
            logger.error(f"Failed to fetch discounts for category {category}, chat_id {chat_id}: {str(e)}", exc_info=True)
            await send_message("Failed to load discounts. Please try again later.")
        return

    elif callback_data.startswith("get_discount:"):
        discount_id = callback_data[len("get_discount:"):]
        try:
            discount = await supabase_find_discount(discount_id, supabase)
            if not discount:
                await send_message("Discount not found.")
                return
            business = await supabase_find_business(discount["business_id"], supabase)
            if not business:
                await send_message("Business not found for this discount.")
                return
            code, expiry = await generate_discount_code(chat_id, discount["business_id"], discount_id, supabase)
            expiry_date = datetime.fromisoformat(expiry).strftime("%Y-%m-%d")
            msg = (
                f"Discount Code: *{code}*\n"
                f"For: {discount['name']} ({discount['discount_percentage']}% off)\n"
                f"At: {business['name']}\n"
                f"Expires: {expiry_date}\n"
                f"Show this code to the business to redeem."
            )
            await send_message(msg)
        except ValueError as e:
            await send_message(str(e))
        except Exception as e:
            logger.error(f"Failed to generate discount code for discount_id {discount_id}: {str(e)}", exc_info=True)
            await send_message("Failed to generate discount code. Please try again.")
        return
