import random
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
import logging
from supabase import Client

# Logging setup
logger = logging.getLogger(__name__)

async def generate_discount_code(chat_id: int, business_id: str, discount_id: str, supabase: Client) -> tuple[str, str]:
    """
    Generate a unique discount promo code for a user and store it in Supabase.
    Returns (code, expiry).
    """
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
    logger.info(f"Generated discount promo code {code} for chat_id {chat_id}, discount_id: {discount_id}")
    return code, expiry

async def generate_promo_code(chat_id: int, business_id: str, giveaway_id: str, discount_type: str = "standard", supabase: Client) -> tuple[str, str]:
    """
    Generate a unique giveaway promo code for a user and store it in Supabase.
    Returns (code, expiry).
    """
    allowed_statuses = ['standard', 'loser', 'awaiting_booking', 'redeemed', 'pending']
    if discount_type not in allowed_statuses:
        raise ValueError(f"Invalid entry_status: {discount_type}. Must be one of {allowed_statuses}")
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
    inserted = await supabase_insert_return("user_giveaways", payload, supabase)
    if not inserted:
        logger.error(f"Failed to insert giveaway promo code for chat_id: {chat_id}, giveaway_id: {giveaway_id}")
        raise RuntimeError("Failed to save promo code")
    logger.info(f"Generated giveaway promo code {code} for chat_id {chat_id}, giveaway_id: {giveaway_id}")
    return code, expiry

async def has_redeemed_discount(chat_id: int, supabase: Client) -> bool:
    """
    Check if the user has redeemed a discount in the current month.
    """
    def _q():
        current_month = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return supabase.table("user_discounts").select("id").eq("telegram_id", chat_id).eq("entry_status", "standard").gte("joined_at", current_month.isoformat()).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data", [])
        has_redeemed = bool(data)
        logger.info(f"Checked redeemed discount for chat_id {chat_id}: {has_redeemed}")
        return has_redeemed
    except Exception as e:
        logger.error(f"has_redeemed_discount failed for chat_id {chat_id}: {str(e)}", exc_info=True)
        return False

async def supabase_insert_return(table: str, payload: dict, supabase: Client) -> Optional[Dict[str, Any]]:
    """
    Helper for inserting and returning data from Supabase.
    """
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
