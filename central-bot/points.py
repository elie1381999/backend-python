import logging
from typing import Optional, Dict, Any
import asyncio
from supabase import Client
from dotenv import load_dotenv
import os
import httpx

load_dotenv()
BOT_TOKEN = os.getenv("CENTRAL_BOT_TOKEN")

# Logging setup
logger = logging.getLogger(__name__)

# Constants for points awards
REFERRAL_POINTS = 50  # Points for referrer on referred user's successful booking
BOOKING_POINTS = 20   # Points for user on successful booking
PROFILE_BONUS = 50    # Points for completing profile (phone + DOB)
TIER_THRESHOLDS = {
    'Bronze': 0,
    'Silver': 201,
    'Gold': 501
}

async def get_user_points(chat_id: int, supabase: Client) -> Optional[Dict[str, Any]]:
    """
    Retrieve and format the user's points and tier from Supabase.
    """
    try:
        def _q():
            return supabase.table("central_bot_leads").select("points, tier").eq("telegram_id", chat_id).eq("is_draft", False).limit(1).execute()
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data", [])
        if not data:
            logger.info(f"No registered user found for chat_id {chat_id}")
            return None
        user = data[0]
        points = user.get("points", 0)
        tier = user.get("tier", "Bronze")
        logger.info(f"Retrieved points for chat_id {chat_id}: {points}, tier: {tier}")
        return {
            "points": points,
            "tier": tier,
            "message": f"Your balance: *{points} points* (Tier: {tier})"
        }
    except Exception as e:
        logger.error(f"Failed to retrieve points for chat_id {chat_id}: {str(e)}", exc_info=True)
        return None

async def generate_referral_link(chat_id: int, supabase: Client, bot_username: str) -> Optional[str]:
    """
    Generate or retrieve the user's referral link.
    bot_username: Your bot's username (e.g., 'YourBot').
    """
    try:
        def _q():
            return supabase.table("central_bot_leads").select("referral_code").eq("telegram_id", chat_id).eq("is_draft", False).limit(1).execute()
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data", [])
        if not data:
            logger.info(f"No registered user found for chat_id {chat_id}")
            return None
        referral_code = data[0]["referral_code"]
        link = f"https://t.me/{bot_username}?start={referral_code}"
        logger.info(f"Generated referral link for chat_id {chat_id}: {link}")
        return link
    except Exception as e:
        logger.error(f"Failed to generate referral link for chat_id {chat_id}: {str(e)}", exc_info=True)
        return None

async def award_referral_points(referred_user_id: str, supabase: Client) -> bool:
    """
    Award points to the referrer when a referred user completes a booking.
    referred_user_id: The ID of the referred user who completed the booking.
    """
    try:
        # Get referred user and their referrer
        def _get_referred():
            return supabase.table("central_bot_leads").select("referred_by, telegram_id").eq("id", referred_user_id).limit(1).execute()
        resp = await asyncio.to_thread(_get_referred)
        data = resp.data[0] if resp.data else None
        if not data or not data["referred_by"]:
            logger.info(f"No referrer found for referred_user_id {referred_user_id}")
            return False
        
        referrer_id = data["referred_by"]
        referred_telegram_id = data["telegram_id"]

        # Award points to referrer
        def _get_referrer_points():
            return supabase.table("central_bot_leads").select("points").eq("id", referrer_id).limit(1).execute()
        referrer_resp = await asyncio.to_thread(_get_referrer_points)
        current_points = referrer_resp.data[0].get("points", 0) if referrer_resp.data else 0
        new_points = current_points + REFERRAL_POINTS

        def _update_referrer():
            return supabase.table("central_bot_leads").update({"points": new_points}).eq("id", referrer_id).execute()
        await asyncio.to_thread(_update_referrer)
        
        # Log history
        await log_points_history(referrer_id, REFERRAL_POINTS, f"Referral from user {referred_telegram_id}", supabase)
        
        # Update tier
        await update_user_tier(referrer_id, supabase)
        
        # Notify referrer
        def _get_referrer_telegram():
            return supabase.table("central_bot_leads").select("telegram_id").eq("id", referrer_id).limit(1).execute()
        referrer_data = await asyncio.to_thread(_get_referrer_telegram)
        if referrer_data.data:
            referrer_chat_id = referrer_data.data[0]["telegram_id"]
            await send_message(referrer_chat_id, f"You earned {REFERRAL_POINTS} points for a friend's successful booking!")
        
        logger.info(f"Awarded {REFERRAL_POINTS} referral points to referrer_id {referrer_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to award referral points for referred_user_id {referred_user_id}: {str(e)}", exc_info=True)
        return False

async def award_booking_points(user_id: str, booking_id: str, supabase: Client) -> bool:
    """
    Award points to the user for a successful booking, and to referrer if applicable.
    Also marks points_awarded on the booking.
    """
    try:
        # Check if already awarded
        def _check_booking():
            return supabase.table("user_bookings").select("points_awarded, referral_awarded, user_id, business_id").eq("id", booking_id).eq("status", "completed").limit(1).execute()
        resp = await asyncio.to_thread(_check_booking)
        booking = resp.data[0] if resp.data else None
        if not booking or booking["points_awarded"]:
            logger.info(f"Booking {booking_id} not found or points already awarded")
            return False
        
        user_id = booking["user_id"]

        # Award to user
        def _get_user_points():
            return supabase.table("central_bot_leads").select("points, telegram_id").eq("id", user_id).limit(1).execute()
        user_resp = await asyncio.to_thread(_get_user_points)
        user_data = user_resp.data[0] if user_resp.data else None
        if not user_data:
            logger.error(f"User {user_id} not found")
            return False
        current_points = user_data.get("points", 0)
        new_points = current_points + BOOKING_POINTS

        def _update_user():
            return supabase.table("central_bot_leads").update({"points": new_points}).eq("id", user_id).execute()
        await asyncio.to_thread(_update_user)
        
        # Log history
        await log_points_history(user_id, BOOKING_POINTS, "Successful booking", supabase)
        
        # Update tier
        await update_user_tier(user_id, supabase)
        
        # Mark awarded
        def _update_booking():
            return supabase.table("user_bookings").update({"points_awarded": True}).eq("id", booking_id).execute()
        await asyncio.to_thread(_update_booking)
        
        # Notify user
        await send_message(user_data["telegram_id"], f"You earned {BOOKING_POINTS} points for a successful booking!")
        
        # If this is a referred user and referral not awarded yet, award to referrer
        if not booking["referral_awarded"]:
            await award_referral_points(user_id, supabase)
            def _update_referral_awarded():
                return supabase.table("user_bookings").update({"referral_awarded": True}).eq("id", booking_id).execute()
            await asyncio.to_thread(_update_referral_awarded)

        logger.info(f"Awarded {BOOKING_POINTS} booking points to user_id {user_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to award booking points for booking_id {booking_id}: {str(e)}", exc_info=True)
        return False

async def award_profile_bonus(chat_id: int, supabase: Client) -> bool:
    """
    Award points for completing profile (phone + DOB). Call after profile update.
    """
    try:
        def _q():
            return supabase.table("central_bot_leads").select("id, points, phone_number, dob, telegram_id").eq("telegram_id", chat_id).limit(1).execute()
        resp = await asyncio.to_thread(_q)
        user = resp.data[0] if resp.data else None
        if not user or not user["phone_number"] or not user["dob"]:
            logger.info(f"Profile not complete for chat_id {chat_id}")
            return False
        
        # Check if bonus already awarded
        def _check_history():
            return supabase.table("points_history").select("id").eq("user_id", user["id"]).eq("reason", "Profile completion").limit(1).execute()
        history_resp = await asyncio.to_thread(_check_history)
        if history_resp.data:
            logger.info(f"Profile bonus already awarded for chat_id {chat_id}")
            return False
        
        current_points = user.get("points", 0)
        new_points = current_points + PROFILE_BONUS
        
        def _update():
            return supabase.table("central_bot_leads").update({"points": new_points}).eq("id", user["id"]).execute()
        await asyncio.to_thread(_update)
        
        await log_points_history(user["id"], PROFILE_BONUS, "Profile completion", supabase)
        await update_user_tier(user["id"], supabase)
        
        # Notify user
        await send_message(user["telegram_id"], f"You earned {PROFILE_BONUS} points for completing your profile!")
        
        logger.info(f"Awarded {PROFILE_BONUS} profile bonus to chat_id {chat_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to award profile bonus for chat_id {chat_id}: {str(e)}", exc_info=True)
        return False

async def update_user_tier(user_id: str, supabase: Client) -> None:
    """
    Update user's tier based on points.
    """
    try:
        def _get_points():
            return supabase.table("central_bot_leads").select("points").eq("id", user_id).limit(1).execute()
        resp = await asyncio.to_thread(_get_points)
        points = resp.data[0].get("points", 0) if resp.data else 0
        
        new_tier = 'Bronze'
        for tier, threshold in sorted(TIER_THRESHOLDS.items(), key=lambda x: x[1], reverse=True):
            if points >= threshold:
                new_tier = tier
                break
        
        def _update_tier():
            return supabase.table("central_bot_leads").update({"tier": new_tier}).eq("id", user_id).execute()
        await asyncio.to_thread(_update_tier)
        
        logger.info(f"Updated tier for user_id {user_id} to {new_tier}")
    except Exception as e:
        logger.error(f"Failed to update tier for user_id {user_id}: {str(e)}", exc_info=True)

async def log_points_history(user_id: str, points: int, reason: str, supabase: Client) -> None:
    """
    Log points award history.
    """
    try:
        payload = {
            "user_id": user_id,
            "points": points,
            "reason": reason,
            "awarded_at": datetime.now(timezone.utc).isoformat()  # Added to match schema
        }
        await supabase_insert_return("points_history", payload, supabase)
    except Exception as e:
        logger.error(f"Failed to log points history for user_id {user_id}: {str(e)}", exc_info=True)

async def supabase_insert_return(table: str, payload: dict, supabase: Client) -> Optional[Dict[str, Any]]:
    """
    Helper for inserting and returning.
    """
    def _ins():
        return supabase.table(table).insert(payload).execute()
    try:
        resp = await asyncio.to_thread(_ins)
        data = resp.data if hasattr(resp, "data") else resp.get("data", [])
        if data:
            return data[0]
        logger.error(f"Insert failed for table {table}: no data returned")
        return None
    except Exception as e:
        logger.error(f"Insert failed for table {table}: {str(e)}", exc_info=True)
        return None

async def send_message(chat_id: int, text: str, retries: int = 3) -> Dict[str, Any]:
    """
    Helper to send Telegram messages (used by award functions).
    """
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
        for attempt in range(retries):
            try:
                logger.debug(f"Sending message to chat_id {chat_id} (attempt {attempt + 1}): {text}")
                response = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Sent message to chat_id {chat_id}: {text}")
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to send message: HTTP {e.response.status_code} - {e.response.text}")
                if e.response.status_code in [403, 400]:
                    return {"ok": False, "error": f"HTTP {e.response.status_code} (skipped)"}
                if e.response.status_code == 429:
                    retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    await asyncio.sleep(retry_after)
                    continue
                return {"ok": False, "error": f"HTTP {e.response.status_code}"}
            except Exception as e:
                logger.error(f"Failed to send message: {str(e)}", exc_info=True)
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        logger.error(f"Failed to send message to chat_id {chat_id} after {retries} attempts")
        return {"ok": False, "error": "Max retries reached"}
