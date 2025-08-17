
import os
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
import httpx
from supabase import Client
from dotenv import load_dotenv

# Set up logging to match central_bot.py
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger("httpx").setLevel(logging.DEBUG)
logging.getLogger("httpcore").setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BOT_TOKEN = os.getenv("CENTRAL_BOT_TOKEN")

if not SUPABASE_URL or not SUPABASE_KEY or not BOT_TOKEN:
    raise RuntimeError("SUPABASE_URL, SUPABASE_KEY, or CENTRAL_BOT_TOKEN must be set in .env")

# Assume supabase client is passed from main.py or central_bot.py
# For standalone testing, you can initialize it here
from supabase import create_client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

async def list_active_giveaways() -> List[Dict[str, Any]]:
    """
    Retrieve all active giveaways from Supabase where the current date is between start_date and end_date.
    Returns a list of giveaway dictionaries.
    """
    try:
        current_date = datetime.now().isoformat()
        logger.debug(f"Fetching active giveaways for date {current_date}")
        
        response = supabase.table("giveaways").select("*").lte("start_date", current_date).gte("end_date", current_date).execute()
        giveaways = response.data if hasattr(response, "data") else response.get("data", [])
        
        if not giveaways:
            logger.info("No active giveaways found")
            return []
        
        logger.debug(f"Found {len(giveaways)} active giveaways: {giveaways}")
        return giveaways
    except Exception as e:
        logger.error(f"Failed to fetch active giveaways: {str(e)}", exc_info=True)
        return []

async def join_giveaway(telegram_id: int, giveaway_id: str) -> Dict[str, Any]:
    """
    Allow a user to join a giveaway if they have enough points and meet eligibility criteria.
    Deducts points and creates an entry in user_giveaways.
    Returns a dict with status or error.
    """
    try:
        # Fetch user
        user_response = supabase.table("central_bot_leads").select("points, phone_number, is_approved").eq("telegram_id", telegram_id).eq("is_draft", False).limit(1).execute()
        user = user_response.data[0] if hasattr(user_response, "data") and user_response.data else None
        if not user:
            logger.error(f"User with telegram_id {telegram_id} not found or not registered")
            return {"error": "User not found or not registered."}
        
        if not user.get("is_approved", False):
            logger.error(f"User {telegram_id} is not approved")
            return {"error": "Your account is not yet approved. Please wait for admin approval."}

        if not user.get("phone_number"):
            logger.error(f"User {telegram_id} has no phone number")
            return {"error": "Phone verification required to join giveaways."}

        # Fetch giveaway
        giveaway_response = supabase.table("giveaways").select("*").eq("id", giveaway_id).limit(1).execute()
        giveaway = giveaway_response.data[0] if hasattr(giveaway_response, "data") and giveaway_response.data else None
        if not giveaway:
            logger.error(f"Giveaway {giveaway_id} not found")
            return {"error": "Giveaway not found."}

        # Check if giveaway is active
        current_date = datetime.now().isoformat()
        if giveaway["start_date"] > current_date or giveaway["end_date"] < current_date:
            logger.error(f"Giveaway {giveaway_id} is not active")
            return {"error": "This giveaway is not active."}

        # Check points
        user_points = user.get("points", 0)
        giveaway_cost = giveaway.get("cost", 0)
        if user_points < giveaway_cost:
            logger.error(f"User {telegram_id} has insufficient points: {user_points} < {giveaway_cost}")
            return {"error": f"Insufficient points. You need {giveaway_cost} points, but you have {user_points}."}

        # Check max entries
        if giveaway.get("max_entries"):
            entry_count_response = supabase.table("user_giveaways").select("count").eq("giveaway_id", giveaway_id).execute()
            entry_count = entry_count_response.data[0]["count"] if hasattr(entry_count_response, "data") and entry_count_response.data else 0
            if entry_count >= giveaway["max_entries"]:
                logger.error(f"Giveaway {giveaway_id} is fully booked")
                return {"error": "This giveaway is fully booked."}

        # Check if user already entered
        existing_entry = supabase.table("user_giveaways").select("*").eq("telegram_id", telegram_id).eq("giveaway_id", giveaway_id).limit(1).execute()
        if existing_entry.data:
            logger.error(f"User {telegram_id} already entered giveaway {giveaway_id}")
            return {"error": "You already entered this giveaway."}

        # Deduct points
        new_points = user_points - giveaway_cost
        update_response = supabase.table("central_bot_leads").update({"points": new_points}).eq("telegram_id", telegram_id).execute()
        if not update_response.data:
            logger.error(f"Failed to deduct points for user {telegram_id}")
            return {"error": "Failed to process entry due to points update error."}

        # Create entry
        entry_id = str(uuid.uuid4())
        entry_response = supabase.table("user_giveaways").insert({
            "telegram_id": telegram_id,
            "giveaway_id": giveaway_id,
            "entry_status": "pending",
            "entry_id": entry_id,
            "created_at": datetime.now().isoformat()
        }).execute()
        
        if not entry_response.data:
            logger.error(f"Failed to create entry for user {telegram_id} in giveaway {giveaway_id}")
            # Roll back points
            supabase.table("central_bot_leads").update({"points": user_points}).eq("telegram_id", telegram_id).execute()
            return {"error": "Failed to create entry. Points have been restored."}

        # Notify user
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
            try:
                message = f"✅ Successfully entered giveaway *{giveaway['name']}*. Remaining points: {new_points}"
                response = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json={"chat_id": telegram_id, "text": message, "parse_mode": "Markdown"}
                )
                response.raise_for_status()
                logger.info(f"Sent confirmation to chat_id {telegram_id} for giveaway {giveaway_id}")
            except Exception as e:
                logger.error(f"Failed to send confirmation to chat_id {telegram_id}: {str(e)}", exc_info=True)

        logger.info(f"User {telegram_id} joined giveaway {giveaway_id} successfully")
        return {"status": "Successfully entered giveaway", "remaining_points": new_points}
    except Exception as e:
        logger.error(f"Error in join_giveaway for user {telegram_id}, giveaway {giveaway_id}: {str(e)}", exc_info=True)
        return {"error": "Internal server error. Please try again later."}
