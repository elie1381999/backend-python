import os
import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException, Header, Depends
from starlette.responses import PlainTextResponse, JSONResponse
from supabase import create_client, Client

from convo import (
    handle_message,
    handle_callback,
    supabase_find_business,
    supabase_find_giveaway,
    supabase_find_registered,
    supabase_update_by_id_return,
    notify_users,
    award_points,
    has_history,
    POINTS_REFERRAL_VERIFIED,
    POINTS_BOOKING_VERIFIED,
    initialize_bot,
    get_points_awarded_today,
    DAILY_POINTS_CAP,
)
from utils import send_message, set_menu_button, safe_clear_markup

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Load environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
ADMIN_SECRET = os.getenv("ADMIN_SECRET")
CENTRAL_BOT_TOKEN = os.getenv("CENTRAL_BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
VERIFY_KEY = os.getenv("VERIFY_KEY")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")

app = FastAPI(title="Multi-Business Telegram Bot")

if not all([SUPABASE_URL, SUPABASE_KEY, CENTRAL_BOT_TOKEN, WEBHOOK_URL]):
    logger.warning("One of SUPABASE_URL, SUPABASE_KEY, CENTRAL_BOT_TOKEN, WEBHOOK_URL missing from .env")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Dependency for admin authentication
async def verify_admin_secret(x_admin_secret: str = Header(None)):
    if not ADMIN_SECRET or x_admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Invalid or missing admin secret")
    return True

# Initialize bot on startup
@app.on_event("startup")
async def startup_event():
    await initialize_bot(WEBHOOK_URL, CENTRAL_BOT_TOKEN)

# --- Routes ---------------------------------------------------------------

@app.post("/hook/central_bot")
async def central_hook(request: Request):
    """Primary webhook endpoint for central bot. Delegates to convo_central handlers."""
    try:
        update = await request.json()
    except Exception:
        logger.exception("Invalid JSON in central hook")
        return PlainTextResponse("ok", status_code=200)

    # require at least something
    if not update:
        return PlainTextResponse("ok", status_code=200)

    # Optionally set menu button (safe to call repeatedly)
    try:
        await set_menu_button(CENTRAL_BOT_TOKEN)
    except Exception:
        logger.exception("set_menu_button failed (continuing)")

    message = update.get("message")
    callback_query = update.get("callback_query")

    chat_id = None
    if message:
        chat_id = message.get("chat", {}).get("id")
    elif callback_query:
        chat_id = callback_query.get("from", {}).get("id")

    # Admin manual approve via text commands (simple pattern)
    if chat_id and ADMIN_CHAT_ID and int(chat_id) == int(ADMIN_CHAT_ID):
        text = (message.get("text") or "") if message else ""
        if text.startswith("/approve_"):
            business_id = text[len("/approve_"):]
            try:
                uuid.UUID(business_id)
                business = await supabase_find_business(business_id)
                if not business:
                    await send_message(chat_id, f"Business {business_id} not found.", token=CENTRAL_BOT_TOKEN)
                    return PlainTextResponse("ok", status_code=200)
                await supabase_update_by_id_return("businesses", business_id, {"status": "approved", "updated_at": datetime.now(timezone.utc).isoformat()})
                await send_message(chat_id, f"Business {business.get('name', business_id)} approved.", token=CENTRAL_BOT_TOKEN)
                await send_message(business["telegram_id"], "Your business has been approved! You can now add discounts and giveaways.", token=CENTRAL_BOT_TOKEN)
            except Exception:
                logger.exception("approve command failed")
                await send_message(chat_id, f"Failed to approve business {business_id}.", token=CENTRAL_BOT_TOKEN)
            return PlainTextResponse("ok", status_code=200)

        if text.startswith("/reject_"):
            business_id = text[len("/reject_"):]
            try:
                uuid.UUID(business_id)
                business = await supabase_find_business(business_id)
                if not business:
                    await send_message(chat_id, f"Business {business_id} not found.", token=CENTRAL_BOT_TOKEN)
                    return PlainTextResponse("ok", status_code=200)
                await supabase_update_by_id_return("businesses", business_id, {"status": "rejected", "updated_at": datetime.now(timezone.utc).isoformat()})
                await send_message(chat_id, f"Business {business.get('name', business_id)} rejected.", token=CENTRAL_BOT_TOKEN)
                await send_message(business["telegram_id"], "Your business registration was rejected. Please contact support.", token=CENTRAL_BOT_TOKEN)
            except Exception:
                logger.exception("reject command failed")
                await send_message(chat_id, f"Failed to reject business {business_id}.", token=CENTRAL_BOT_TOKEN)
            return PlainTextResponse("ok", status_code=200)

    # Delegate updates
    try:
        if message:
            await handle_message(chat_id, message, CENTRAL_BOT_TOKEN)
        if callback_query:
            await handle_callback(chat_id, callback_query, CENTRAL_BOT_TOKEN)
    except Exception:
        logger.exception("Error delegating update")
    return PlainTextResponse("ok", status_code=200)

# Backwards-compat alias expected by previous main.py
# Some deployment scaffolds import `webhook_handler` from central_bot.
# Provide that name so `from central_bot import webhook_handler` works.
@app.post("/hook/central_bot_legacy")
async def webhook_handler(request: Request):
    return await central_hook(request)

@app.post("/admin/notify/city")
async def admin_notify_city(request: Request, is_admin: bool = Depends(verify_admin_secret)):
    try:
        payload = await request.json()
        city = payload.get("city")
        message = payload.get("message")
        if not city or not message:
            raise HTTPException(status_code=400, detail="City and message are required")
        
        # Find users in the specified city
        def _q_users():
            return supabase.table("central_bot_leads").select("telegram_id").eq("city", city).execute()
        
        resp = await asyncio.to_thread(_q_users)
        users = resp.data if hasattr(resp, "data") else resp.get("data", [])
        
        # Send notification to all users in the city
        for user in users:
            await send_message(user["telegram_id"], f"City-wide announcement: {message}", token=CENTRAL_BOT_TOKEN)
            
        return {"ok": True, "notified": len(users)}
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

@app.post("/verify_booking")
async def verify_booking(request: Request):
    if VERIFY_KEY:
        provided = request.headers.get("x-verify-key")
        if provided != VERIFY_KEY:
            return PlainTextResponse("Forbidden", status_code=403)
    try:
        body = await request.json()
        promo_code = body.get("promo_code")
        business_id = body.get("business_id")
        if not promo_code or not business_id:
            return PlainTextResponse("promo_code and business_id required", status_code=400)

        # Try to find in user_giveaways first
        def _q_giveaway():
            return supabase.table("user_giveaways").select("*").eq("promo_code", promo_code).eq("business_id", business_id).limit(1).execute()
        
        resp = await asyncio.to_thread(_q_giveaway)
        ug = resp.data[0] if (hasattr(resp, "data") and resp.data) else None

        found_row = None
        table_name = None
        
        if ug:
            found_row = ug
            table_name = "user_giveaways"
        else:
            # fallback to user_discounts
            def _q_discount():
                return supabase.table("user_discounts").select("*").eq("promo_code", promo_code).eq("business_id", business_id).limit(1).execute()
            
            resp2 = await asyncio.to_thread(_q_discount)
            ud = resp2.data[0] if (hasattr(resp2, "data") and resp2.data) else None
            if ud:
                found_row = ud
                table_name = "user_discounts"

        if not found_row:
            return PlainTextResponse("Promo not found", status_code=404)

        telegram_id = found_row.get("telegram_id")
        if not telegram_id:
            return PlainTextResponse("No telegram_id for promo", status_code=400)

        # find user
        user = await supabase_find_registered(telegram_id)
        if not user:
            return PlainTextResponse("User not found", status_code=404)

        # create or update booking record to completed
        def _find_booking():
            return supabase.table("user_bookings").select("*").eq("user_id", user["id"]).eq("business_id", business_id).limit(1).execute()
        
        resp_b = await asyncio.to_thread(_find_booking)
        booking = resp_b.data[0] if (hasattr(resp_b, "data") and resp_b.data) else None

        booking_id = None
        if booking:
            # if already awarded, do nothing
            if booking.get("status") == "completed" or booking.get("points_awarded"):
                return {"ok": True, "message": "already_verified"}
            
            # update booking to completed and mark points_awarded True
            def _upd_booking():
                return supabase.table("user_bookings").update({
                    "status": "completed", 
                    "points_awarded": True, 
                    "booking_date": datetime.now(timezone.utc).isoformat()
                }).eq("id", booking["id"]).execute()
            
            await asyncio.to_thread(_upd_booking)
            booking_id = booking["id"]
        else:
            # create completed booking
            def _create_booking():
                return supabase.table("user_bookings").insert({
                    "user_id": user["id"],
                    "business_id": business_id,
                    "booking_date": datetime.now(timezone.utc).isoformat(),
                    "status": "completed",
                    "points_awarded": True
                }).execute()
            
            resp_create = await asyncio.to_thread(_create_booking)
            booking_data = resp_create.data[0] if (hasattr(resp_create, "data") and resp_create.data) else None
            booking_id = booking_data["id"] if booking_data else None

        # award verified booking points (idempotent by using a unique reason including promo_code)
        reason = f"booking_verified:{promo_code}"
        if not await has_history(user["id"], reason):
            await award_points(user["id"], POINTS_BOOKING_VERIFIED, reason, booking_id)

        # update promo row entry_status -> redeemed
        try:
            if table_name == "user_giveaways":
                await supabase_update_by_id_return("user_giveaways", found_row["id"], {
                    "entry_status": "redeemed", 
                    "redeemed_at": datetime.now(timezone.utc).isoformat()
                })
            else:
                await supabase_update_by_id_return("user_discounts", found_row["id"], {
                    "entry_status": "redeemed", 
                    "redeemed_at": datetime.now(timezone.utc).isoformat()
                })
        except Exception:
            logger.exception("Failed to update promo entry_status after verification")

        return {"ok": True, "user_id": user["id"], "booking_id": booking_id}
    except json.JSONDecodeError:
        return PlainTextResponse("Invalid JSON", status_code=400)
    except Exception as e:
        logger.exception("verify_booking failed")
        return PlainTextResponse("Internal Error", status_code=500)

@app.get("/")
def root():
    return {"message": "Multi-Business Telegram Bot is running!"}

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/admin/stats")
async def admin_stats(is_admin: bool = Depends(verify_admin_secret)):
    """Get system statistics (admin only)"""
    try:
        # Get user count
        def _q_users():
            return supabase.table("central_bot_leads").select("id", count="exact").execute()
        
        # Get business count
        def _q_businesses():
            return supabase.table("businesses").select("id", count="exact").execute()
        
        # Get active discounts count
        def _q_discounts():
            return supabase.table("discounts").select("id", count="exact").eq("active", True).execute()
        
        # Get active giveaways count
        def _q_giveaways():
            return supabase.table("giveaways").select("id", count="exact").eq("active", True).execute()
        
        users_resp = await asyncio.to_thread(_q_users)
        businesses_resp = await asyncio.to_thread(_q_businesses)
        discounts_resp = await asyncio.to_thread(_q_discounts)
        giveaways_resp = await asyncio.to_thread(_q_giveaways)
        
        users_count = users_resp.count if hasattr(users_resp, "count") else len(users_resp.data or [])
        businesses_count = businesses_resp.count if hasattr(businesses_resp, "count") else len(businesses_resp.data or [])
        discounts_count = discounts_resp.count if hasattr(discounts_resp, "count") else len(discounts_resp.data or [])
        giveaways_count = giveaways_resp.count if hasattr(giveaways_resp, "count") else len(giveaways_resp.data or [])
        
        return {
            "users": users_count,
            "businesses": businesses_count,
            "active_discounts": discounts_count,
            "active_giveaways": giveaways_count
        }
    except Exception as e:
        logger.exception("Failed to get admin stats")
        raise HTTPException(status_code=500, detail="Internal server error")










'''
# central_bot.py
import os
import asyncio
import logging
import uuid
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException, Header
from starlette.responses import PlainTextResponse

# Import helpers from convo_central
from convo_central import (
    handle_message,
    handle_callback,
    supabase_find_business,
    supabase_find_giveaway,
    supabase_find_registered,
    supabase_update_by_id_return,
    notify_users,
    award_points,
    has_history,
    POINTS_REFERRAL_VERIFIED,
    find_promo_row,
    mark_promo_as_winner,
)

from utils import send_message, set_menu_button

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Load environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
ADMIN_SECRET = os.getenv("ADMIN_SECRET")
CENTRAL_BOT_TOKEN = os.getenv("CENTRAL_BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
VERIFY_KEY = os.getenv("VERIFY_KEY")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")

app = FastAPI(title="Multi-Business Telegram Bot")

if not all([SUPABASE_URL, SUPABASE_KEY, CENTRAL_BOT_TOKEN, WEBHOOK_URL]):
    logger.warning("One of SUPABASE_URL, SUPABASE_KEY, CENTRAL_BOT_TOKEN, WEBHOOK_URL missing from .env")

# --- Routes ---------------------------------------------------------------

@app.post("/hook/central_bot")
async def central_hook(request: Request):
    """Primary webhook endpoint for central bot. Delegates to convo_central handlers."""
    try:
        update = await request.json()
    except Exception:
        logger.exception("Invalid JSON in central hook")
        return PlainTextResponse("ok", status_code=200)

    if not update:
        return PlainTextResponse("ok", status_code=200)

    # Optionally set menu button (safe to call repeatedly)
    try:
        await set_menu_button()
    except Exception:
        logger.exception("set_menu_button failed (continuing)")

    message = update.get("message")
    callback_query = update.get("callback_query")

    chat_id = None
    if message:
        chat_id = message.get("chat", {}).get("id")
    elif callback_query:
        chat_id = callback_query.get("from", {}).get("id")

    # Admin manual approve via text commands (simple pattern)
    if chat_id and ADMIN_CHAT_ID and int(chat_id) == int(ADMIN_CHAT_ID):
        text = (message.get("text") or "") if message else ""
        if text.startswith("/approve_"):
            business_id = text[len("/approve_"):]
            try:
                uuid.UUID(business_id)
                business = await supabase_find_business(business_id)
                if not business:
                    await send_message(chat_id, f"Business {business_id} not found.")
                    return PlainTextResponse("ok", status_code=200)
                await supabase_update_by_id_return("businesses", business_id, {"status": "approved", "updated_at": datetime.utcnow().isoformat()})
                await send_message(chat_id, f"Business {business.get('name', business_id)} approved.")
                if business.get("telegram_id"):
                    await send_message(business["telegram_id"], "Your business has been approved! You can now add discounts and giveaways.")
            except Exception:
                logger.exception("approve command failed")
                await send_message(chat_id, f"Failed to approve business {business_id}.")
            return PlainTextResponse("ok", status_code=200)

        if text.startswith("/reject_"):
            business_id = text[len("/reject_"):]
            try:
                uuid.UUID(business_id)
                business = await supabase_find_business(business_id)
                if not business:
                    await send_message(chat_id, f"Business {business_id} not found.")
                    return PlainTextResponse("ok", status_code=200)
                await supabase_update_by_id_return("businesses", business_id, {"status": "rejected", "updated_at": datetime.utcnow().isoformat()})
                await send_message(chat_id, f"Business {business.get('name', business_id)} rejected.")
                if business.get("telegram_id"):
                    await send_message(business["telegram_id"], "Your business registration was rejected. Please contact support.")
            except Exception:
                logger.exception("reject command failed")
                await send_message(chat_id, f"Failed to reject business {business_id}.")
            return PlainTextResponse("ok", status_code=200)

    # Delegate updates
    try:
        if message:
            await handle_message(chat_id, message)
        if callback_query:
            await handle_callback(chat_id, callback_query)
    except Exception:
        logger.exception("Error delegating update")
    return PlainTextResponse("ok", status_code=200)

# Backwards-compat alias expected by previous main.py
async def webhook_handler(request: Request):
    return await central_hook(request)

@app.post("/admin/notify/city")
async def admin_notify_city(request: Request, x_admin_secret: str = Header(...)):
    if not ADMIN_SECRET or x_admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Invalid or missing admin secret")
    try:
        payload = await request.json()
        city = payload.get("city")
        message = payload.get("message")
        if not city or not message:
            raise HTTPException(status_code=400, detail="City and message are required")
        # notify_city logic: find businesses or users by city and notify (left abstract here)
        giveaway_id = payload.get("giveaway_id")
        if giveaway_id:
            await notify_users(giveaway_id)
            return {"ok": True}
        return {"ok": True}
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

@app.post("/verify_booking")
async def verify_booking(request: Request):
    if VERIFY_KEY:
        provided = request.headers.get("x-verify-key")
        if provided != VERIFY_KEY:
            return PlainTextResponse("Forbidden", status_code=403)
    try:
        body = await request.json()
        promo_code = body.get("promo_code")
        business_id = body.get("business_id")
        if not promo_code or not business_id:
            return PlainTextResponse("promo_code and business_id required", status_code=400)

        # Use convo_central helper to find promo row in either table
        promo_row, table_name = await find_promo_row(promo_code, business_id)
        if not promo_row:
            return PlainTextResponse("Invalid promo code or business ID", status_code=400)

        # only verify promos that were awaiting booking
        if promo_row.get("entry_status") != "awaiting_booking":
            return PlainTextResponse("Promo code not eligible for verification", status_code=400)

        giveaway_id = promo_row.get("giveaway_id")
        giveaway = await supabase_find_giveaway(giveaway_id)
        if not giveaway:
            return PlainTextResponse("Giveaway not found", status_code=400)

        # mark promo row as winner (delegated helper)
        try:
            await mark_promo_as_winner(table_name, promo_row["id"])
        except Exception:
            logger.exception("mark_promo_as_winner failed (continuing)")

        chat_id = promo_row["telegram_id"]
        business = await supabase_find_business(business_id)
        await send_message(chat_id, f"Congratulations! Your booking for {giveaway['name']} at {business.get('name', 'Unknown')} has been verified. You're a winner!")

        user = await supabase_find_registered(chat_id)
        if user:
            reason = f"booking_verified:{giveaway_id}"
            if not await has_history(user["id"], reason):
                await award_points(user["id"], POINTS_REFERRAL_VERIFIED, reason)
        return PlainTextResponse("Booking verified", status_code=200)
    except Exception:
        logger.exception("Failed to verify booking")
        return PlainTextResponse("Internal server error", status_code=500)

@app.get("/")
def root():
    return {"message": "Multi-Business Telegram Bot is running!"}

@app.get("/health")
def health_check():
    return {"status": "ok"}
'''
