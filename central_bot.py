# central_bot.py
import os
import asyncio
import logging
import uuid
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException, Header
from starlette.responses import PlainTextResponse

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

        # Find promo in giveaways (primary)
        def _q_giveaway():
            return convo_supabase.table("user_giveaways").select("*").eq("promo_code", promo_code).eq("business_id", business_id).limit(1).execute()  # placeholder replaced below
        # Because central_bot shouldn't talk directly to supabase instance in this module,
        # we delegate verification logic to convo_central via its helpers by reusing patterns there.
        # Here we call functions from convo_central:
        from convo_central import find_promo_row  # local import to avoid circular import earlier
        promo_row, table_name = await find_promo_row(promo_code, business_id)
        if not promo_row:
            return PlainTextResponse("Invalid promo code or business ID", status_code=400)
        # Only allow verification for awaiting_booking rows
        if promo_row.get("entry_status") != "awaiting_booking":
            return PlainTextResponse("Promo code not eligible for verification", status_code=400)

        giveaway_id = promo_row.get("giveaway_id")
        giveaway = await supabase_find_giveaway(giveaway_id)
        if not giveaway:
            return PlainTextResponse("Giveaway not found", status_code=400)

        # mark winner
        await convo_central_update_promo_to_winner = None
        # delegate to convo_central helper that updates DB rows:
        from convo_central import mark_promo_as_winner, supabase_find_registered as _unused
        try:
            await mark_promo_as_winner(table_name, promo_row["id"])
        except Exception:
            logger.exception("Failed marking promo winner (continuing)")

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
