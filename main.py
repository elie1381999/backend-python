import os
import logging
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException, Header
import httpx
from supabase import create_client, Client
from central_bot import webhook_handler as central_webhook_handler
from business_bot import webhook_handler as business_webhook_handler
from notifications import notify_city
from webhook_handler import handle_webhook_by_username, handle_webhook_by_webhook_id

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

app = FastAPI(title="Multi-Business Telegram Bot")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
ADMIN_SECRET = os.getenv("ADMIN_SECRET")
CENTRAL_BOT_TOKEN = os.getenv("CENTRAL_BOT_TOKEN")
BUSINESS_BOT_TOKEN = os.getenv("BUSINESS_BOT_TOKEN")

if not all([SUPABASE_URL, SUPABASE_KEY, ADMIN_SECRET, CENTRAL_BOT_TOKEN, BUSINESS_BOT_TOKEN]):
    raise RuntimeError("SUPABASE_URL, SUPABASE_KEY, ADMIN_SECRET, CENTRAL_BOT_TOKEN, or BUSINESS_BOT_TOKEN not set in .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Central bot webhook route
@app.post("/hook/central_bot")
async def central_hook(request: Request):
    return await central_webhook_handler(request)

# Business bot webhook route
@app.post("/hook/business_bot")
async def business_hook(request: Request):
    return await business_webhook_handler(request)

# Admin notification endpoint for city-based notifications
@app.post("/admin/notify/city")
async def admin_notify_city(request: Request, x_admin_secret: str = Header(...)):
    logger.info(f"Request headers: {dict(request.headers)}")
    if not ADMIN_SECRET or x_admin_secret != ADMIN_SECRET:
        logger.error("Authentication failed: Invalid or missing admin secret")
        raise HTTPException(status_code=403, detail="Invalid or missing admin secret")
    try:
        payload = await request.json()
        city = payload.get("city")
        message = payload.get("message")
        if not city or not message:
            raise HTTPException(status_code=400, detail="City and message are required")
        result = await notify_city(city, message)
        return result
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

# Generic webhook routes for other bots
@app.post("/telegram/{bot_username}")
async def telegram_by_username(bot_username: str, request: Request):
    return await handle_webhook_by_username(request, bot_username)

@app.post("/hook/{webhook_id}")
async def telegram_by_webhook_id(webhook_id: str, request: Request):
    return await handle_webhook_by_webhook_id(request, webhook_id)

@app.get("/")
def root():
    return {"message": "Multi-Business Telegram Bot is running!"}


# Health check
@app.get("/health")
def health_check():
    return {"status": "ok"}





















'''import os
import logging
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException, Header
import httpx
from supabase import create_client, Client
from central_bot import webhook_handler as central_webhook_handler
from notifications import notify_city  # Assuming notifications.py exists
from webhook_handler import handle_webhook_by_username, handle_webhook_by_webhook_id

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

app = FastAPI(title="Multi-Salon Telegram Bot")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
ADMIN_SECRET = os.getenv("ADMIN_SECRET")
BOT_TOKEN = os.getenv("CENTRAL_BOT_TOKEN")

if not SUPABASE_URL or not SUPABASE_KEY or not ADMIN_SECRET or not BOT_TOKEN:
    raise RuntimeError("SUPABASE_URL, SUPABASE_KEY, ADMIN_SECRET, or CENTRAL_BOT_TOKEN not set in .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Central bot webhook route: put this BEFORE the generic one ---
@app.post("/hook/central_bot")
async def central_hook(request: Request):
    return await central_webhook_handler(request)

# Admin notification endpoint for city-based notifications
@app.post("/admin/notify/city")
async def admin_notify_city(request: Request, x_admin_secret: str = Header(...)):
    logger.info(f"Request headers: {dict(request.headers)}")
    if not ADMIN_SECRET or x_admin_secret != ADMIN_SECRET:
        logger.error("Authentication failed: Invalid or missing admin secret")
        raise HTTPException(status_code=403, detail="Invalid or missing admin secret")
    try:
        payload = await request.json()
        city = payload.get("city")
        message = payload.get("message")
        if not city or not message:
            raise HTTPException(status_code=400, detail="City and message are required")
        result = await notify_city(city, message)
        return result
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

# Admin approval endpoint for user VK account
@app.post("/admin/approve/{user_id}")
async def approve_user(user_id: int, x_admin_secret: str = Header(...)):
    if x_admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Invalid admin secret")

    try:
        # Find the user in central_bot_leads
        response = supabase.table("central_bot_leads").select("*").eq("telegram_id", user_id).eq("is_draft", False).execute()
        users = response.data if hasattr(response, "data") else response.get("data", [])
        if not users:
            raise HTTPException(status_code=404, detail=f"User with telegram_id {user_id} not found or not registered")

        user = users[0]
        if user.get("is_approved", False):
            raise HTTPException(status_code=400, detail=f"User with telegram_id {user_id} is already approved")

        # Update is_approved to True
        updated = supabase.table("central_bot_leads").update({"is_approved": True}).eq("telegram_id", user_id).execute()
        if not updated.data:
            raise HTTPException(status_code=500, detail="Failed to update user approval status")

        # Send Telegram notification to the user
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
            try:
                await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json={
                        "chat_id": user_id,
                        "text": "🎉 Your VK account has been verified and approved! You can now join giveaways using /join_giveaway."
                    }
                )
                logger.info(f"Approval notification sent to chat_id {user_id}")
            except Exception as e:
                logger.error(f"Failed to send approval notification to chat_id {user_id}: {e}")
                raise HTTPException(status_code=500, detail="Failed to send approval notification")

        return {"status": f"User {user_id} approved successfully"}
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Failed to approve user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Route 1: Telegram hits this when bot username is used in webhook URL
@app.post("/telegram/{bot_username}")
async def telegram_by_username(bot_username: str, request: Request):
    return await handle_webhook_by_username(request, bot_username)

# Route 2: Telegram hits this when a webhook_id is used instead
@app.post("/hook/{webhook_id}")
async def telegram_by_webhook_id(webhook_id: str, request: Request):
    return await handle_webhook_by_webhook_id(request, webhook_id)

@app.get("/health")
def health_check():
    return {"status": "ok"}

'''










'''import os
import logging
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
from webhook_handler import handle_webhook_by_username, handle_webhook_by_webhook_id
from central_bot import webhook_handler as central_webhook_handler
from notifications import notify_city

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

app = FastAPI(title="Multi-Salon Telegram Bot")

ADMIN_SECRET = os.getenv("ADMIN_SECRET")
logger.info(f"Loaded ADMIN_SECRET: {ADMIN_SECRET}")
if not ADMIN_SECRET:
    raise RuntimeError("ADMIN_SECRET environment variable must be set")

# --- Central bot webhook route: put this BEFORE the generic one ---
@app.post("/hook/central_bot")
async def central_hook(request: Request):
    return await central_webhook_handler(request)

@app.post("/admin/notify/city")
async def admin_notify_city(request: Request):
    logger.info(f"Request headers: {dict(request.headers)}")
    secret = request.headers.get("x-admin-secret")
    logger.info(f"Received x-admin-secret: {secret}, Expected: {ADMIN_SECRET}")
    if not ADMIN_SECRET or secret != ADMIN_SECRET:
        logger.error("Authentication failed: Invalid or missing admin secret")
        raise HTTPException(status_code=403, detail="Invalid or missing admin secret")
    try:
        payload = await request.json()
        city = payload.get("city")
        message = payload.get("message")
        if not city or not message:
            raise HTTPException(status_code=400, detail="City and message are required")
        result = await notify_city(city, message)
        return result
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

# Route 1: Telegram hits this when bot username is used in webhook URL
@app.post("/telegram/{bot_username}")
async def telegram_by_username(bot_username: str, request: Request):
    return await handle_webhook_by_username(request, bot_username)

# Route 2: Telegram hits this when a webhook_id is used instead
# (kept last to avoid catching specific central route)
@app.post("/hook/{webhook_id}")
async def telegram_by_webhook_id(webhook_id: str, request: Request):
    return await handle_webhook_by_webhook_id(request, webhook_id)

@app.get("/health")
def health_check():
    return {"status": "ok"}'''
