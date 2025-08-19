import os
import logging
import asyncio
from typing import Dict, Any
from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse
import httpx
from dotenv import load_dotenv
from supabase import create_client, Client
from webhook_handler import handle_webhook_by_username, handle_webhook_by_webhook_id

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger("httpx").setLevel(logging.DEBUG)
logging.getLogger("httpcore").setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Environment variables
REQUIRED_ENV_VARS = [
    "SUPABASE_URL",
    "SUPABASE_KEY",
    "ADMIN_SECRET",
    "CENTRAL_BOT_TOKEN",
    "BUSINESS_BOT_TOKEN"
]

# Optional environment variables with defaults
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "https://default-webhook-url.com")  # Fallback URL

# Validate required environment variables
for var in REQUIRED_ENV_VARS:
    if not os.getenv(var):
        logger.error(f"Missing required environment variable: {var}")
        raise RuntimeError(f"{var} not set in .env")

# Warn if WEBHOOK_BASE_URL is using fallback
if WEBHOOK_BASE_URL == "https://default-webhook-url.com":
    logger.warning("WEBHOOK_BASE_URL not set, using fallback URL. Webhooks may not work correctly.")

ADMIN_SECRET = os.getenv("ADMIN_SECRET")

# Initialize Supabase client
supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

async def initialize_webhooks():
    """Initialize webhooks for all bots in Supabase."""
    try:
        def _get_bots():
            return supabase.table("bots").select("id, bot_username, bot_token").execute()
        bots = await asyncio.to_thread(_get_bots)
        bots_data = bots.data if hasattr(bots, "data") else bots.get("data", [])
        
        if not bots_data:
            logger.warning("No bots found in Supabase 'bots' table. Skipping webhook initialization.")
            return

        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
            for bot in bots_data:
                bot_id = bot["id"]
                bot_token = bot["bot_token"]
                bot_username = bot["bot_username"]
                webhook_url = f"{WEBHOOK_BASE_URL}/hook/{bot_id}"
                
                try:
                    response = await client.post(
                        f"https://api.telegram.org/bot{bot_token}/setWebhook",
                        json={"url": webhook_url, "allowed_updates": ["message", "callback_query"]}
                    )
                    response.raise_for_status()
                    logger.info(f"Webhook set for bot {bot_username} at {webhook_url}")
                except httpx.HTTPStatusError as e:
                    logger.error(f"Failed to set webhook for bot {bot_username}: HTTP {e.response.status_code} - {e.response.text}")
                except Exception as e:
                    logger.error(f"Failed to set webhook for bot {bot_username}: {str(e)}", exc_info=True)
    except Exception as e:
        logger.error(f"Failed to initialize webhooks: {str(e)}", exc_info=True)

async def startup_event():
    logger.info("Starting webhook initialization...")
    await initialize_webhooks()
    logger.info("Webhook initialization completed.")

async def lifespan(app: FastAPI):
    await startup_event()
    yield

# Initialize FastAPI app with lifespan
app = FastAPI(title="Multi-Business Telegram Bot", version="2.0.1", lifespan=lifespan)

# Central bot webhook route
@app.post("/hook/central_bot")
async def central_hook(request: Request):
    try:
        return await handle_webhook_by_username(request, "@CentralBot")
    except Exception as e:
        logger.error(f"Error in central_hook: {str(e)}", exc_info=True)
        return JSONResponse(status_code=200, content={"ok": True})

# Business bot webhook route
@app.post("/hook/business_bot")
async def business_hook(request: Request):
    try:
        return await handle_webhook_by_username(request, "@BusinessBot")
    except Exception as e:
        logger.error(f"Error in business_hook: {str(e)}", exc_info=True)
        return JSONResponse(status_code=200, content={"ok": True})

# Admin notification endpoint for city-based notifications
@app.post("/admin/notify/city")
async def admin_notify_city(request: Request, x_admin_secret: str = Header(...)):
    # Avoid logging full headers to prevent leaking sensitive info
    if x_admin_secret != ADMIN_SECRET:
        logger.error("Authentication failed: Invalid or missing admin secret")
        raise HTTPException(status_code=403, detail="Invalid or missing admin secret")
    
    try:
        payload = await request.json()
        city = payload.get("city")
        message = payload.get("message")
        if not city or not message:
            logger.error("Missing city or message in admin notify payload")
            raise HTTPException(status_code=400, detail="City and message are required")
        
        # Query users in the specified city (fixed field to 'location')
        def _get_users():
            return supabase.table("central_bot_leads").select("telegram_id").eq("is_draft", False).eq("location", city).execute()
        users = await asyncio.to_thread(_get_users)
        user_ids = [user["telegram_id"] for user in (users.data if hasattr(users, "data") else users.get("data", []))]
        
        if not user_ids:
            logger.info(f"No users found in city {city}")
            return {"status": "success", "notified_users": 0, "message": f"No users found in {city}"}

        # Send notifications
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
            for user_id in user_ids:
                try:
                    response = await client.post(
                        f"https://api.telegram.org/bot{os.getenv('CENTRAL_BOT_TOKEN')}/sendMessage",
                        json={"chat_id": user_id, "text": message, "parse_mode": "Markdown"}
                    )
                    response.raise_for_status()
                    logger.info(f"Sent notification to user {user_id} in city {city}")
                except httpx.HTTPStatusError as e:
                    logger.error(f"Failed to send notification to user {user_id}: HTTP {e.response.status_code} - {e.response.text}")
                except Exception as e:
                    logger.error(f"Failed to send notification to user {user_id}: {str(e)}", exc_info=True)
        
        return {"status": "success", "notified_users": len(user_ids)}
    except ValueError:
        logger.error("Invalid JSON payload in admin notify")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    except Exception as e:
        logger.error(f"Error in admin_notify_city: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

# Generic webhook routes for other bots
@app.post("/telegram/{bot_username}")
async def telegram_by_username(bot_username: str, request: Request):
    try:
        return await handle_webhook_by_username(request, bot_username)
    except Exception as e:
        logger.error(f"Error handling webhook for bot_username {bot_username}: {str(e)}", exc_info=True)
        return JSONResponse(status_code=200, content={"ok": True})

@app.post("/hook/{webhook_id}")
async def telegram_by_webhook_id(webhook_id: str, request: Request):
    try:
        return await handle_webhook_by_webhook_id(request, webhook_id)
    except Exception as e:
        logger.error(f"Error handling webhook for webhook_id {webhook_id}: {str(e)}", exc_info=True)
        return JSONResponse(status_code=200, content={"ok": True})

# Root endpoint
@app.get("/")
async def root():
    return {"message": "Multi-Business Telegram Bot is running!", "version": "2.0.1"}

# Health check
@app.get("/health")
async def health_check():
    try:
        # Basic Supabase connectivity check
        def _ping():
            return supabase.table("bots").select("id").limit(1).execute()
        await asyncio.to_thread(_ping)
        return {"status": "ok", "supabase": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}", exc_info=True)
        return {"status": "error", "detail": str(e)}

















'''
it work with ful code central bot
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
'''





