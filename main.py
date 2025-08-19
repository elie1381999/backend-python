import os
import logging
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException, Header
import httpx
from supabase import create_client, Client

# Debug: Print directory contents to confirm file presence
print("Directory contents:", os.listdir('.'))

# Import webhook handlers
try:
    from central_bot import webhook_handler as central_webhook_handler
except ImportError as e:
    print(f"Failed to import central_bot: {e}")
    raise
try:
    from business_bot import webhook_handler as business_webhook_handler
except ImportError as e:
    print(f"Failed to import business_bot: {e}")
    raise
try:
    from notifications import notify_city
except ImportError as e:
    print(f"Failed to import notifications: {e}")
    raise
try:
    from webhook_handler import handle_webhook_by_username, handle_webhook_by_webhook_id
except ImportError as e:
    print(f"Failed to import webhook_handler: {e}")
    raise

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
it work the code 1 ful page 1
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





