import os
import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
from fastapi import FastAPI, Request
from starlette.responses import PlainTextResponse, Response

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logging.getLogger("httpx").setLevel(logging.DEBUG)
logging.getLogger("httpcore").setLevel(logging.DEBUG)
logger = logging.getLogger("business_bot")

# Initialize FastAPI app
app = FastAPI()

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("BUSINESS_BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")

if not all([BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY, ADMIN_CHAT_ID]):
    raise RuntimeError("Missing required environment variables")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Constants
USER_STATES: Dict[int, Dict[str, Any]] = {}
STATE_TTL_SECONDS = 30 * 60  # 30 minutes
CATEGORIES = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining"]
WEEK_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
MAX_DISCOUNT_PERCENTAGE = 100
MIN_DISCOUNT_PERCENTAGE = 1
MAX_NAME_LENGTH = 100
MAX_DESCRIPTION_LENGTH = 500

def now_iso():
    """Return current UTC time in ISO format."""
    return datetime.now(timezone.utc).isoformat()

def set_state(chat_id: int, state: Dict[str, Any]):
    """Set user state with timestamp."""
    state["updated_at"] = now_iso()
    USER_STATES[chat_id] = state
    logger.debug(f"Set state for chat_id {chat_id}: {state}")

def get_state(chat_id: int) -> Optional[Dict[str, Any]]:
    """Get user state, expire if too old."""
    state = USER_STATES.get(chat_id)
    if not state:
        logger.debug(f"No state found for chat_id {chat_id}")
        return None
    try:
        updated = datetime.fromisoformat(state.get("updated_at"))
        if (datetime.now(timezone.utc) - updated).total_seconds() > STATE_TTL_SECONDS:
            USER_STATES.pop(chat_id, None)
            logger.info(f"Expired state for chat_id {chat_id}")
            return None
    except Exception as e:
        logger.error(f"Invalid state timestamp for chat_id {chat_id}: {str(e)}")
        USER_STATES.pop(chat_id, None)
        return None
    return state

async def cleanup_states():
    """Periodically clean up expired states."""
    while True:
        current_time = datetime.now(timezone.utc)
        expired = [
            chat_id for chat_id, state in USER_STATES.items()
            if (current_time - datetime.fromisoformat(state.get("updated_at"))).total_seconds() > STATE_TTL_SECONDS
        ]
        for chat_id in expired:
            USER_STATES.pop(chat_id, None)
            logger.info(f"Cleaned up expired state for chat_id {chat_id}")
        await asyncio.sleep(60)  # Check every minute

async def log_error_to_supabase(error_message: str):
    """Log errors to Supabase bot_errors table."""
    payload = {
        "error": error_message[:1000],
        "created_at": now_iso(),
        "bot": "business_bot"
    }
    try:
        def _ins():
            return supabase.table("bot_errors").insert(payload).execute()
        resp = await asyncio.to_thread(_ins)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if data:
            logger.info(f"Logged error to Supabase: {error_message}")
        else:
            logger.error(f"Failed to log error to Supabase: {error_message}")
    except Exception as e:
        logger.error(f"Failed to log error to Supabase: {str(e)}", exc_info=True)

async def send_message(chat_id: int, text: str, reply_markup: Optional[dict] = None, retries: int = 3):
    """Send a message to a Telegram chat with retry logic."""
    if not isinstance(chat_id, int) or chat_id == 0:
        logger.error(f"Invalid chat_id: {chat_id}")
        await log_error_to_supabase(f"Invalid chat_id: {chat_id}")
        return {"ok": False, "error": "Invalid chat_id"}
    
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        payload = {"chat_id": chat_id, "text": text[:4096], "parse_mode": "Markdown"}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        for attempt in range(retries):
            try:
                logger.debug(f"Sending message to chat_id {chat_id} (attempt {attempt + 1}): {text[:100]}...")
                response = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Sent message to chat_id {chat_id}: {text[:100]}...")
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to send message: HTTP {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 400 and "chat not found" in e.response.text.lower():
                    await log_error_to_supabase(f"Chat not found for chat_id {chat_id}")
                    return {"ok": False, "error": "Chat not found"}
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
        await log_error_to_supabase(f"Failed to send message to chat_id {chat_id} after {retries} attempts")
        return {"ok": False, "error": "Max retries reached"}

async def edit_message(chat_id: int, message_id: int, text: str, reply_markup: Optional[dict] = None, retries: int = 3):
    """Edit an existing message in a Telegram chat."""
    if not isinstance(chat_id, int) or chat_id == 0 or not isinstance(message_id, int):
        logger.error(f"Invalid parameters: chat_id={chat_id}, message_id={message_id}")
        await log_error_to_supabase(f"Invalid parameters: chat_id={chat_id}, message_id={message_id}")
        return {"ok": False, "error": "Invalid parameters"}
    
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        payload = {"chat_id": chat_id, "message_id": message_id, "text": text[:4096], "parse_mode": "Markdown"}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        for attempt in range(retries):
            try:
                logger.debug(f"Editing message {message_id} in chat_id {chat_id} (attempt {attempt + 1}): {text[:100]}...")
                response = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Edited message {message_id} in chat_id {chat_id}: {text[:100]}...")
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to edit message: HTTP {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 400 and "chat not found" in e.response.text.lower():
                    await log_error_to_supabase(f"Chat not found for chat_id {chat_id}")
                    return {"ok": False, "error": "Chat not found"}
                if e.response.status_code == 429:
                    retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    await asyncio.sleep(retry_after)
                    continue
                return {"ok": False, "error": f"HTTP {e.response.status_code}"}
            except Exception as e:
                logger.error(f"Failed to edit message: {str(e)}", exc_info=True)
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        logger.error(f"Failed to edit message {message_id} in chat_id {chat_id} after {retries} attempts")
        await log_error_to_supabase(f"Failed to edit message {message_id} in chat_id {chat_id} after {retries} attempts")
        return {"ok": False, "error": "Max retries reached"}

async def send_admin_message(text: str, reply_markup: Optional[dict] = None, retries: int = 3):
    """Send a message to the admin chat."""
    try:
        admin_chat_id = int(ADMIN_CHAT_ID)
    except ValueError:
        logger.error(f"Invalid ADMIN_CHAT_ID: {ADMIN_CHAT_ID}")
        await log_error_to_supabase(f"Invalid ADMIN_CHAT_ID: {ADMIN_CHAT_ID}")
        return {"ok": False, "error": "Invalid ADMIN_CHAT_ID"}
    
    return await send_message(admin_chat_id, text, reply_markup, retries)

async def supabase_insert_return(table: str, payload: dict) -> Optional[Dict[str, Any]]:
    """Insert data into Supabase and return the inserted record."""
    payload['created_at'] = now_iso()
    payload['updated_at'] = now_iso()
    try:
        def _ins():
            return supabase.table(table).insert(payload).execute()
        resp = await asyncio.to_thread(_ins)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.error(f"Failed to insert into {table}: no data returned")
            await log_error_to_supabase(f"Failed to insert into {table}: no data returned")
            return None
        logger.info(f"Inserted into {table}: {data[0]}")
        return data[0]
    except Exception as e:
        logger.error(f"supabase_insert_return failed for table {table}: {str(e)}", exc_info=True)
        error_message = str(e)
        if "Category" in error_message and "is not associated with business_id" in error_message:
            return {"error": "invalid_category", "message": error_message}
        if "foreign_key_violation" in error_message.lower():
            return {"error": "foreign_key_violation", "message": error_message}
        await log_error_to_supabase(f"supabase_insert_return failed for table {table}: {str(e)}")
        return None

async def supabase_update_by_id_return(table: str, entry_id: str, payload: dict) -> Optional[Dict[str, Any]]:
    """Update a Supabase record by ID and return the updated record."""
    payload['updated_at'] = now_iso()
    try:
        def _upd():
            return supabase.table(table).update(payload).eq("id", entry_id).execute()
        resp = await asyncio.to_thread(_upd)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.error(f"Failed to update {table} with id {entry_id}: no data returned")
            await log_error_to_supabase(f"Failed to update {table} with id {entry_id}: no data returned")
            return None
        logger.info(f"Updated {table} with id {entry_id}: {data[0]}")
        return data[0]
    except Exception as e:
        logger.error(f"supabase_update_by_id_return failed for table {table}, id {entry_id}: {str(e)}", exc_info=True)
        error_message = str(e)
        if "Category" in error_message and "is not associated with business_id" in error_message:
            return {"error": "invalid_category", "message": error_message}
        if "foreign_key_violation" in error_message.lower():
            return {"error": "foreign_key_violation", "message": error_message}
        await log_error_to_supabase(f"supabase_update_by_id_return failed for table {table}, id {entry_id}: {str(e)}")
        return None

async def supabase_delete_by_id(table: str, entry_id: str) -> bool:
    """Delete a record from Supabase by ID."""
    try:
        def _del():
            return supabase.table(table).delete().eq("id", entry_id).execute()
        resp = await asyncio.to_thread(_del)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if data:
            logger.info(f"Deleted from {table} with id {entry_id}")
            return True
        logger.error(f"Failed to delete from {table} with id {entry_id}: no data returned")
        await log_error_to_supabase(f"Failed to delete from {table} with id {entry_id}: no data returned")
        return False
    except Exception as e:
        logger.error(f"supabase_delete_by_id failed for table {table}, id {entry_id}: {str(e)}", exc_info=True)
        await log_error_to_supabase(f"supabase_delete_by_id failed for table {table}, id {entry_id}: {str(e)}")
        return False

async def supabase_find_business(chat_id: int) -> Optional[Dict[str, Any]]:
    """Find a business by chat_id in Supabase."""
    try:
        def _q():
            return supabase.table("businesses").select("*, business_categories(category)").eq("telegram_id", chat_id).limit(1).execute()
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.info(f"No business found for chat_id {chat_id}")
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_find_business failed for chat_id {chat_id}: {str(e)}", exc_info=True)
        await log_error_to_supabase(f"supabase_find_business failed for chat_id {chat_id}: {str(e)}")
        return None

async def supabase_get_business_categories(business_id: str) -> List[str]:
    """Get categories for a business."""
    try:
        def _q():
            return supabase.table("business_categories").select("category").eq("business_id", business_id).execute()
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data", [])
        return [item["category"] for item in data]
    except Exception as e:
        logger.error(f"supabase_get_business_categories failed for business_id {business_id}: {str(e)}")
        await log_error_to_supabase(f"supabase_get_business_categories failed for business_id {business_id}: {str(e)}")
        return []

async def supabase_get_services(business_id: str) -> List[Dict[str, Any]]:
    """Get services for a business."""
    try:
        def _q():
            return supabase.table("services").select("*").eq("business_id", business_id).execute()
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data", [])
        return data
    except Exception as e:
        logger.error(f"supabase_get_services failed for business_id {business_id}: {str(e)}")
        await log_error_to_supabase(f"supabase_get_services failed for business_id {business_id}: {str(e)}")
        return []

async def supabase_get_discounts(business_id: str) -> List[Dict[str, Any]]:
    """Get discounts for a business."""
    try:
        def _q():
            return supabase.table("discounts").select("*").eq("business_id", business_id).execute()
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data", [])
        return data
    except Exception as e:
        logger.error(f"supabase_get_discounts failed for business_id {business_id}: {str(e)}")
        await log_error_to_supabase(f"supabase_get_discounts failed for business_id {business_id}: {str(e)}")
        return []

async def create_category_keyboard(selected: List[str] = []) -> dict:
    """Create inline keyboard for category selection."""
    buttons = []
    for category in CATEGORIES:
        prefix = "✅ " if category in selected else ""
        buttons.append([{"text": f"{prefix}{category}", "callback_data": f"category:{category}"}])
    buttons.append([{"text": "Confirm", "callback_data": "category:confirm"}])
    return {"inline_keyboard": buttons}

async def create_workdays_keyboard(selected: list) -> dict:
    """Create inline keyboard for work days selection."""
    buttons = []
    for day in WEEK_DAYS:
        prefix = "✅ " if day in selected else ""
        buttons.append([{"text": f"{prefix}{day}", "callback_data": f"workday:{day}"}])
    buttons.append([{"text": "Confirm", "callback_data": "workday:confirm"}])
    return {"inline_keyboard": buttons}

async def create_yes_no_keyboard(prefix: str) -> dict:
    """Create inline yes/no keyboard."""
    return {
        "inline_keyboard": [
            [
                {"text": "Yes", "callback_data": f"{prefix}:yes"},
                {"text": "No", "callback_data": f"{prefix}:no"}
            ]
        ]
    }

async def create_service_category_keyboard(business_id: str) -> dict:
    """Create inline keyboard for selecting a service category."""
    categories = await supabase_get_business_categories(business_id)
    if not categories:
        return {"inline_keyboard": [[{"text": "No categories available", "callback_data": "none"}]]}
    buttons = [
        [{"text": category, "callback_data": f"service_category:{category}"}]
        for category in categories
    ]
    buttons.append([{"text": "Skip", "callback_data": "service_category:skip"}])
    return {"inline_keyboard": buttons}

async def create_service_selection_keyboard(services: List[Dict[str, Any]]) -> dict:
    """Create inline keyboard for selecting a service to delete."""
    if not services:
        return {"inline_keyboard": [[{"text": "No services available", "callback_data": "none"}]]}
    buttons = [
        [{"text": f"{service['name']} ({service['category']}): ${service['price']}", "callback_data": f"delete_service:{service['id']}"}]
        for service in services
    ]
    buttons.append([{"text": "Cancel", "callback_data": "delete_service:cancel"}])
    return {"inline_keyboard": buttons}

async def create_discount_selection_keyboard(discounts: List[Dict[str, Any]]) -> dict:
    """Create inline keyboard for selecting a discount to delete."""
    if not discounts:
        return {"inline_keyboard": [[{"text": "No discounts available", "callback_data": "none"}]]}
    buttons = [
        [{"text": f"{discount['name']} ({discount['category']}, {discount['discount_percentage']}%): {'Active' if discount['active'] else 'Pending/Inactive'}", "callback_data": f"delete_discount:{discount['id']}"}]
        for discount in discounts
    ]
    buttons.append([{"text": "Cancel", "callback_data": "delete_discount:cancel"}])
    return {"inline_keyboard": buttons}

async def initialize_bot():
    """Initialize bot by setting webhook and commands."""
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        try:
            # Test admin chat_id
            test_response = await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": int(ADMIN_CHAT_ID),
                    "text": "Business Bot initialized successfully",
                    "parse_mode": "Markdown"
                }
            )
            test_response.raise_for_status()
            logger.info(f"Admin chat_id {ADMIN_CHAT_ID} verified successfully")
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to verify ADMIN_CHAT_ID {ADMIN_CHAT_ID}: HTTP {e.response.status_code} - {e.response.text}")
            await log_error_to_supabase(f"Failed to verify ADMIN_CHAT_ID {ADMIN_CHAT_ID}: {e.response.text}")
            if "chat not found" in e.response.text.lower():
                raise RuntimeError(f"ADMIN_CHAT_ID {ADMIN_CHAT_ID} is invalid. Please verify using /getUpdates.")
        except Exception as e:
            logger.error(f"Failed to verify ADMIN_CHAT_ID {ADMIN_CHAT_ID}: {str(e)}")
            await log_error_to_supabase(f"Failed to verify ADMIN_CHAT_ID {ADMIN_CHAT_ID}: {str(e)}")
            raise

        try:
            # Set webhook
            webhook_url = "https://backend-python-6q8a.onrender.com/hook/business_bot"
            response = await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
                json={"url": webhook_url, "allowed_updates": ["message", "callback_query"]}
            )
            response.raise_for_status()
            logger.info(f"Webhook set to {webhook_url}")
            
            # Set menu button
            await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/setChatMenuButton",
                json={"menu_button": {"type": "commands"}}
            )
            logger.info("Set menu button")
            
            # Set bot commands
            commands = [
                {"command": "start", "description": "Start the bot"},
                {"command": "register", "description": "Register your business"},
                {"command": "add_discount", "description": "Add a discount"},
                {"command": "delete_discount", "description": "Delete a discount"},
                {"command": "edit_business", "description": "Edit your business details"},
                {"command": "list_services", "description": "List your services"},
                {"command": "list_discounts", "description": "List your discounts"},
                {"command": "cancel", "description": "Cancel current operation"}
            ]
            await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands",
                json={"commands": commands}
            )
            logger.info("Set bot commands")
        except Exception as e:
            logger.error(f"Failed to initialize bot: {str(e)}", exc_info=True)
            await log_error_to_supabase(f"Failed to initialize bot: {str(e)}")
            raise

async def webhook_handler(request: Request):
    """Handle incoming Telegram updates."""
    try:
        update = await request.json()
        if not update:
            logger.error("Received empty update from Telegram")
            return Response(status_code=200)
        logger.info(f"Received update: {json.dumps(update, indent=2)}")
        message = update.get("message")
        if message:
            await handle_message_update(message)
        callback_query = update.get("callback_query")
        if callback_query:
            await handle_callback_query(callback_query)
        return Response(status_code=200)
    except json.JSONDecodeError:
        logger.error("Invalid JSON in webhook", exc_info=True)
        await log_error_to_supabase("Invalid JSON in webhook")
        return Response(status_code=200)
    except Exception as e:
        logger.error(f"Error processing webhook update: {str(e)}", exc_info=True)
        await log_error_to_supabase(f"Error processing webhook update: {str(e)}")
        return Response(status_code=200)

async def handle_message_update(message: Dict[str, Any]):
    """Handle incoming messages."""
    chat_id = message.get("chat", {}).get("id")
    if not chat_id:
        logger.error("No chat_id in message")
        await log_error_to_supabase("No chat_id in message")
        return {"ok": True}
    text = (message.get("text") or "").strip()
    logger.info(f"Handling message from chat_id {chat_id}: {text}")
    state = get_state(chat_id) or {}

    # /start
    if text.lower() == "/start":
        business = await supabase_find_business(chat_id)
        if business:
            if business["status"] == "approved":
                await send_message(chat_id, "Your business is approved! Use /add_discount, /delete_discount, /edit_business, /list_services, or /list_discounts.")
            else:
                await send_message(chat_id, "Your business is pending approval. We'll notify you soon!")
        else:
            await send_message(chat_id, "Welcome to the Business Bot! Register your business with /register.")
        USER_STATES.pop(chat_id, None)
        return {"ok": True}

    # /cancel
    if text.lower() == "/cancel":
        if state:
            USER_STATES.pop(chat_id, None)
            await send_message(chat_id, "Current operation cancelled.")
        else:
            await send_message(chat_id, "No operation to cancel.")
        return {"ok": True}

    # /register
    if text.lower() == "/register":
        business = await supabase_find_business(chat_id)
        if business:
            await send_message(chat_id, "You’ve already registered! Use /edit_business to update details or /add_discount to add offers.")
            return {"ok": True}
        state = {
            "stage": "awaiting_name",
            "data": {
                "telegram_id": chat_id,
                "categories": [],
                "work_days": [],
                "website": None,
                "description": None,
                "services": []
            },
            "entry_id": None
        }
        await send_message(chat_id, f"Enter your business name (max {MAX_NAME_LENGTH} characters):")
        set_state(chat_id, state)
        return {"ok": True}

    # /add_discount
    if text.lower() == "/add_discount":
        business = await supabase_find_business(chat_id)
        if not business:
            await send_message(chat_id, "Please register your business first with /register.")
            return {"ok": True}
        if business["status"] != "approved":
            await send_message(chat_id, "Your business is not yet approved.")
            return {"ok": True}
        categories = await supabase_get_business_categories(business["id"])
        if not categories:
            await send_message(chat_id, "No categories found for your business. Add categories using /edit_business.")
            return {"ok": True}
        state = {
            "stage": "awaiting_discount_name",
            "data": {"business_id": business["id"], "business_name": business["name"]},
            "entry_id": None
        }
        await send_message(chat_id, f"Enter the discount name (e.g., '20% Off Nail Art', max {MAX_NAME_LENGTH} characters):")
        set_state(chat_id, state)
        return {"ok": True}

    # /delete_discount
    if text.lower() == "/delete_discount":
        business = await supabase_find_business(chat_id)
        if not business:
            await send_message(chat_id, "Please register your business first with /register.")
            return {"ok": True}
        if business["status"] != "approved":
            await send_message(chat_id, "Your business is not yet approved.")
            return {"ok": True}
        discounts = await supabase_get_discounts(business["id"])
        if not discounts:
            await send_message(chat_id, "No discounts found. Add discounts using /add_discount.")
            return {"ok": True}
        state = {
            "stage": "awaiting_discount_deletion",
            "data": {"business_id": business["id"]},
            "entry_id": None
        }
        resp = await send_message(
            chat_id,
            "Select a discount to delete:",
            reply_markup=await create_discount_selection_keyboard(discounts)
        )
        if resp.get("ok"):
            state["temp_message_id"] = resp["result"]["message_id"]
        set_state(chat_id, state)
        return {"ok": True}

    # /edit_business
    if text.lower() == "/edit_business":
        business = await supabase_find_business(chat_id)
        if not business:
            await send_message(chat_id, "Please register your business first with /register.")
            return {"ok": True}
        state = {
            "stage": "edit_choose_field",
            "data": {"business_id": business["id"], "telegram_id": chat_id},
            "entry_id": business["id"]
        }
        await send_message(
            chat_id,
            "Choose a field to edit:",
            reply_markup={
                "inline_keyboard": [
                    [{"text": "Name", "callback_data": "edit_field:name"}],
                    [{"text": "Categories", "callback_data": "edit_field:categories"}],
                    [{"text": "Phone Number", "callback_data": "edit_field:phone_number"}],
                    [{"text": "Location", "callback_data": "edit_field:location"}],
                    [{"text": "Work Days", "callback_data": "edit_field:work_days"}],
                    [{"text": "Services", "callback_data": "edit_field:services"}],
                    [{"text": "Delete Services", "callback_data": "edit_field:delete_services"}],
                    [{"text": "Website", "callback_data": "edit_field:website"}],
                    [{"text": "Description", "callback_data": "edit_field:description"}]
                ]
            }
        )
        set_state(chat_id, state)
        return {"ok": True}

    # /list_services
    if text.lower() == "/list_services":
        business = await supabase_find_business(chat_id)
        if not business:
            await send_message(chat_id, "Please register your business first with /register.")
            return {"ok": True}
        services = await supabase_get_services(business["id"])
        if not services:
            await send_message(chat_id, "No services registered. Add services using /edit_business.")
        else:
            services_text = "\n".join([f"- {service['name']} ({service['category']}): ${service['price']}" for service in services])
            await send_message(chat_id, f"Your services:\n{services_text}")
        return {"ok": True}

    # /list_discounts
    if text.lower() == "/list_discounts":
        business = await supabase_find_business(chat_id)
        if not business:
            await send_message(chat_id, "Please register your business first with /register.")
            return {"ok": True}
        discounts = await supabase_get_discounts(business["id"])
        if not discounts:
            await send_message(chat_id, "No discounts registered. Add discounts using /add_discount.")
            return {"ok": True}
        offers_text = [f"- {d['name']} ({d['category']}, {d['discount_percentage']}%): {'Active' if d['active'] else 'Pending/Inactive'}" for d in discounts]
        offers_text_joined = '\n'.join(offers_text)
        await send_message(chat_id, f"Your discounts:\n{offers_text_joined}")
        return {"ok": True}

    # Registration steps
    if state.get("stage") == "awaiting_name":
        if len(text) > MAX_NAME_LENGTH:
            await send_message(chat_id, f"Business name too long. Please use {MAX_NAME_LENGTH} characters or fewer:")
            return {"ok": True}
        state["data"]["name"] = text
        state["data"]["categories"] = []
        resp = await send_message(
            chat_id,
            f"Selected categories: None\nSelect business categories (select all that apply, then Confirm):",
            reply_markup=await create_category_keyboard()
        )
        if resp.get("ok"):
            state["temp_message_id"] = resp["result"]["message_id"]
        state["stage"] = "awaiting_categories"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_phone":
        if not re.match(r"^\+\d{10,15}$", text):
            await send_message(chat_id, "Please enter a valid phone number starting with + (e.g., +1234567890):")
            return {"ok": True}
        state["data"]["phone_number"] = text
        await send_message(chat_id, "Enter your business location (e.g., 123 Main St, City):")
        state["stage"] = "awaiting_location"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_location":
        state["data"]["location"] = text
        selected = state["data"].get("work_days", [])
        resp = await send_message(
            chat_id,
            f"Selected work days: {', '.join(selected) or 'None'}\nSelect work days:",
            reply_markup=await create_workdays_keyboard(selected)
        )
        if resp.get("ok"):
            state["temp_message_id"] = resp["result"]["message_id"]
        state["stage"] = "awaiting_work_days"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_website":
        if text.lower() == "none":
            state["data"]["website"] = None
        else:
            if not re.match(r"^(https?://)?[\w\-]+(\.[\w\-]+)+[/#?]?.*$", text):
                await send_message(chat_id, "Please enter a valid URL (e.g., https://example.com) or 'none':")
                return {"ok": True}
            state["data"]["website"] = text
        await send_message(chat_id, f"Enter a brief business description (max {MAX_DESCRIPTION_LENGTH} characters, or 'none'):")
        state["stage"] = "awaiting_description"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_description":
        if len(text) > MAX_DESCRIPTION_LENGTH:
            await send_message(chat_id, f"Description too long. Please use {MAX_DESCRIPTION_LENGTH} characters or fewer:")
            return {"ok": True}
        state["data"]["description"] = text if text.lower() != "none" else None
        business_id = state["data"].get("business_id", state.get("entry_id"))
        if not business_id:
            business = await supabase_insert_return("businesses", {
                "name": state["data"]["name"],
                "phone_number": state["data"]["phone_number"],
                "location": state["data"]["location"],
                "work_days": state["data"]["work_days"],
                "website": state["data"]["website"],
                "description": state["data"]["description"],
                "telegram_id": state["data"]["telegram_id"],
                "status": "pending"
            })
            if isinstance(business, dict) and business.get("error") == "foreign_key_violation":
                await send_message(chat_id, f"Database error: {business['message']}. Please try again or contact support.")
                USER_STATES.pop(chat_id, None)
                return {"ok": True}
            if not business:
                await send_message(chat_id, "Failed to create business. Please try again.")
                USER_STATES.pop(chat_id, None)
                return {"ok": True}
            business_id = business["id"]
            state["data"]["business_id"] = business_id
            state["entry_id"] = business_id
        categories = await supabase_get_business_categories(business_id)
        if not categories:
            await send_message(chat_id, "No categories found. Please add at least one category using /edit_business.")
            USER_STATES.pop(chat_id, None)
            return {"ok": True}
        resp = await send_message(
            chat_id,
            "Choose the category for your first service (or /skip if none):",
            reply_markup=await create_service_category_keyboard(business_id)
        )
        if resp.get("ok"):
            state["temp_message_id"] = resp["result"]["message_id"]
        state["stage"] = "awaiting_service_category"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_service_category":
        if text.lower() == "/skip":
            await submit_business_registration(chat_id, state)
            return {"ok": True}
        await send_message(chat_id, "Please select a category from the keyboard or use /skip to submit.")
        return {"ok": True}

    if state.get("stage") == "awaiting_service_name":
        if len(text) > MAX_NAME_LENGTH:
            await send_message(chat_id, f"Service name too long. Please use {MAX_NAME_LENGTH} characters or fewer:")
            return {"ok": True}
        state["temp_service_name"] = text
        await send_message(chat_id, f"Enter price for {text} (number in USD, e.g., 50.00):")
        state["stage"] = "awaiting_service_price"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_service_price":
        try:
            price = float(text)
            if price <= 0:
                raise ValueError("Price must be positive")
            service_name = state.get("temp_service_name")
            service_category = state.get("temp_service_category")
            business_id = state["data"].get("business_id", state.get("entry_id"))
            if not (service_name and service_category and business_id):
                await send_message(chat_id, "Error: Missing service details. Please start over with /register or /edit_business.")
                USER_STATES.pop(chat_id, None)
                return {"ok": True}
            service = await supabase_insert_return("services", {
                "business_id": business_id,
                "name": service_name,
                "price": price,
                "category": service_category
            })
            if isinstance(service, dict) and service.get("error") == "invalid_category":
                await send_message(chat_id, f"Error: {service['message']}. Please choose a valid category.")
                resp = await send_message(
                    chat_id,
                    "Choose the category for the service:",
                    reply_markup=await create_service_category_keyboard(business_id)
                )
                if resp.get("ok"):
                    state["temp_message_id"] = resp["result"]["message_id"]
                state["stage"] = "awaiting_service_category"
                set_state(chat_id, state)
                return {"ok": True}
            if isinstance(service, dict) and service.get("error") == "foreign_key_violation":
                await send_message(chat_id, f"Database error: {service['message']}. Please try again or contact support.")
                USER_STATES.pop(chat_id, None)
                return {"ok": True}
            if service:
                state["data"]["services"].append({
                    "name": service_name,
                    "price": price,
                    "category": service_category
                })
                await send_message(
                    chat_id,
                    f"Added service: {service_name} ({service_category}): ${price}. Add another service?",
                    reply_markup=await create_yes_no_keyboard("add_service")
                )
                state["stage"] = "awaiting_add_another_service"
                del state["temp_service_name"]
                del state["temp_service_category"]
                set_state(chat_id, state)
            else:
                await send_message(chat_id, "Failed to add service due to a database error. Please try again.")
        except ValueError:
            await send_message(chat_id, "Please enter a valid number for price (e.g., 50.00).")
        return {"ok": True}

    # Discount steps
    if state.get("stage") == "awaiting_discount_name":
        if len(text) > MAX_NAME_LENGTH:
            await send_message(chat_id, f"Name too long. Please use {MAX_NAME_LENGTH} characters or fewer:")
            return {"ok": True}
        state["data"]["name"] = text
        business_id = state["data"]["business_id"]
        categories = await supabase_get_business_categories(business_id)
        if not categories:
            await send_message(chat_id, "No categories found for your business. Add categories using /edit_business.")
            USER_STATES.pop(chat_id, None)
            return {"ok": True}
        resp = await send_message(
            chat_id,
            "Choose the category for this discount:",
            reply_markup=await create_service_category_keyboard(business_id)
        )
        if resp.get("ok"):
            state["temp_message_id"] = resp["result"]["message_id"]
        state["stage"] = "awaiting_discount_category"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_discount_category":
        await send_message(chat_id, "Please select a category from the keyboard.")
        return {"ok": True}

    if state.get("stage") == "awaiting_discount_percentage":
        try:
            percentage = int(text)
            if not (MIN_DISCOUNT_PERCENTAGE <= percentage <= MAX_DISCOUNT_PERCENTAGE):
                await send_message(
                    chat_id,
                    f"Percentage must be between {MIN_DISCOUNT_PERCENTAGE}% and {MAX_DISCOUNT_PERCENTAGE}%. Please try again:"
                )
                return {"ok": True}
            state["data"]["discount_percentage"] = percentage
            await submit_discount(chat_id, state)
        except ValueError:
            await send_message(chat_id, "Please enter a valid integer for percentage (e.g., 20).")
        return {"ok": True}

    # Edit business steps
    if state.get("stage") == "edit_name":
        if len(text) > MAX_NAME_LENGTH:
            await send_message(chat_id, f"Business name too long. Please use {MAX_NAME_LENGTH} characters or fewer:")
            return {"ok": True}
        updated = await supabase_update_by_id_return("businesses", state["entry_id"], {"name": text})
        if isinstance(updated, dict) and updated.get("error") == "foreign_key_violation":
            await send_message(chat_id, f"Database error: {updated['message']}. Please try again or contact support.")
            USER_STATES.pop(chat_id, None)
            return {"ok": True}
        if updated:
            await send_message(chat_id, "Business name updated successfully!")
            await send_admin_message(f"Business updated:\nID: {state['entry_id']}\nNew Name: {text}")
            USER_STATES.pop(chat_id, None)
        else:
            await send_message(chat_id, "Failed to update business name due to a database error. Please try again.")
        return {"ok": True}

    if state.get("stage") == "edit_phone":
        if not re.match(r"^\+\d{10,15}$", text):
            await send_message(chat_id, "Please enter a valid phone number starting with + (e.g., +1234567890):")
            return {"ok": True}
        updated = await supabase_update_by_id_return("businesses", state["entry_id"], {"phone_number": text})
        if isinstance(updated, dict) and updated.get("error") == "foreign_key_violation":
            await send_message(chat_id, f"Database error: {updated['message']}. Please try again or contact support.")
            USER_STATES.pop(chat_id, None)
            return {"ok": True}
        if updated:
            await send_message(chat_id, "Phone number updated successfully!")
            await send_admin_message(f"Business updated:\nID: {state['entry_id']}\nNew Phone: {text}")
            USER_STATES.pop(chat_id, None)
        else:
            await send_message(chat_id, "Failed to update phone number due to a database error. Please try again.")
        return {"ok": True}

    if state.get("stage") == "edit_location":
        updated = await supabase_update_by_id_return("businesses", state["entry_id"], {"location": text})
        if isinstance(updated, dict) and updated.get("error") == "foreign_key_violation":
            await send_message(chat_id, f"Database error: {updated['message']}. Please try again or contact support.")
            USER_STATES.pop(chat_id, None)
            return {"ok": True}
        if updated:
            await send_message(chat_id, "Location updated successfully!")
            await send_admin_message(f"Business updated:\nID: {state['entry_id']}\nNew Location: {text}")
            USER_STATES.pop(chat_id, None)
        else:
            await send_message(chat_id, "Failed to update location due to a database error. Please try again.")
        return {"ok": True}

    if state.get("stage") == "edit_website":
        if text.lower() == "none":
            website = None
        else:
            if not re.match(r"^(https?://)?[\w\-]+(\.[\w\-]+)+[/#?]?.*$", text):
                await send_message(chat_id, "Please enter a valid URL (e.g., https://example.com) or 'none':")
                return {"ok": True}
            website = text
        updated = await supabase_update_by_id_return("businesses", state["entry_id"], {"website": website})
        if isinstance(updated, dict) and updated.get("error") == "foreign_key_violation":
            await send_message(chat_id, f"Database error: {updated['message']}. Please try again or contact support.")
            USER_STATES.pop(chat_id, None)
            return {"ok": True}
        if updated:
            await send_message(chat_id, "Website updated successfully!")
            await send_admin_message(f"Business updated:\nID: {state['entry_id']}\nNew Website: {website or 'None'}")
            USER_STATES.pop(chat_id, None)
        else:
            await send_message(chat_id, "Failed to update website due to a database error. Please try again.")
        return {"ok": True}

    if state.get("stage") == "edit_description":
        if len(text) > MAX_DESCRIPTION_LENGTH:
            await send_message(chat_id, f"Description too long. Please use {MAX_DESCRIPTION_LENGTH} characters or fewer:")
            return {"ok": True}
        description = text if text.lower() != "none" else None
        updated = await supabase_update_by_id_return("businesses", state["entry_id"], {"description": description})
        if isinstance(updated, dict) and updated.get("error") == "foreign_key_violation":
            await send_message(chat_id, f"Database error: {updated['message']}. Please try again or contact support.")
            USER_STATES.pop(chat_id, None)
            return {"ok": True}
        if updated:
            await send_message(chat_id, "Description updated successfully!")
            await send_admin_message(f"Business updated:\nID: {state['entry_id']}\nNew Description: {description or 'None'}")
            USER_STATES.pop(chat_id, None)
        else:
            await send_message(chat_id, "Failed to update description due to a database error. Please try again.")
        return {"ok": True}

    if state.get("stage") == "edit_service_category":
        if text.lower() == "/skip":
            await send_message(chat_id, "Service addition skipped. Returning to edit menu.")
            state["stage"] = "edit_choose_field"
            resp = await send_message(
                chat_id,
                "Choose a field to edit:",
                reply_markup={
                    "inline_keyboard": [
                        [{"text": "Name", "callback_data": "edit_field:name"}],
                        [{"text": "Categories", "callback_data": "edit_field:categories"}],
                        [{"text": "Phone Number", "callback_data": "edit_field:phone_number"}],
                        [{"text": "Location", "callback_data": "edit_field:location"}],
                        [{"text": "Work Days", "callback_data": "edit_field:work_days"}],
                        [{"text": "Services", "callback_data": "edit_field:services"}],
                        [{"text": "Delete Services", "callback_data": "edit_field:delete_services"}],
                        [{"text": "Website", "callback_data": "edit_field:website"}],
                        [{"text": "Description", "callback_data": "edit_field:description"}]
                    ]
                }
            )
            if resp.get("ok"):
                state["temp_message_id"] = resp["result"]["message_id"]
            set_state(chat_id, state)
            return {"ok": True}
        await send_message(chat_id, "Please select a category from the keyboard or use /skip to cancel.")
        return {"ok": True}

    if state.get("stage") == "edit_service_name":
        if len(text) > MAX_NAME_LENGTH:
            await send_message(chat_id, f"Service name too long. Please use {MAX_NAME_LENGTH} characters or fewer:")
            return {"ok": True}
        state["temp_service_name"] = text
        await send_message(chat_id, f"Enter price for {text} (number in USD, e.g., 50.00):")
        state["stage"] = "edit_service_price"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "edit_service_price":
        try:
            price = float(text)
            if price <= 0:
                raise ValueError("Price must be positive")
            service_name = state.get("temp_service_name")
            service_category = state.get("temp_service_category")
            business_id = state["entry_id"]
            if not (service_name and service_category and business_id):
                await send_message(chat_id, "Error: Missing service details. Please start over with /edit_business.")
                USER_STATES.pop(chat_id, None)
                return {"ok": True}
            service = await supabase_insert_return("services", {
                "business_id": business_id,
                "name": service_name,
                "price": price,
                "category": service_category
            })
            if isinstance(service, dict) and service.get("error") == "invalid_category":
                await send_message(chat_id, f"Error: {service['message']}. Please choose a valid category.")
                resp = await send_message(
                    chat_id,
                    "Choose the category for the service:",
                    reply_markup=await create_service_category_keyboard(business_id)
                )
                if resp.get("ok"):
                    state["temp_message_id"] = resp["result"]["message_id"]
                state["stage"] = "edit_service_category"
                set_state(chat_id, state)
                return {"ok": True}
            if isinstance(service, dict) and service.get("error") == "foreign_key_violation":
                await send_message(chat_id, f"Database error: {service['message']}. Please try again or contact support.")
                USER_STATES.pop(chat_id, None)
                return {"ok": True}
            if service:
                await send_message(chat_id, f"Service {service_name} ({service_category}) added with price ${price}!")
                await send_admin_message(f"Business updated:\nID: {state['entry_id']}\nService {service_name} ({service_category}): ${price}")
                resp = await send_message(
                    chat_id,
                    "Add another service?",
                    reply_markup=await create_yes_no_keyboard("add_service")
                )
                if resp.get("ok"):
                    state["temp_message_id"] = resp["result"]["message_id"]
                state["stage"] = "awaiting_add_another_service"
                del state["temp_service_name"]
                del state["temp_service_category"]
                set_state(chat_id, state)
            else:
                await send_message(chat_id, "Failed to add service due to a database error. Please try again.")
        except ValueError:
            await send_message(chat_id, "Please enter a valid number for price (e.g., 50.00).")
        return {"ok": True}

    await send_message(chat_id, "Unknown command or state. Use /start, /register, /add_discount, /delete_discount, /edit_business, /list_services, or /list_discounts.")
    return {"ok": True}

async def submit_business_registration(chat_id: int, state: Dict[str, Any]):
    """Submit business registration to Supabase and notify admin."""
    try:
        state["data"]["status"] = "pending"
        business = await supabase_insert_return("businesses", {
            "name": state["data"]["name"],
            "phone_number": state["data"]["phone_number"],
            "location": state["data"]["location"],
            "work_days": state["data"]["work_days"],
            "website": state["data"]["website"],
            "description": state["data"]["description"],
            "telegram_id": state["data"]["telegram_id"],
            "status": state["data"]["status"]
        })
        if isinstance(business, dict) and business.get("error") == "foreign_key_violation":
            await send_message(chat_id, f"Database error: {business['message']}. Please try again or contact support.")
            USER_STATES.pop(chat_id, None)
            return
        if not business:
            await send_message(chat_id, "Failed to register business due to a database error. Please try again.")
            return

        # Insert categories
        for category in state["data"]["categories"]:
            result = await supabase_insert_return("business_categories", {
                "business_id": business["id"],
                "category": category
            })
            if isinstance(result, dict) and result.get("error") == "foreign_key_violation":
                await send_message(chat_id, f"Database error adding category '{category}': {result['message']}. Please try again.")
                continue
            if not result:
                await send_message(chat_id, f"Failed to add category '{category}' due to a database error. Please try again.")
                continue

        # Insert services
        for service in state["data"]["services"]:
            result = await supabase_insert_return("services", {
                "business_id": business["id"],
                "name": service["name"],
                "price": service["price"],
                "category": service["category"]
            })
            if isinstance(result, dict) and result.get("error") == "invalid_category":
                await send_message(chat_id, f"Error adding service '{service['name']}': {result['message']}. Please add the category using /edit_business.")
                continue
            if isinstance(result, dict) and result.get("error") == "foreign_key_violation":
                await send_message(chat_id, f"Database error adding service '{service['name']}': {result['message']}. Please try again.")
                continue
            if not result:
                await send_message(chat_id, f"Failed to add service '{service['name']}' due to a database error. Please try again.")
                continue

        await send_message(chat_id, "Business registered successfully! Awaiting admin approval.")
        categories = state["data"]["categories"] or ["None"]
        services_text = "\n".join([f"- {s['name']} ({s['category']}): ${s['price']}" for s in state["data"]["services"]]) or "None"
        await send_admin_message(
            f"New business registration:\n"
            f"Name: {business['name']}\n"
            f"Categories: {', '.join(categories)}\n"
            f"Phone: {business['phone_number']}\n"
            f"Location: {business['location']}\n"
            f"Work Days: {', '.join(business['work_days'])}\n"
            f"Website: {business['website'] or 'None'}\n"
            f"Description: {business['description'] or 'None'}\n"
            f"Services:\n{services_text}",
            reply_markup={
                "inline_keyboard": [
                    [
                        {"text": "Approve", "callback_data": f"approve:{business['id']}"},
                        {"text": "Reject", "callback_data": f"reject:{business['id']}"}
                    ]
                ]
            }
        )
        USER_STATES.pop(chat_id, None)
    except Exception as e:
        logger.error(f"Failed to register business for chat_id {chat_id}: {str(e)}", exc_info=True)
        await log_error_to_supabase(f"Failed to register business for chat_id {chat_id}: {str(e)}")
        await send_message(chat_id, "Failed to register business due to an unexpected error. Please try again.")

async def submit_discount(chat_id: int, state: Dict[str, Any]):
    """Submit discount to Supabase and notify admin."""
    try:
        state["data"]["active"] = False
        discount = await supabase_insert_return("discounts", state["data"])
        if isinstance(discount, dict) and discount.get("error") == "invalid_category":
            await send_message(chat_id, f"Error: {discount['message']}. Please choose a valid category.")
            resp = await send_message(
                chat_id,
                "Choose the category for this discount:",
                reply_markup=await create_service_category_keyboard(state["data"]["business_id"])
            )
            if resp.get("ok"):
                state["temp_message_id"] = resp["result"]["message_id"]
            state["stage"] = "awaiting_discount_category"
            set_state(chat_id, state)
            return
        if isinstance(discount, dict) and discount.get("error") == "foreign_key_violation":
            await send_message(chat_id, f"Database error: {discount['message']}. Please try again or contact support.")
            USER_STATES.pop(chat_id, None)
            return
        if not discount:
            await send_message(chat_id, "Failed to submit discount due to a database error. Please try again.")
            return
        await send_message(chat_id, "Discount submitted! Awaiting admin approval.")
        await send_admin_message(
            f"New discount submission:\n"
            f"Name: {discount['name']}\n"
            f"Category: {discount['category']}\n"
            f"Business ID: {discount['business_id']}\n"
            f"Percentage: {discount['discount_percentage']}%",
            reply_markup={
                "inline_keyboard": [
                    [
                        {"text": "Approve", "callback_data": f"discount_approve:{discount['id']}"},
                        {"text": "Reject", "callback_data": f"discount_reject:{discount['id']}"}
                    ]
                ]
            }
        )
        USER_STATES.pop(chat_id, None)
    except Exception as e:
        logger.error(f"Failed to submit discount for chat_id {chat_id}: {str(e)}", exc_info=True)
        await log_error_to_supabase(f"Failed to submit discount for chat_id {chat_id}: {str(e)}")
        await send_message(chat_id, "Failed to submit discount due to an unexpected error. Please try again.")

async def handle_callback_query(callback_query: Dict[str, Any]):
    """Handle callback queries from inline keyboards."""
    chat_id = callback_query.get("from", {}).get("id")
    callback_data = callback_query.get("data")
    message_id = callback_query.get("message", {}).get("message_id")
    if not chat_id or not callback_data or not message_id:
        logger.error(f"Invalid callback query: chat_id={chat_id}, callback_data={callback_data}, message_id={message_id}")
        await log_error_to_supabase(f"Invalid callback query: chat_id={chat_id}, callback_data={callback_data}, message_id={message_id}")
        return {"ok": True}

    state = get_state(chat_id) or {}
    logger.info(f"Processing callback query from chat_id {chat_id}: {callback_data}")

    # Admin Approval/Rejection for Business
    if callback_data.startswith("approve:") or callback_data.startswith("reject:"):
        action, business_id = callback_data.split(":", 1)
        status = "approved" if action == "approve" else "rejected"
        
        if str(chat_id) != str(ADMIN_CHAT_ID):
            logger.warning(f"Unauthorized approval attempt by chat_id {chat_id}")
            await send_message(chat_id, "You are not authorized to approve or reject businesses.")
            return {"ok": True}

        try:
            def _q():
                return supabase.table("businesses").select("telegram_id, name").eq("id", business_id).limit(1).execute()
            resp = await asyncio.to_thread(_q)
            data = resp.data if hasattr(resp, "data") else resp.get("data")
            if not data:
                logger.error(f"Business not found for id {business_id}")
                await log_error_to_supabase(f"Business not found for id {business_id}")
                await edit_message(chat_id, message_id, "Error: Business not found.")
                return {"ok": True}
            business = data[0]
            user_chat_id = business["telegram_id"]
            business_name = business["name"]
        except Exception as e:
            logger.error(f"Failed to fetch business {business_id}: {str(e)}")
            await log_error_to_supabase(f"Failed to fetch business {business_id}: {str(e)}")
            await edit_message(chat_id, message_id, "Error: Failed to fetch business details.")
            return {"ok": True}

        updated = await supabase_update_by_id_return("businesses", business_id, {"status": status})
        if isinstance(updated, dict) and updated.get("error") == "foreign_key_violation":
            await edit_message(chat_id, message_id, f"Database error: {updated['message']}. Please try again or contact support.")
            return {"ok": True}
        if updated:
            await edit_message(
                chat_id,
                message_id,
                f"Business '{business_name}' {status} successfully!",
                reply_markup=None
            )
            await send_message(
                user_chat_id,
                f"Your business '{business_name}' has been {status}!"
            )
            logger.info(f"Business {business_id} set to {status}, user {user_chat_id} notified")
        else:
            await edit_message(
                chat_id,
                message_id,
                f"Failed to {action} business '{business_name}' due to a database error. Please try again.",
                reply_markup=None
            )
            logger.error(f"Failed to update business {business_id} to {status}")
            await log_error_to_supabase(f"Failed to update business {business_id} to {status}")
        return {"ok": True}

    # Admin Approval/Rejection for Discount
    if callback_data.startswith("discount_approve:") or callback_data.startswith("discount_reject:"):
        action, discount_id = callback_data.split(":", 1)
        active = action == "discount_approve"
        
        if str(chat_id) != str(ADMIN_CHAT_ID):
            logger.warning(f"Unauthorized discount approval attempt by chat_id {chat_id}")
            await send_message(chat_id, "You are not authorized to approve or reject discounts.")
            return {"ok": True}

        try:
            def _q():
                return supabase.table("discounts").select("name, business_id").eq("id", discount_id).limit(1).execute()
            resp = await asyncio.to_thread(_q)
            data = resp.data if hasattr(resp, "data") else resp.get("data")
            if not data:
                logger.error(f"Discount not found for id {discount_id}")
                await log_error_to_supabase(f"Discount not found for id {discount_id}")
                await edit_message(chat_id, message_id, "Error: Discount not found.")
                return {"ok": True}
            discount = data[0]
            discount_name = discount["name"]
            business_id = discount["business_id"]
        except Exception as e:
            logger.error(f"Failed to fetch discount {discount_id}: {str(e)}")
            await log_error_to_supabase(f"Failed to fetch discount {discount_id}: {str(e)}")
            await edit_message(chat_id, message_id, "Error: Failed to fetch discount details.")
            return {"ok": True}

        try:
            def _q():
                return supabase.table("businesses").select("telegram_id").eq("id", business_id).limit(1).execute()
            resp = await asyncio.to_thread(_q)
            data = resp.data if hasattr(resp, "data") else resp.get("data")
            if not data:
                logger.error(f"Business not found for id {business_id}")
                await log_error_to_supabase(f"Business not found for id {business_id}")
                await edit_message(chat_id, message_id, "Error: Business not found.")
                return {"ok": True}
            user_chat_id = data[0]["telegram_id"]
        except Exception as e:
            logger.error(f"Failed to fetch business {business_id}: {str(e)}")
            await log_error_to_supabase(f"Failed to fetch business {business_id}: {str(e)}")
            await edit_message(chat_id, message_id, "Error: Failed to fetch business details.")
            return {"ok": True}

        updated = await supabase_update_by_id_return("discounts", discount_id, {"active": active})
        if isinstance(updated, dict) and updated.get("error") == "foreign_key_violation":
            await edit_message(chat_id, message_id, f"Database error: {updated['message']}. Please try again or contact support.")
            return {"ok": True}
        if updated:
            await edit_message(
                chat_id,
                message_id,
                f"Discount '{discount_name}' {'approved' if active else 'rejected'} successfully!",
                reply_markup=None
            )
            await send_message(
                user_chat_id,
                f"Your discount '{discount_name}' has been {'approved' if active else 'rejected'}!"
            )
            logger.info(f"Discount {discount_id} set to active={active}, user {user_chat_id} notified")
        else:
            await edit_message(
                chat_id,
                message_id,
                f"Failed to {action} discount '{discount_name}' due to a database error. Please try again.",
                reply_markup=None
            )
            logger.error(f"Failed to update discount {discount_id} to active={active}")
            await log_error_to_supabase(f"Failed to update discount {discount_id} to active={active}")
        return {"ok": True}

    # Registration: Category selection
    if state.get("stage") in ["awaiting_categories", "edit_categories"] and callback_data.startswith("category:"):
        category = callback_data[len("category:"):]
        if category == "confirm":
            if not state["data"].get("categories"):
                await edit_message(
                    chat_id,
                    message_id,
                    "Please select at least one category.",
                    reply_markup=await create_category_keyboard(state["data"].get("categories", []))
                )
                return {"ok": True}
            if state.get("stage") == "awaiting_categories":
                await send_message(chat_id, "Enter your business phone number (e.g., +1234567890):")
                state["stage"] = "awaiting_phone"
                set_state(chat_id, state)
            else:  # edit_categories
                try:
                    def _delete():
                        return supabase.table("business_categories").delete().eq("business_id", state["entry_id"]).execute()
                    await asyncio.to_thread(_delete)
                except Exception as e:
                    logger.error(f"Failed to delete old categories for business {state['entry_id']}: {str(e)}")
                    await log_error_to_supabase(f"Failed to delete old categories for business {state['entry_id']}: {str(e)}")
                    await send_message(chat_id, "Failed to update categories due to a database error. Please try again.")
                    return {"ok": True}

                for category in state["data"]["categories"]:
                    result = await supabase_insert_return("business_categories", {
                        "business_id": state["entry_id"],
                        "category": category
                    })
                    if isinstance(result, dict) and result.get("error") == "foreign_key_violation":
                        await send_message(chat_id, f"Database error adding category '{category}': {result['message']}. Please try again.")
                        continue
                    if not result:
                        await send_message(chat_id, f"Failed to add category '{category}' due to a database error. Please try again.")
                        continue
                await send_message(chat_id, "Categories updated successfully!")
                await send_admin_message(f"Business updated:\nID: {state['entry_id']}\nNew Categories: {', '.join(state['data']['categories'])}")
                USER_STATES.pop(chat_id, None)
            return {"ok": True}
        if category in CATEGORIES:
            selected = state["data"].get("categories", [])
            if category in selected:
                selected.remove(category)
            else:
                selected.append(category)
            state["data"]["categories"] = selected
            set_state(chat_id, state)
            await edit_message(
                chat_id,
                message_id,
                f"Selected categories: {', '.join(selected) or 'None'}\nSelect categories (select all that apply, then Confirm):",
                reply_markup=await create_category_keyboard(selected)
            )
            return {"ok": True}

    # Registration: Work days selection
    if state.get("stage") in ["awaiting_work_days", "edit_work_days"] and callback_data.startswith("workday:"):
        day = callback_data[len("workday:"):]
        if day == "confirm":
            if not state["data"].get("work_days"):
                await edit_message(
                    chat_id,
                    message_id,
                    "Please select at least one work day.",
                    reply_markup=await create_workdays_keyboard(state["data"].get("work_days", []))
                )
                return {"ok": True}
            if state.get("stage") == "awaiting_work_days":
                await send_message(chat_id, "Enter your business website (e.g., https://example.com, or 'none'):")
                state["stage"] = "awaiting_website"
                set_state(chat_id, state)
            else:  # edit_work_days
                updated = await supabase_update_by_id_return("businesses", state["entry_id"], {"work_days": state["data"]["work_days"]})
                if isinstance(updated, dict) and updated.get("error") == "foreign_key_violation":
                    await send_message(chat_id, f"Database error: {updated['message']}. Please try again or contact support.")
                    USER_STATES.pop(chat_id, None)
                    return {"ok": True}
                if updated:
                    await send_message(chat_id, "Work days updated successfully!")
                    await send_admin_message(f"Business updated:\nID: {state['entry_id']}\nNew Work Days: {', '.join(state['data']['work_days'])}")
                    USER_STATES.pop(chat_id, None)
                else:
                    await send_message(chat_id, "Failed to update work days due to a database error. Please try again.")
            return {"ok": True}
        if day in WEEK_DAYS:
            selected = state["data"].get("work_days", [])
            if day in selected:
                selected.remove(day)
            else:
                selected.append(day)
            state["data"]["work_days"] = selected
            set_state(chat_id, state)
            await edit_message(
                chat_id,
                message_id,
                f"Selected work days: {', '.join(selected) or 'None'}\nSelect work days:",
                reply_markup=await create_workdays_keyboard(selected)
            )
            return {"ok": True}

    # Registration/Edit: Add another service
    if state.get("stage") == "awaiting_add_another_service" and callback_data.startswith("add_service:"):
        choice = callback_data[len("add_service:"):]
        business_id = state["data"].get("business_id", state.get("entry_id"))
        if choice == "yes":
            categories = await supabase_get_business_categories(business_id)
            if not categories:
                await send_message(chat_id, "No categories found. Please add categories using /edit_business.")
                USER_STATES.pop(chat_id, None)
                return {"ok": True}
            resp = await send_message(
                chat_id,
                "Choose the category for the next service (or /skip if none):",
                reply_markup=await create_service_category_keyboard(business_id)
            )
            if resp.get("ok"):
                state["temp_message_id"] = resp["result"]["message_id"]
            state["stage"] = "awaiting_service_category" if state.get("stage") == "awaiting_add_another_service" else "edit_service_category"
            set_state(chat_id, state)
        elif choice == "no":
            if state.get("stage") == "awaiting_add_another_service" and "business_id" not in state["data"]:
                await submit_business_registration(chat_id, state)
            else:
                await send_message(chat_id, "Service addition completed. Returning to edit menu.")
                state["stage"] = "edit_choose_field"
                resp = await send_message(
                    chat_id,
                    "Choose a field to edit:",
                    reply_markup={
                        "inline_keyboard": [
                            [{"text": "Name", "callback_data": "edit_field:name"}],
                            [{"text": "Categories", "callback_data": "edit_field:categories"}],
                            [{"text": "Phone Number", "callback_data": "edit_field:phone_number"}],
                            [{"text": "Location", "callback_data": "edit_field:location"}],
                            [{"text": "Work Days", "callback_data": "edit_field:work_days"}],
                            [{"text": "Services", "callback_data": "edit_field:services"}],
                            [{"text": "Delete Services", "callback_data": "edit_field:delete_services"}],
                            [{"text": "Website", "callback_data": "edit_field:website"}],
                            [{"text": "Description", "callback_data": "edit_field:description"}]
                        ]
                    }
                )
                if resp.get("ok"):
                    state["temp_message_id"] = resp["result"]["message_id"]
                set_state(chat_id, state)
        return {"ok": True}

    # Registration/Edit: Service category selection
    if state.get("stage") in ["awaiting_service_category", "edit_service_category"] and callback_data.startswith("service_category:"):
        category = callback_data[len("service_category:"):]
        business_id = state["data"].get("business_id", state.get("entry_id"))
        if category == "skip":
            if state.get("stage") == "awaiting_service_category":
                await submit_business_registration(chat_id, state)
            else:
                await send_message(chat_id, "Service addition skipped. Returning to edit menu.")
                state["stage"] = "edit_choose_field"
                resp = await send_message(
                    chat_id,
                    "Choose a field to edit:",
                    reply_markup={
                        "inline_keyboard": [
                            [{"text": "Name", "callback_data": "edit_field:name"}],
                            [{"text": "Categories", "callback_data": "edit_field:categories"}],
                            [{"text": "Phone Number", "callback_data": "edit_field:phone_number"}],
                            [{"text": "Location", "callback_data": "edit_field:location"}],
                            [{"text": "Work Days", "callback_data": "edit_field:work_days"}],
                            [{"text": "Services", "callback_data": "edit_field:services"}],
                            [{"text": "Website", "callback_data": "edit_field:website"}],
                            [{"text": "Description", "callback_data": "edit_field:description"}]
                        ]
                    }
                )
                if resp.get("ok"):
                    state["temp_message_id"] = resp["result"]["message_id"]
                set_state(chat_id, state)
            return {"ok": True}
        categories = await supabase_get_business_categories(business_id)
        if category not in categories:
            await edit_message(
                chat_id,
                message_id,
                "Invalid category. Please choose again:",
                reply_markup=await create_service_category_keyboard(business_id)
            )
            return {"ok": True}
        state["temp_service_category"] = category
        await send_message(chat_id, f"Enter the service name (max {MAX_NAME_LENGTH} characters):")
        state["stage"] = "awaiting_service_name" if state.get("stage") == "awaiting_service_category" else "edit_service_name"
        set_state(chat_id, state)
        return {"ok": True}

    # Discount: Category selection
    if state.get("stage") == "awaiting_discount_category" and callback_data.startswith("service_category:"):
        category = callback_data[len("service_category:"):]
        business_id = state["data"]["business_id"]
        categories = await supabase_get_business_categories(business_id)
        if category not in categories:
            await edit_message(
                chat_id,
                message_id,
                "Invalid category. Please choose again:",
                reply_markup=await create_service_category_keyboard(business_id)
            )
            return {"ok": True}
        state["data"]["category"] = category
        await send_message(chat_id, "Enter the discount percentage (e.g., 20 for 20%):")
        state["stage"] = "awaiting_discount_percentage"
        set_state(chat_id, state)
        return {"ok": True}

    # Edit business: Field selection
    if state.get("stage") == "edit_choose_field" and callback_data.startswith("edit_field:"):
        field = callback_data[len("edit_field:"):]
        state["stage"] = f"edit_{field}"
        if field == "categories":
            business = await supabase_find_business(chat_id)
            state["data"]["categories"] = [c["category"] for c in business.get("business_categories", [])]
            resp = await send_message(
                chat_id,
                f"Current categories: {', '.join(state['data']['categories']) or 'None'}\nSelect new categories (select all that apply, then Confirm):",
                reply_markup=await create_category_keyboard(state["data"]["categories"])
            )
            if resp.get("ok"):
                state["temp_message_id"] = resp["result"]["message_id"]
        elif field == "work_days":
            business = await supabase_find_business(chat_id)
            state["data"]["work_days"] = business.get("work_days", [])
            resp = await send_message(
                chat_id,
                f"Current work days: {', '.join(state['data']['work_days']) or 'None'}\nSelect new work days:",
                reply_markup=await create_workdays_keyboard(state["data"]["work_days"])
            )
            if resp.get("ok"):
                state["temp_message_id"] = resp["result"]["message_id"]
        elif field == "services":
            categories = await supabase_get_business_categories(state["entry_id"])
            if not categories:
                await send_message(chat_id, "No categories found. Please add categories using /edit_business.")
                USER_STATES.pop(chat_id, None)
                return {"ok": True}
            resp = await send_message(
                chat_id,
                "Choose the category for the new service (or /skip if none):",
                reply_markup=await create_service_category_keyboard(state["entry_id"])
            )
            if resp.get("ok"):
                state["temp_message_id"] = resp["result"]["message_id"]
            state["stage"] = "edit_service_category"
        elif field == "name":
            await send_message(chat_id, f"Enter the new business name (max {MAX_NAME_LENGTH} characters):")
        elif field == "phone_number":
            await send_message(chat_id, "Enter the new phone number (e.g., +1234567890):")
        elif field == "location":
            await send_message(chat_id, "Enter the new location (e.g., 123 Main St, City):")
        elif field == "website":
            await send_message(chat_id, "Enter the new website (e.g., https://example.com, or 'none'):")
        elif field == "description":
            await send_message(chat_id, f"Enter the new description (max {MAX_DESCRIPTION_LENGTH} characters, or 'none'):")
        set_state(chat_id, state)
        return {"ok": True}

    logger.warning(f"Unhandled callback query: {callback_data} from chat_id {chat_id}")
    await log_error_to_supabase(f"Unhandled callback query: {callback_data} from chat_id {chat_id}")
    return {"ok": True}

@app.get("/health")
async def health() -> PlainTextResponse:
    """Health check endpoint."""
    return PlainTextResponse("OK", status_code=200)

@app.post("/hook/business_bot")
async def webhook(request: Request):
    """Webhook endpoint for Business Bot."""
    return await webhook_handler(request)

if __name__ == "__main__":
    import uvicorn
    asyncio.run(initialize_bot())
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))







'''
import os
import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
import logging
import httpx
from dotenv import load_dotenv
from supabase import create_client, Client
from fastapi import FastAPI, Request
from starlette.responses import PlainTextResponse, Response

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logging.getLogger("httpx").setLevel(logging.DEBUG)
logging.getLogger("httpcore").setLevel(logging.DEBUG)
logger = logging.getLogger("business_bot")

# Initialize FastAPI app
app = FastAPI()

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("BUSINESS_BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")

if not all([BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY, ADMIN_CHAT_ID]):
    raise RuntimeError("Missing required environment variables: BUSINESS_BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY, ADMIN_CHAT_ID")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Constants
USER_STATES: Dict[int, Dict[str, Any]] = {}
STATE_TTL_SECONDS = 30 * 60  # 30 minutes
CATEGORIES = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining"]
WEEK_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
MAX_DISCOUNT_PERCENTAGE = 100
MIN_DISCOUNT_PERCENTAGE = 1
MAX_NAME_LENGTH = 100
MAX_DESCRIPTION_LENGTH = 500

def now_iso():
    """Return current UTC time in ISO format."""
    return datetime.now(timezone.utc).isoformat()

def set_state(chat_id: int, state: Dict[str, Any]):
    """Set user state with timestamp."""
    state["updated_at"] = now_iso()
    USER_STATES[chat_id] = state
    logger.debug(f"Set state for chat_id {chat_id}: {state}")

def get_state(chat_id: int) -> Optional[Dict[str, Any]]:
    """Get user state, expire if too old."""
    state = USER_STATES.get(chat_id)
    if not state:
        logger.debug(f"No state found for chat_id {chat_id}")
        return None
    try:
        updated = datetime.fromisoformat(state.get("updated_at"))
        if (datetime.now(timezone.utc) - updated).total_seconds() > STATE_TTL_SECONDS:
            USER_STATES.pop(chat_id, None)
            logger.info(f"Expired state for chat_id {chat_id}")
            return None
    except Exception as e:
        logger.error(f"Invalid state timestamp for chat_id {chat_id}: {str(e)}")
        USER_STATES.pop(chat_id, None)
        return None
    return state

async def log_error_to_supabase(error_message: str):
    """Log errors to Supabase bot_errors table."""
    payload = {
        "error": error_message[:1000],  # Truncate to avoid Supabase limits
        "created_at": now_iso(),
        "bot": "business_bot"
    }
    try:
        def _ins():
            return supabase.table("bot_errors").insert(payload).execute()
        resp = await asyncio.to_thread(_ins)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if data:
            logger.info(f"Logged error to Supabase: {error_message}")
        else:
            logger.error(f"Failed to log error to Supabase: {error_message}")
    except Exception as e:
        logger.error(f"Failed to log error to Supabase: {str(e)}", exc_info=True)

async def send_message(chat_id: int, text: str, reply_markup: Optional[dict] = None, retries: int = 3):
    """Send a message to a Telegram chat with retry logic."""
    if not isinstance(chat_id, int) or chat_id == 0:
        logger.error(f"Invalid chat_id: {chat_id}")
        await log_error_to_supabase(f"Invalid chat_id: {chat_id}")
        return {"ok": False, "error": "Invalid chat_id"}
    
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        payload = {"chat_id": chat_id, "text": text[:4096], "parse_mode": "Markdown"}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        for attempt in range(retries):
            try:
                logger.debug(f"Sending message to chat_id {chat_id} (attempt {attempt + 1}): {text[:100]}...")
                response = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Sent message to chat_id {chat_id}: {text[:100]}...")
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to send message: HTTP {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 400 and "chat not found" in e.response.text.lower():
                    await log_error_to_supabase(f"Chat not found for chat_id {chat_id}")
                    return {"ok": False, "error": "Chat not found"}
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
        await log_error_to_supabase(f"Failed to send message to chat_id {chat_id} after {retries} attempts")
        return {"ok": False, "error": "Max retries reached"}

async def edit_message(chat_id: int, message_id: int, text: str, reply_markup: Optional[dict] = None, retries: int = 3):
    """Edit an existing message in a Telegram chat."""
    if not isinstance(chat_id, int) or chat_id == 0 or not isinstance(message_id, int):
        logger.error(f"Invalid parameters: chat_id={chat_id}, message_id={message_id}")
        await log_error_to_supabase(f"Invalid parameters: chat_id={chat_id}, message_id={message_id}")
        return {"ok": False, "error": "Invalid parameters"}
    
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        payload = {"chat_id": chat_id, "message_id": message_id, "text": text[:4096], "parse_mode": "Markdown"}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        for attempt in range(retries):
            try:
                logger.debug(f"Editing message {message_id} in chat_id {chat_id} (attempt {attempt + 1}): {text[:100]}...")
                response = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Edited message {message_id} in chat_id {chat_id}: {text[:100]}...")
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to edit message: HTTP {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 400 and "chat not found" in e.response.text.lower():
                    await log_error_to_supabase(f"Chat not found for chat_id {chat_id}")
                    return {"ok": False, "error": "Chat not found"}
                if e.response.status_code == 429:
                    retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    await asyncio.sleep(retry_after)
                    continue
                return {"ok": False, "error": f"HTTP {e.response.status_code}"}
            except Exception as e:
                logger.error(f"Failed to edit message: {str(e)}", exc_info=True)
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        logger.error(f"Failed to edit message {message_id} in chat_id {chat_id} after {retries} attempts")
        await log_error_to_supabase(f"Failed to edit message {message_id} in chat_id {chat_id} after {retries} attempts")
        return {"ok": False, "error": "Max retries reached"}

async def send_admin_message(text: str, reply_markup: Optional[dict] = None, retries: int = 3):
    """Send a message to the admin chat."""
    try:
        admin_chat_id = int(ADMIN_CHAT_ID)
    except ValueError:
        logger.error(f"Invalid ADMIN_CHAT_ID: {ADMIN_CHAT_ID}")
        await log_error_to_supabase(f"Invalid ADMIN_CHAT_ID: {ADMIN_CHAT_ID}")
        return {"ok": False, "error": "Invalid ADMIN_CHAT_ID"}
    
    return await send_message(admin_chat_id, text, reply_markup, retries)

async def supabase_insert_return(table: str, payload: dict) -> Optional[Dict[str, Any]]:
    """Insert data into Supabase and return the inserted record."""
    payload['created_at'] = now_iso()
    payload['updated_at'] = now_iso()
    try:
        def _ins():
            return supabase.table(table).insert(payload).execute()
        resp = await asyncio.to_thread(_ins)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.error(f"Failed to insert into {table}: no data returned")
            await log_error_to_supabase(f"Failed to insert into {table}: no data returned")
            return None
        logger.info(f"Inserted into {table}: {data[0]}")
        return data[0]
    except Exception as e:
        logger.error(f"supabase_insert_return failed for table {table}: {str(e)}", exc_info=True)
        await log_error_to_supabase(f"supabase_insert_return failed for table {table}: {str(e)}")
        return None

async def supabase_update_by_id_return(table: str, entry_id: str, payload: dict) -> Optional[Dict[str, Any]]:
    """Update a Supabase record by ID and return the updated record."""
    payload['updated_at'] = now_iso()
    try:
        def _upd():
            return supabase.table(table).update(payload).eq("id", entry_id).execute()
        resp = await asyncio.to_thread(_upd)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.error(f"Failed to update {table} with id {entry_id}: no data returned")
            await log_error_to_supabase(f"Failed to update {table} with id {entry_id}: no data returned")
            return None
        logger.info(f"Updated {table} with id {entry_id}: {data[0]}")
        return data[0]
    except Exception as e:
        logger.error(f"supabase_update_by_id_return failed for table {table}, id {entry_id}: {str(e)}", exc_info=True)
        await log_error_to_supabase(f"supabase_update_by_id_return failed for table {table}, id {entry_id}: {str(e)}")
        return None

async def supabase_find_business(chat_id: int) -> Optional[Dict[str, Any]]:
    """Find a business by chat_id in Supabase."""
    try:
        def _q():
            return supabase.table("businesses").select("*").eq("telegram_id", chat_id).limit(1).execute()
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.info(f"No business found for chat_id {chat_id}")
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_find_business failed for chat_id {chat_id}: {str(e)}", exc_info=True)
        await log_error_to_supabase(f"supabase_find_business failed for chat_id {chat_id}: {str(e)}")
        return None

async def create_category_keyboard() -> dict:
    """Create inline keyboard for category selection."""
    buttons = [
        [{"text": category, "callback_data": f"category:{category}"}]
        for category in CATEGORIES
    ]
    return {"inline_keyboard": buttons}

async def create_workdays_keyboard(selected: list) -> dict:
    """Create inline keyboard for work days selection."""
    buttons = []
    for day in WEEK_DAYS:
        prefix = "✅ " if day in selected else ""
        buttons.append([{"text": f"{prefix}{day}", "callback_data": f"workday:{day}"}])
    buttons.append([{"text": "Confirm", "callback_data": "workday:confirm"}])
    return {"inline_keyboard": buttons}

async def create_yes_no_keyboard(prefix: str) -> dict:
    """Create inline yes/no keyboard."""
    return {
        "inline_keyboard": [
            [
                {"text": "Yes", "callback_data": f"{prefix}:yes"},
                {"text": "No", "callback_data": f"{prefix}:no"}
            ]
        ]
    }

async def create_discount_type_keyboard() -> dict:
    """Create inline keyboard for discount/giveaway selection."""
    return {
        "inline_keyboard": [
            [
                {"text": "Discount", "callback_data": "discount_type:discount"},
                {"text": "Giveaway", "callback_data": "discount_type:giveaway"}
            ]
        ]
    }

async def initialize_bot():
    """Initialize bot by setting webhook and commands."""
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        try:
            # Test admin chat_id
            test_response = await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": int(ADMIN_CHAT_ID),
                    "text": "Business Bot initialized successfully",
                    "parse_mode": "Markdown"
                }
            )
            test_response.raise_for_status()
            logger.info(f"Admin chat_id {ADMIN_CHAT_ID} verified successfully")
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to verify ADMIN_CHAT_ID {ADMIN_CHAT_ID}: HTTP {e.response.status_code} - {e.response.text}")
            await log_error_to_supabase(f"Failed to verify ADMIN_CHAT_ID {ADMIN_CHAT_ID}: {e.response.text}")
            if "chat not found" in e.response.text.lower():
                raise RuntimeError(f"ADMIN_CHAT_ID {ADMIN_CHAT_ID} is invalid. Please verify using /getUpdates.")
        except Exception as e:
            logger.error(f"Failed to verify ADMIN_CHAT_ID {ADMIN_CHAT_ID}: {str(e)}")
            await log_error_to_supabase(f"Failed to verify ADMIN_CHAT_ID {ADMIN_CHAT_ID}: {str(e)}")
            raise

        try:
            # Set webhook
            webhook_url = "https://backend-python-6q8a.onrender.com/hook/business_bot"
            response = await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
                json={"url": webhook_url, "allowed_updates": ["message", "callback_query"]}
            )
            response.raise_for_status()
            logger.info(f"Webhook set to {webhook_url}")
            
            # Set menu button
            await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/setChatMenuButton",
                json={"menu_button": {"type": "commands"}}
            )
            logger.info("Set menu button")
            
            # Set bot commands
            commands = [
                {"command": "start", "description": "Start the bot"},
                {"command": "register", "description": "Register your business"},
                {"command": "add_discount", "description": "Add a discount or giveaway"},
                {"command": "edit_business", "description": "Edit your business details"},
                {"command": "list_services", "description": "List your services"},
                {"command": "list_discounts", "description": "List your discounts/giveaways"},
                {"command": "cancel", "description": "Cancel current operation"}
            ]
            await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands",
                json={"commands": commands}
            )
            logger.info("Set bot commands")
        except Exception as e:
            logger.error(f"Failed to initialize bot: {str(e)}", exc_info=True)
            await log_error_to_supabase(f"Failed to initialize bot: {str(e)}")
            raise

async def webhook_handler(request: Request):
    """Handle incoming Telegram updates."""
    try:
        update = await request.json()
        if not update:
            logger.error("Received empty update from Telegram")
            return Response(status_code=200)
        logger.info(f"Received update: {json.dumps(update, indent=2)}")
        message = update.get("message")
        if message:
            await handle_message_update(message)
        callback_query = update.get("callback_query")
        if callback_query:
            await handle_callback_query(callback_query)
        return Response(status_code=200)
    except json.JSONDecodeError:
        logger.error("Invalid JSON in webhook", exc_info=True)
        await log_error_to_supabase("Invalid JSON in webhook")
        return Response(status_code=200)
    except Exception as e:
        logger.error(f"Error processing webhook update: {str(e)}", exc_info=True)
        await log_error_to_supabase(f"Error processing webhook update: {str(e)}")
        return Response(status_code=200)

async def handle_message_update(message: Dict[str, Any]):
    """Handle incoming messages."""
    chat_id = message.get("chat", {}).get("id")
    if not chat_id:
        logger.error("No chat_id in message")
        await log_error_to_supabase("No chat_id in message")
        return {"ok": True}
    text = (message.get("text") or "").strip()
    logger.info(f"Handling message from chat_id {chat_id}: {text}")
    state = get_state(chat_id) or {}

    # /start
    if text.lower() == "/start":
        business = await supabase_find_business(chat_id)
        if business:
            if business["status"] == "approved":
                await send_message(chat_id, "Your business is approved! Use /add_discount to add offers, /edit_business to update details, or /list_services to view services.")
            else:
                await send_message(chat_id, "Your business is pending approval. We'll notify you soon!")
        else:
            await send_message(chat_id, "Welcome to the Business Bot! Register your business with /register.")
        return {"ok": True}

    # /cancel
    if text.lower() == "/cancel":
        if state:
            USER_STATES.pop(chat_id, None)
            await send_message(chat_id, "Current operation cancelled.")
        else:
            await send_message(chat_id, "No operation to cancel.")
        return {"ok": True}

    # /register
    if text.lower() == "/register":
        business = await supabase_find_business(chat_id)
        if business:
            await send_message(chat_id, "You’ve already registered! Use /edit_business to update details or /add_discount to add offers.")
            return {"ok": True}
        state = {
            "stage": "awaiting_name",
            "data": {"telegram_id": chat_id, "prices": {}, "work_days": [], "website": None, "description": None},
            "entry_id": None
        }
        await send_message(chat_id, f"Enter your business name (max {MAX_NAME_LENGTH} characters):")
        set_state(chat_id, state)
        return {"ok": True}

    # /add_discount
    if text.lower() == "/add_discount":
        business = await supabase_find_business(chat_id)
        if not business:
            await send_message(chat_id, "Please register your business first with /register.")
            return {"ok": True}
        if business["status"] != "approved":
            await send_message(chat_id, "Your business is not yet approved.")
            return {"ok": True}
        state = {
            "stage": "awaiting_discount_type",
            "data": {"business_id": business["id"], "salon_name": business["name"]},
            "entry_id": None
        }
        await send_message(chat_id, "Is this a discount or giveaway?", reply_markup=await create_discount_type_keyboard())
        set_state(chat_id, state)
        return {"ok": True}

    # /edit_business
    if text.lower() == "/edit_business":
        business = await supabase_find_business(chat_id)
        if not business:
            await send_message(chat_id, "Please register your business first with /register.")
            return {"ok": True}
        state = {
            "stage": "edit_choose_field",
            "data": {"business_id": business["id"], "telegram_id": chat_id},
            "entry_id": business["id"]
        }
        await send_message(
            chat_id,
            "Choose a field to edit:",
            reply_markup={
                "inline_keyboard": [
                    [{"text": "Name", "callback_data": "edit_field:name"}],
                    [{"text": "Category", "callback_data": "edit_field:category"}],
                    [{"text": "Phone Number", "callback_data": "edit_field:phone_number"}],
                    [{"text": "Location", "callback_data": "edit_field:location"}],
                    [{"text": "Work Days", "callback_data": "edit_field:work_days"}],
                    [{"text": "Services", "callback_data": "edit_field:prices"}],
                    [{"text": "Website", "callback_data": "edit_field:website"}],
                    [{"text": "Description", "callback_data": "edit_field:description"}]
                ]
            }
        )
        set_state(chat_id, state)
        return {"ok": True}

    # /list_services
    if text.lower() == "/list_services":
        business = await supabase_find_business(chat_id)
        if not business:
            await send_message(chat_id, "Please register your business first with /register.")
            return {"ok": True}
        prices = business.get("prices", {})
        if not prices:
            await send_message(chat_id, "No services registered.")
        else:
            services_text = "\n".join([f"- {service}: ${price}" for service, price in prices.items()])
            await send_message(chat_id, f"Your services:\n{services_text}")
        return {"ok": True}

    # /list_discounts
    if text.lower() == "/list_discounts":
        business = await supabase_find_business(chat_id)
        if not business:
            await send_message(chat_id, "Please register your business first with /register.")
            return {"ok": True}
        try:
            def _q_discounts():
                return supabase.table("discounts").select("*").eq("business_id", business["id"]).execute()
            def _q_giveaways():
                return supabase.table("giveaways").select("*").eq("business_id", business["id"]).execute()
            resp_discounts = await asyncio.to_thread(_q_discounts)
            resp_giveaways = await asyncio.to_thread(_q_giveaways)
            discounts = resp_discounts.data if hasattr(resp_discounts, "data") else resp_discounts.get("data", [])
            giveaways = resp_giveaways.data if hasattr(resp_giveaways, "data") else resp_giveaways.get("data", [])
            if not (discounts or giveaways):
                await send_message(chat_id, "No discounts or giveaways registered.")
                return {"ok": True}
            offers_text = []
            for d in discounts:
                offers_text.append(f"- {d['name']} (Discount, {d['discount_percentage']}%): {'Active' if d['active'] else 'Pending/Inactive'}")
            for g in giveaways:
                offers_text.append(f"- {g['name']} (Giveaway): {'Active' if g['active'] else 'Pending/Inactive'}")
            await send_message(chat_id, "Your discounts/giveaways:\n" + "\n".join(offers_text))
        except Exception as e:
            logger.error(f"Failed to list discounts for chat_id {chat_id}: {str(e)}")
            await send_message(chat_id, "Failed to retrieve discounts. Please try again.")
        return {"ok": True}

    # Registration steps
    if state.get("stage") == "awaiting_name":
        if len(text) > MAX_NAME_LENGTH:
            await send_message(chat_id, f"Business name too long. Please use {MAX_NAME_LENGTH} characters or fewer:")
            return {"ok": True}
        state["data"]["name"] = text
        await send_message(chat_id, "Choose your business category:", reply_markup=await create_category_keyboard())
        state["stage"] = "awaiting_category"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_phone":
        if not re.match(r"^\+\d{10,15}$", text):
            await send_message(chat_id, "Please enter a valid phone number starting with + (e.g., +1234567890):")
            return {"ok": True}
        state["data"]["phone_number"] = text
        await send_message(chat_id, "Enter your business location (e.g., 123 Main St, City):")
        state["stage"] = "awaiting_location"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_location":
        state["data"]["location"] = text
        selected = state["data"].get("work_days", [])
        resp = await send_message(
            chat_id,
            f"Selected work days: {', '.join(selected) or 'None'}\nSelect work days:",
            reply_markup=await create_workdays_keyboard(selected)
        )
        if resp.get("ok"):
            state["temp_message_id"] = resp["result"]["message_id"]
        state["stage"] = "awaiting_work_days"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_website":
        if text.lower() == "none":
            state["data"]["website"] = None
        else:
            if not re.match(r"^(https?://)?[\w\-]+(\.[\w\-]+)+[/#?]?.*$", text):
                await send_message(chat_id, "Please enter a valid URL (e.g., https://example.com) or 'none':")
                return {"ok": True}
            state["data"]["website"] = text
        await send_message(chat_id, f"Enter a brief business description (max {MAX_DESCRIPTION_LENGTH} characters, or 'none'):")
        state["stage"] = "awaiting_description"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_description":
        if len(text) > MAX_DESCRIPTION_LENGTH:
            await send_message(chat_id, f"Description too long. Please use {MAX_DESCRIPTION_LENGTH} characters or fewer:")
            return {"ok": True}
        state["data"]["description"] = text if text.lower() != "none" else None
        await send_message(chat_id, "Enter the name of your first service (or /skip if none):")
        state["stage"] = "awaiting_service_name"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_service_name":
        if text.lower() == "/skip":
            await send_message(chat_id, "No services added. Submitting registration.")
            await submit_business_registration(chat_id, state)
            return {"ok": True}
        if len(text) > MAX_NAME_LENGTH:
            await send_message(chat_id, f"Service name too long. Please use {MAX_NAME_LENGTH} characters or fewer:")
            return {"ok": True}
        state["temp_service_name"] = text
        await send_message(chat_id, f"Enter price for {text} (number in USD):")
        state["stage"] = "awaiting_service_price"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_service_price":
        try:
            price = float(text)
            if price <= 0:
                raise ValueError("Price must be positive")
            service = state.get("temp_service_name")
            if service:
                state["data"]["prices"][service] = price
                await send_message(
                    chat_id,
                    f"Added {service}: ${price}. Add another service?",
                    reply_markup=await create_yes_no_keyboard("add_service")
                )
                state["stage"] = "awaiting_add_another"
                del state["temp_service_name"]
                set_state(chat_id, state)
            else:
                await send_message(chat_id, "Error: No service name set. Please start over with /register.")
        except ValueError:
            await send_message(chat_id, "Please enter a valid number for price (e.g., 50.00).")
        return {"ok": True}

    # Discount/Giveaway steps
    if state.get("stage") == "awaiting_discount_name":
        if len(text) > MAX_NAME_LENGTH:
            await send_message(chat_id, f"Name too long. Please use {MAX_NAME_LENGTH} characters or fewer:")
            return {"ok": True}
        state["data"]["name"] = text
        await send_message(chat_id, "Choose the category for this offer:", reply_markup=await create_category_keyboard())
        state["stage"] = "awaiting_discount_category"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "awaiting_discount_percentage":
        try:
            percentage = int(text)
            if not (MIN_DISCOUNT_PERCENTAGE <= percentage <= MAX_DISCOUNT_PERCENTAGE):
                await send_message(
                    chat_id,
                    f"Percentage must be between {MIN_DISCOUNT_PERCENTAGE}% and {MAX_DISCOUNT_PERCENTAGE}%. Please try again:"
                )
                return {"ok": True}
            state["data"]["discount_percentage"] = percentage
            await submit_discount(chat_id, state)
        except ValueError:
            await send_message(chat_id, "Please enter a valid integer for percentage (e.g., 20).")
        return {"ok": True}

    if state.get("stage") == "awaiting_giveaway_cost":
        try:
            cost = int(text)
            if cost < 0:
                raise ValueError("Cost must be non-negative")
            state["data"]["cost"] = cost
            await submit_giveaway(chat_id, state)
        except ValueError:
            await send_message(chat_id, "Please enter a valid integer for cost (e.g., 200).")
        return {"ok": True}

    # Edit business steps
    if state.get("stage") == "edit_name":
        if len(text) > MAX_NAME_LENGTH:
            await send_message(chat_id, f"Business name too long. Please use {MAX_NAME_LENGTH} characters or fewer:")
            return {"ok": True}
        updated = await supabase_update_by_id_return("businesses", state["entry_id"], {"name": text})
        if updated:
            await send_message(chat_id, "Business name updated successfully!")
            await send_admin_message(f"Business updated:\nID: {state['entry_id']}\nNew Name: {text}")
            USER_STATES.pop(chat_id, None)
        else:
            await send_message(chat_id, "Failed to update business name. Please try again.")
        return {"ok": True}

    if state.get("stage") == "edit_phone":
        if not re.match(r"^\+\d{10,15}$", text):
            await send_message(chat_id, "Please enter a valid phone number starting with + (e.g., +1234567890):")
            return {"ok": True}
        updated = await supabase_update_by_id_return("businesses", state["entry_id"], {"phone_number": text})
        if updated:
            await send_message(chat_id, "Phone number updated successfully!")
            await send_admin_message(f"Business updated:\nID: {state['entry_id']}\nNew Phone: {text}")
            USER_STATES.pop(chat_id, None)
        else:
            await send_message(chat_id, "Failed to update phone number. Please try again.")
        return {"ok": True}

    if state.get("stage") == "edit_location":
        updated = await supabase_update_by_id_return("businesses", state["entry_id"], {"location": text})
        if updated:
            await send_message(chat_id, "Location updated successfully!")
            await send_admin_message(f"Business updated:\nID: {state['entry_id']}\nNew Location: {text}")
            USER_STATES.pop(chat_id, None)
        else:
            await send_message(chat_id, "Failed to update location. Please try again.")
        return {"ok": True}

    if state.get("stage") == "edit_website":
        if text.lower() == "none":
            website = None
        else:
            if not re.match(r"^(https?://)?[\w\-]+(\.[\w\-]+)+[/#?]?.*$", text):
                await send_message(chat_id, "Please enter a valid URL (e.g., https://example.com) or 'none':")
                return {"ok": True}
            website = text
        updated = await supabase_update_by_id_return("businesses", state["entry_id"], {"website": website})
        if updated:
            await send_message(chat_id, "Website updated successfully!")
            await send_admin_message(f"Business updated:\nID: {state['entry_id']}\nNew Website: {website or 'None'}")
            USER_STATES.pop(chat_id, None)
        else:
            await send_message(chat_id, "Failed to update website. Please try again.")
        return {"ok": True}

    if state.get("stage") == "edit_description":
        if len(text) > MAX_DESCRIPTION_LENGTH:
            await send_message(chat_id, f"Description too long. Please use {MAX_DESCRIPTION_LENGTH} characters or fewer:")
            return {"ok": True}
        description = text if text.lower() != "none" else None
        updated = await supabase_update_by_id_return("businesses", state["entry_id"], {"description": description})
        if updated:
            await send_message(chat_id, "Description updated successfully!")
            await send_admin_message(f"Business updated:\nID: {state['entry_id']}\nNew Description: {description or 'None'}")
            USER_STATES.pop(chat_id, None)
        else:
            await send_message(chat_id, "Failed to update description. Please try again.")
        return {"ok": True}

    if state.get("stage") == "edit_service_name":
        if len(text) > MAX_NAME_LENGTH:
            await send_message(chat_id, f"Service name too long. Please use {MAX_NAME_LENGTH} characters or fewer:")
            return {"ok": True}
        state["temp_service_name"] = text
        await send_message(chat_id, f"Enter price for {text} (number in USD):")
        state["stage"] = "edit_service_price"
        set_state(chat_id, state)
        return {"ok": True}

    if state.get("stage") == "edit_service_price":
        try:
            price = float(text)
            if price <= 0:
                raise ValueError("Price must be positive")
            service = state.get("temp_service_name")
            if service:
                business = await supabase_find_business(chat_id)
                prices = business.get("prices", {})
                prices[service] = price
                updated = await supabase_update_by_id_return("businesses", state["entry_id"], {"prices": prices})
                if updated:
                    await send_message(chat_id, f"Service {service} updated to ${price}!")
                    await send_admin_message(f"Business updated:\nID: {state['entry_id']}\nService {service}: ${price}")
                    USER_STATES.pop(chat_id, None)
                else:
                    await send_message(chat_id, "Failed to update service. Please try again.")
            else:
                await send_message(chat_id, "Error: No service name set. Please start over with /edit_business.")
        except ValueError:
            await send_message(chat_id, "Please enter a valid number for price (e.g., 50.00).")
        return {"ok": True}

    await send_message(chat_id, "Unknown command or state. Use /start, /register, /add_discount, /edit_business, /list_services, or /list_discounts.")
    return {"ok": True}

async def submit_business_registration(chat_id: int, state: Dict[str, Any]):
    """Submit business registration to Supabase and notify admin."""
    try:
        state["data"]["status"] = "pending"
        business = await supabase_insert_return("businesses", state["data"])
        if not business:
            await send_message(chat_id, "Failed to register business. Please try again.")
            return
        await send_message(chat_id, "Business registered successfully! Awaiting admin approval.")
        await send_admin_message(
            f"New business registration:\n"
            f"Name: {business['name']}\n"
            f"Category: {business['category']}\n"
            f"Phone: {business['phone_number']}\n"
            f"Location: {business['location']}\n"
            f"Work Days: {', '.join(business['work_days'])}\n"
            f"Website: {business['website'] or 'None'}\n"
            f"Description: {business['description'] or 'None'}\n"
            f"Services: {json.dumps(business['prices']) if business['prices'] else 'None'}",
            reply_markup={
                "inline_keyboard": [
                    [
                        {"text": "Approve", "callback_data": f"approve:{business['id']}"},
                        {"text": "Reject", "callback_data": f"reject:{business['id']}"}
                    ]
                ]
            }
        )
        USER_STATES.pop(chat_id, None)
    except Exception as e:
        logger.error(f"Failed to register business for chat_id {chat_id}: {str(e)}", exc_info=True)
        await log_error_to_supabase(f"Failed to register business for chat_id {chat_id}: {str(e)}")
        await send_message(chat_id, "Failed to register business. Please try again.")

async def submit_discount(chat_id: int, state: Dict[str, Any]):
    """Submit discount to Supabase and notify admin."""
    try:
        state["data"]["active"] = False  # Pending approval
        discount = await supabase_insert_return("discounts", state["data"])
        if not discount:
            await send_message(chat_id, "Failed to submit discount. Please try again.")
            return
        await send_message(chat_id, "Discount submitted! Awaiting admin approval.")
        await send_admin_message(
            f"New discount submission:\n"
            f"Name: {discount['name']}\n"
            f"Category: {discount['category']}\n"
            f"Business ID: {discount['business_id']}\n"
            f"Percentage: {discount['discount_percentage']}%",
            reply_markup={
                "inline_keyboard": [
                    [
                        {"text": "Approve", "callback_data": f"giveaway_approve:{discount['id']}"},
                        {"text": "Reject", "callback_data": f"giveaway_reject:{discount['id']}"}
                    ]
                ]
            }
        )
        USER_STATES.pop(chat_id, None)
    except Exception as e:
        logger.error(f"Failed to submit discount for chat_id {chat_id}: {str(e)}", exc_info=True)
        await log_error_to_supabase(f"Failed to submit discount for chat_id {chat_id}: {str(e)}")
        await send_message(chat_id, "Failed to submit discount. Please try again.")

async def submit_giveaway(chat_id: int, state: Dict[str, Any]):
    """Submit giveaway to Supabase and notify admin."""
    try:
        state["data"]["business_type"] = "salon"
        state["data"]["active"] = False  # Pending approval
        giveaway = await supabase_insert_return("giveaways", state["data"])
        if not giveaway:
            await send_message(chat_id, "Failed to submit giveaway. Please try again.")
            return
        await send_message(chat_id, "Giveaway submitted! Awaiting admin approval.")
        await send_admin_message(
            f"New giveaway submission:\n"
            f"Name: {giveaway['name']}\n"
            f"Category: {giveaway['category']}\n"
            f"Business ID: {giveaway['business_id']}\n"
            f"Cost: {giveaway['cost'] or 'N/A'} points",
            reply_markup={
                "inline_keyboard": [
                    [
                        {"text": "Approve", "callback_data": f"giveaway_approve:{giveaway['id']}"},
                        {"text": "Reject", "callback_data": f"giveaway_reject:{giveaway['id']}"}
                    ]
                ]
            }
        )
        USER_STATES.pop(chat_id, None)
    except Exception as e:
        logger.error(f"Failed to submit giveaway for chat_id {chat_id}: {str(e)}", exc_info=True)
        await log_error_to_supabase(f"Failed to submit giveaway for chat_id {chat_id}: {str(e)}")
        await send_message(chat_id, "Failed to submit giveaway. Please try again.")

async def handle_callback_query(callback_query: Dict[str, Any]):
    """Handle callback queries from inline keyboards."""
    chat_id = callback_query.get("from", {}).get("id")
    callback_data = callback_query.get("data")
    message_id = callback_query.get("message", {}).get("message_id")
    if not chat_id or not callback_data or not message_id:
        logger.error(f"Invalid callback query: chat_id={chat_id}, callback_data={callback_data}, message_id={message_id}")
        await log_error_to_supabase(f"Invalid callback query: chat_id={chat_id}, callback_data={callback_data}, message_id={message_id}")
        return {"ok": True}

    state = get_state(chat_id) or {}
    logger.info(f"Processing callback query from chat_id {chat_id}: {callback_data}")

    # Admin Approval/Rejection
    if callback_data.startswith("approve:") or callback_data.startswith("reject:"):
        action, business_id = callback_data.split(":", 1)
        status = "approved" if action == "approve" else "rejected"
        
        # Verify admin privileges
        if str(chat_id) != str(ADMIN_CHAT_ID):
            logger.warning(f"Unauthorized approval attempt by chat_id {chat_id}")
            await send_message(chat_id, "You are not authorized to approve or reject businesses.")
            return {"ok": True}

        # Find the business to get telegram_id
        try:
            def _q():
                return supabase.table("businesses").select("telegram_id, name").eq("id", business_id).limit(1).execute()
            resp = await asyncio.to_thread(_q)
            data = resp.data if hasattr(resp, "data") else resp.get("data")
            if not data:
                logger.error(f"Business not found for id {business_id}")
                await log_error_to_supabase(f"Business not found for id {business_id}")
                await edit_message(chat_id, message_id, "Error: Business not found.")
                return {"ok": True}
            business = data[0]
            user_chat_id = business["telegram_id"]
            business_name = business["name"]
        except Exception as e:
            logger.error(f"Failed to fetch business {business_id}: {str(e)}")
            await log_error_to_supabase(f"Failed to fetch business {business_id}: {str(e)}")
            await edit_message(chat_id, message_id, "Error: Failed to fetch business details.")
            return {"ok": True}

        # Update business status
        updated = await supabase_update_by_id_return("businesses", business_id, {"status": status})
        if updated:
            # Notify admin
            await edit_message(
                chat_id,
                message_id,
                f"Business '{business_name}' {status} successfully!",
                reply_markup=None
            )
            # Notify user
            await send_message(
                user_chat_id,
                f"Your business '{business_name}' has been {status}!"
            )
            logger.info(f"Business {business_id} set to {status}, user {user_chat_id} notified")
        else:
            await edit_message(
                chat_id,
                message_id,
                f"Failed to {action} business '{business_name}'. Please try again.",
                reply_markup=None
            )
            logger.error(f"Failed to update business {business_id} to {status}")
            await log_error_to_supabase(f"Failed to update business {business_id} to {status}")
        return {"ok": True}

    # Registration: Category selection
    if state.get("stage") in ["awaiting_category", "edit_category"] and callback_data.startswith("category:"):
        category = callback_data[len("category:"):]
        if category not in CATEGORIES:
            await send_message(chat_id, "Invalid category. Please choose again:", reply_markup=await create_category_keyboard())
            return {"ok": True}
        if state.get("stage") == "awaiting_category":
            state["data"]["category"] = category
            await send_message(chat_id, "Enter your business phone number (e.g., +1234567890):")
            state["stage"] = "awaiting_phone"
            set_state(chat_id, state)
        else:  # edit_category
            updated = await supabase_update_by_id_return("businesses", state["entry_id"], {"category": category})
            if updated:
                await send_message(chat_id, "Category updated successfully!")
                await send_admin_message(f"Business updated:\nID: {state['entry_id']}\nNew Category: {category}")
                USER_STATES.pop(chat_id, None)
            else:
                await send_message(chat_id, "Failed to update category. Please try again.")
        return {"ok": True}

    # Registration: Work days selection
    if state.get("stage") in ["awaiting_work_days", "edit_work_days"] and callback_data.startswith("workday:"):
        day = callback_data[len("workday:"):]
        if day == "confirm":
            if not state["data"].get("work_days"):
                await edit_message(
                    chat_id,
                    message_id,
                    "Please select at least one work day.",
                    reply_markup=await create_workdays_keyboard(state["data"].get("work_days", []))
                )
                return {"ok": True}
            if state.get("stage") == "awaiting_work_days":
                await send_message(chat_id, "Enter your business website (e.g., https://example.com, or 'none' if none):")
                state["stage"] = "awaiting_website"
                set_state(chat_id, state)
            else:  # edit_work_days
                updated = await supabase_update_by_id_return("businesses", state["entry_id"], {"work_days": state["data"]["work_days"]})
                if updated:
                    await send_message(chat_id, "Work days updated successfully!")
                    await send_admin_message(f"Business updated:\nID: {state['entry_id']}\nNew Work Days: {', '.join(state['data']['work_days'])}")
                    USER_STATES.pop(chat_id, None)
                else:
                    await send_message(chat_id, "Failed to update work days. Please try again.")
            return {"ok": True}
        if day in WEEK_DAYS:
            selected = state["data"].get("work_days", [])
            if day in selected:
                selected.remove(day)
            else:
                selected.append(day)
            state["data"]["work_days"] = selected
            set_state(chat_id, state)
            await edit_message(
                chat_id,
                message_id,
                f"Selected work days: {', '.join(selected) or 'None'}\nSelect work days:",
                reply_markup=await create_workdays_keyboard(selected)
            )
            return {"ok": True}

    # Registration: Add another service
    if state.get("stage") == "awaiting_add_another" and callback_data.startswith("add_service:"):
        choice = callback_data[len("add_service:"):]
        if choice == "yes":
            await send_message(chat_id, "Enter the next service name:")
            state["stage"] = "awaiting_service_name"
            set_state(chat_id, state)
        elif choice == "no":
            await submit_business_registration(chat_id, state)
        return {"ok": True}

    # Discount/Giveaway: Type selection
    if state.get("stage") == "awaiting_discount_type" and callback_data.startswith("discount_type:"):
        discount_type = callback_data[len("discount_type:"):]
        if discount_type == "discount":
            await send_message(chat_id, "Enter the discount name (e.g., '20% Off Nail Art'):")
            state["stage"] = "awaiting_discount_name"
        else:  # giveaway
            await send_message(chat_id, "Enter the giveaway name (e.g., 'Free Massage Session'):")
            state["stage"] = "awaiting_discount_name"
        state["data"]["type"] = discount_type
        set_state(chat_id, state)
        return {"ok": True}

    # Discount/Giveaway: Category selection
    if state.get("stage") == "awaiting_discount_category" and callback_data.startswith("category:"):
        category = callback_data[len("category:"):]
        if category not in CATEGORIES:
            await send_message(chat_id, "Invalid category. Please choose again:", reply_markup=await create_category_keyboard())
            return {"ok": True}
        state["data"]["category"] = category
        if state["data"]["type"] == "discount":
            await send_message(chat_id, "Enter the discount percentage (e.g., 20 for 20%):")
            state["stage"] = "awaiting_discount_percentage"
        else:  # giveaway
            await send_message(chat_id, "Enter the point cost for this giveaway (e.g., 200, or 0 for none):")
            state["stage"] = "awaiting_giveaway_cost"
        set_state(chat_id, state)
        return {"ok": True}

    # Edit business: Field selection
    if state.get("stage") == "edit_choose_field" and callback_data.startswith("edit_field:"):
        field = callback_data[len("edit_field:"):]
        state["stage"] = f"edit_{field}"
        if field == "category":
            await send_message(chat_id, "Choose the new category:", reply_markup=await create_category_keyboard())
        elif field == "work_days":
            business = await supabase_find_business(chat_id)
            state["data"]["work_days"] = business.get("work_days", [])
            resp = await send_message(
                chat_id,
                f"Current work days: {', '.join(state['data']['work_days']) or 'None'}\nSelect new work days:",
                reply_markup=await create_workdays_keyboard(state["data"]["work_days"])
            )
            if resp.get("ok"):
                state["temp_message_id"] = resp["result"]["message_id"]
        elif field == "prices":
            await send_message(chat_id, "Enter the new service name:")
            state["stage"] = "edit_service_name"
        elif field == "name":
            await send_message(chat_id, f"Enter the new business name (max {MAX_NAME_LENGTH} characters):")
        elif field == "phone_number":
            await send_message(chat_id, "Enter the new phone number (e.g., +1234567890):")
        elif field == "location":
            await send_message(chat_id, "Enter the new location (e.g., 123 Main St, City):")
        elif field == "website":
            await send_message(chat_id, "Enter the new website (e.g., https://example.com, or 'none'):")
        elif field == "description":
            await send_message(chat_id, f"Enter the new description (max {MAX_DESCRIPTION_LENGTH} characters, or 'none'):")
        set_state(chat_id, state)
        return {"ok": True}

    # Log unhandled callback
    logger.warning(f"Unhandled callback query: {callback_data} from chat_id {chat_id}")
    await log_error_to_supabase(f"Unhandled callback query: {callback_data} from chat_id {chat_id}")
    return {"ok": True}

@app.get("/health")
async def health() -> PlainTextResponse:
    """Health check endpoint."""
    return PlainTextResponse("OK", status_code=200)

@app.post("/hook/business_bot")
async def webhook(request: Request):
    """Webhook endpoint for Business Bot."""
    return await webhook_handler(request)

if __name__ == "__main__":
    import uvicorn
    asyncio.run(initialize_bot())
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

'''
