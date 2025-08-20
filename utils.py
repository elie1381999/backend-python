# utils.py
import os
import asyncio
import logging
from typing import Optional
import httpx
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CENTRAL_BOT_TOKEN = os.getenv("CENTRAL_BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

if not CENTRAL_BOT_TOKEN:
    logger.warning("CENTRAL_BOT_TOKEN not set; send_message will fail unless token provided explicitly")

# --- Telegram API helpers ---
async def send_message(chat_id: int, text: str, reply_markup: Optional[dict] = None,
                       token: Optional[str] = None, parse_mode: str = "Markdown") -> dict:
    bot_token = token or CENTRAL_BOT_TOKEN
    if not bot_token:
        raise RuntimeError("No bot token configured")
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            r = await client.post(f"https://api.telegram.org/bot{bot_token}/sendMessage", json=payload)
            r.raise_for_status()
            return r.json()
        except Exception:
            logger.exception("send_message failed")
            return {"ok": False, "error": "send_failed"}

async def edit_message_text(chat_id: int, message_id: int, text: str, reply_markup: Optional[dict] = None,
                            token: Optional[str] = None, parse_mode: str = "Markdown") -> dict:
    bot_token = token or CENTRAL_BOT_TOKEN
    if not bot_token:
        raise RuntimeError("No bot token")
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": parse_mode}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            r = await client.post(f"https://api.telegram.org/bot{bot_token}/editMessageText", json=payload)
            r.raise_for_status()
            return r.json()
        except Exception:
            logger.exception("edit_message_text failed")
            return {"ok": False, "error": "edit_failed"}

async def edit_message_keyboard(chat_id: int, message_id: int, reply_markup: dict,
                                token: Optional[str] = None) -> dict:
    bot_token = token or CENTRAL_BOT_TOKEN
    if not bot_token:
        raise RuntimeError("No bot token")
    payload = {"chat_id": chat_id, "message_id": message_id, "reply_markup": reply_markup}
    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            r = await client.post(f"https://api.telegram.org/bot{bot_token}/editMessageReplyMarkup", json=payload)
            r.raise_for_status()
            return r.json()
        except Exception:
            logger.exception("edit_message_keyboard failed")
            return {"ok": False, "error": "edit_failed"}

async def clear_inline_keyboard(chat_id: int, message_id: int, token: Optional[str] = None):
    bot_token = token or CENTRAL_BOT_TOKEN
    if not bot_token:
        raise RuntimeError("No bot token")
    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            await client.post(f"https://api.telegram.org/bot{bot_token}/editMessageReplyMarkup", json={
                "chat_id": chat_id, "message_id": message_id, "reply_markup": {}
            })
            return {"ok": True}
        except Exception:
            logger.exception("clear_inline_keyboard failed")
            return {"ok": False}

async def safe_clear_markup(chat_id: int, message_id: Optional[int], token: Optional[str] = None):
    if message_id is None:
        return
    try:
        await clear_inline_keyboard(chat_id, message_id, token=token)
    except Exception:
        logger.debug("Ignored error clearing markup")

async def set_menu_button(token: Optional[str] = None):
    bot_token = token or CENTRAL_BOT_TOKEN
    if not bot_token:
        logger.warning("No bot token for set_menu_button")
        return
    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            await client.post(f"https://api.telegram.org/bot{bot_token}/setChatMenuButton", json={"menu_button": {"type": "commands"}})
            await client.post(f"https://api.telegram.org/bot{bot_token}/setMyCommands", json={
                "commands": [
                    {"command": "start", "description": "Start the bot"},
                    {"command": "menu", "description": "Open menu"},
                    {"command": "myid", "description": "Show your ID"},
                ]
            })
        except Exception:
            logger.exception("set_menu_button failed")

# --- Keyboards ---
def create_menu_options_keyboard():
    return {"inline_keyboard": [[{"text": "Main Menu", "callback_data": "menu:main"}], [{"text": "Change Language", "callback_data": "menu:language"}]]}

def create_language_keyboard():
    return {"inline_keyboard": [[{"text": "English", "callback_data": "lang:en"}], [{"text": "Русский", "callback_data": "lang:ru"}]]}

def create_gender_keyboard():
    return {"inline_keyboard": [[{"text": "Female", "callback_data": "gender:female"}, {"text": "Male", "callback_data": "gender:male"}]]}

def create_interests_keyboard(selected: list = None, interests: list = None, emojis: list = None):
    if selected is None:
        selected = []
    if interests is None:
        interests = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining", "Discounts only", "Giveaways only"]
    if emojis is None:
        emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣"]
    buttons = []
    for i, it in enumerate(interests):
        label = it
        for idx, sel in enumerate(selected):
            if sel == it and idx < len(emojis):
                label = f"{emojis[idx]} {it}"
                break
        buttons.append([{"text": label, "callback_data": f"interest:{it}"}])
    buttons.append([{"text": "Done", "callback_data": "interests_done"}])
    return {"inline_keyboard": buttons}

def create_main_menu_keyboard():
    return {"inline_keyboard": [
        [{"text": "My Points", "callback_data": "menu:points"}],
        [{"text": "Profile", "callback_data": "menu:profile"}],
        [{"text": "Discounts", "callback_data": "menu:discounts"}],
        [{"text": "Giveaways", "callback_data": "menu:giveaways"}],
        [{"text": "Refer Friends", "callback_data": "menu:refer"}]
    ]}

def create_categories_keyboard(categories: list = None):
    if categories is None:
        categories = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining"]
    keyboard = {"inline_keyboard": [[{"text": c, "callback_data": f"discount_category:{c}"}] for c in categories]}
    return keyboard

def create_phone_keyboard():
    return {"keyboard": [[{"text": "Share phone", "request_contact": True}]], "resize_keyboard": True, "one_time_keyboard": True}

'''
# utils.py
import os
import asyncio
import logging
from typing import Optional, Dict, Any
import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

CENTRAL_BOT_TOKEN = os.getenv("CENTRAL_BOT_TOKEN")

if not CENTRAL_BOT_TOKEN:
    logger.warning("CENTRAL_BOT_TOKEN not set; send_message will fail unless a token is passed explicitly")

# --- Telegram helpers ------------------------------------------------------

async def send_message(chat_id: int, text: str, reply_markup: Optional[dict] = None,
                       token: Optional[str] = None, parse_mode: str = "Markdown", retries: int = 3):
    """Send a Telegram message using async httpx. If token omitted, uses CENTRAL_BOT_TOKEN env var."""
    bot_token = token or CENTRAL_BOT_TOKEN
    if not bot_token:
        raise RuntimeError("No bot token configured for send_message")

    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup

    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        for attempt in range(retries):
            try:
                logger.debug(f"send_message attempt {attempt+1} -> chat {chat_id}: {text!r}")
                r = await client.post(f"https://api.telegram.org/bot{bot_token}/sendMessage", json=payload)
                r.raise_for_status()
                return r.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"send_message HTTP {e.response.status_code}: {e.response.text}")
                if e.response.status_code == 429:
                    try:
                        retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    except Exception:
                        retry_after = 1
                    await asyncio.sleep(retry_after)
                    continue
                return {"ok": False, "error": f"HTTP {e.response.status_code}"}
            except Exception as exc:
                logger.exception("send_message error")
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        return {"ok": False, "error": "max_retries"}

async def edit_message_text(chat_id: int, message_id: int, text: str, reply_markup: Optional[dict] = None,
                            token: Optional[str] = None, parse_mode: str = "Markdown", retries: int = 3):
    bot_token = token or CENTRAL_BOT_TOKEN
    if not bot_token:
        raise RuntimeError("No bot token configured for edit_message_text")
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": parse_mode}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        for attempt in range(retries):
            try:
                r = await client.post(f"https://api.telegram.org/bot{bot_token}/editMessageText", json=payload)
                r.raise_for_status()
                return r.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"edit_message_text HTTP {e.response.status_code}: {e.response.text}")
                if e.response.status_code == 429:
                    try:
                        retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    except Exception:
                        retry_after = 1
                    await asyncio.sleep(retry_after)
                    continue
                return {"ok": False, "error": f"HTTP {e.response.status_code}"}
            except Exception:
                logger.exception("edit_message_text error")
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        return {"ok": False, "error": "max_retries"}

async def edit_message_keyboard(chat_id: int, message_id: int, reply_markup: dict,
                                token: Optional[str] = None, retries: int = 3):
    bot_token = token or CENTRAL_BOT_TOKEN
    if not bot_token:
        raise RuntimeError("No bot token configured for edit_message_keyboard")
    payload = {"chat_id": chat_id, "message_id": message_id, "reply_markup": reply_markup}
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        for attempt in range(retries):
            try:
                r = await client.post(f"https://api.telegram.org/bot{bot_token}/editMessageReplyMarkup", json=payload)
                r.raise_for_status()
                return r.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"edit_message_keyboard HTTP {e.response.status_code}: {e.response.text}")
                if e.response.status_code == 429:
                    try:
                        retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    except Exception:
                        retry_after = 1
                    await asyncio.sleep(retry_after)
                    continue
                return {"ok": False, "error": f"HTTP {e.response.status_code}"}
            except Exception:
                logger.exception("edit_message_keyboard error")
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        return {"ok": False, "error": "max_retries"}

async def clear_inline_keyboard(chat_id: int, message_id: int, token: Optional[str] = None, retries: int = 3):
    bot_token = token or CENTRAL_BOT_TOKEN
    if not bot_token:
        raise RuntimeError("No bot token configured for clear_inline_keyboard")
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        for attempt in range(retries):
            try:
                r = await client.post(
                    f"https://api.telegram.org/bot{bot_token}/editMessageReplyMarkup",
                    json={"chat_id": chat_id, "message_id": message_id, "reply_markup": {}}
                )
                r.raise_for_status()
                return r.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"clear_inline_keyboard HTTP {e.response.status_code}")
                if e.response.status_code == 429:
                    try:
                        retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    except Exception:
                        retry_after = 1
                    await asyncio.sleep(retry_after)
                    continue
                break
            except Exception:
                logger.exception("clear_inline_keyboard")
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        return {"ok": False, "error": "max_retries"}

async def safe_clear_markup(chat_id: int, message_id: Optional[int], token: Optional[str] = None):
    if message_id is None:
        return
    try:
        await clear_inline_keyboard(chat_id, message_id, token=token)
    except Exception:
        logger.debug("Ignored error clearing markup", exc_info=True)

async def set_menu_button(token: Optional[str] = None):
    """Set chat menu button + default commands for a bot."""
    bot_token = token or CENTRAL_BOT_TOKEN
    if not bot_token:
        logger.warning("No bot token set for set_menu_button")
        return
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        try:
            await client.post(f"https://api.telegram.org/bot{bot_token}/setChatMenuButton", json={"menu_button": {"type": "commands"}})
            await client.post(f"https://api.telegram.org/bot{bot_token}/setMyCommands", json={
                "commands": [
                    {"command": "start", "description": "Start the bot"},
                    {"command": "menu", "description": "Open the menu"},
                    {"command": "myid", "description": "Get your Telegram ID"},
                    {"command": "approve", "description": "Approve a business (admin only)"},
                    {"command": "reject", "description": "Reject a business (admin only)"},
                ]
            })
            logger.info("set_menu_button completed")
        except Exception:
            logger.exception("set_menu_button error")

# --- Keyboards -------------------------------------------------------------

def create_menu_options_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "Main Menu", "callback_data": "menu:main"}],
            [{"text": "Change Language", "callback_data": "menu:language"}]
        ]
    }

def create_language_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "English", "callback_data": "lang:en"}],
            [{"text": "Русский", "callback_data": "lang:ru"}]
        ]
    }

def create_gender_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "Female", "callback_data": "gender:female"},
                {"text": "Male", "callback_data": "gender:male"}
            ]
        ]
    }

def create_interests_keyboard(selected: list = None, interests: list = None, emojis: list = None):
    if selected is None:
        selected = []
    if interests is None:
        interests = INTERESTS
    if emojis is None:
        emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣"]
    buttons = []
    for i, interest in enumerate(interests):
        text = interest
        for idx, sel in enumerate(selected):
            if sel == interest:
                text = f"{emojis[idx]} {interest}"
                break
        buttons.append([{"text": text, "callback_data": f"interest:{interest}"}])
    buttons.append([{"text": "Done", "callback_data": "interests_done"}])
    return {"inline_keyboard": buttons}

def create_main_menu_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "My Points", "callback_data": "menu:points"}],
            [{"text": "Profile", "callback_data": "menu:profile"}],
            [{"text": "Discounts", "callback_data": "menu:discounts"}],
            [{"text": "Giveaways", "callback_data": "menu:giveaways"}],
            [{"text": "Refer Friends", "callback_data": "menu:refer"}]
        ]
    }

def create_categories_keyboard(categories: list = None):
    if categories is None:
        categories = CATEGORIES
    buttons = []
    for cat in categories:
        buttons.append([{"text": cat, "callback_data": f"discount_category:{cat}"}])
    return {"inline_keyboard": buttons}

def create_phone_keyboard():
    return {
        "keyboard": [[{"text": "Share phone", "request_contact": True}]],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }
'''
