import os
import asyncio
import logging
import httpx
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger("httpx").setLevel(logging.DEBUG)
logging.getLogger("httpcore").setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("CENTRAL_BOT_TOKEN")

if not BOT_TOKEN:
    raise RuntimeError("CENTRAL_BOT_TOKEN must be set in .env")

async def send_message(chat_id: int, text: str, reply_markup: Optional[dict] = None, retries: int = 3):
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
        if reply_markup:
            payload["reply_markup"] = reply_markup
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

async def clear_inline_keyboard(chat_id: int, message_id: int, retries: int = 3):
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        for attempt in range(retries):
            try:
                logger.debug(f"Clearing inline keyboard for chat_id {chat_id}, message_id {message_id}")
                response = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageReplyMarkup",
                    json={"chat_id": chat_id, "message_id": message_id, "reply_markup": {}}
                )
                response.raise_for_status()
                logger.info(f"Cleared keyboard for chat_id {chat_id}, message_id {message_id}")
                return
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to clear keyboard: HTTP {e.response.status_code}")
                if e.response.status_code == 429:
                    retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    await asyncio.sleep(retry_after)
                    continue
                break
            except Exception as e:
                logger.error(f"Failed to clear keyboard: {str(e)}")
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        logger.error(f"Failed to clear keyboard for chat_id {chat_id} after {retries} attempts")

async def safe_clear_markup(chat_id: int, message_id: int):
    try:
        await clear_inline_keyboard(chat_id, message_id)
    except Exception:
        logger.debug(f"Ignored error while clearing keyboard for chat_id {chat_id}")

async def edit_message_keyboard(chat_id: int, message_id: int, reply_markup: dict, retries: int = 3):
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        payload = {"chat_id": chat_id, "message_id": message_id, "reply_markup": reply_markup}
        for attempt in range(retries):
            try:
                logger.debug(f"Editing keyboard for chat_id {chat_id}, message_id {message_id}")
                response = await client.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageReplyMarkup",
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Edited keyboard for chat_id {chat_id}, message_id {message_id}")
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Failed to edit keyboard: HTTP {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 429:
                    retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 1))
                    await asyncio.sleep(retry_after)
                    continue
                return {"ok": False, "error": f"HTTP {e.response.status_code}"}
            except Exception as e:
                logger.error(f"Failed to edit keyboard: {str(e)}", exc_info=True)
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))
                continue
        logger.error(f"Failed to edit keyboard for chat_id {chat_id} after {retries} attempts")
        return {"ok": False, "error": "Max retries reached"}

async def set_menu_button():
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        try:
            response = await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/setChatMenuButton",
                json={"menu_button": {"type": "commands"}}
            )
            response.raise_for_status()
            response = await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands",
                json={
                    "commands": [
                        {"command": "start", "description": "Start the bot"},
                        {"command": "menu", "description": "Open the menu"},
                        {"command": "myid", "description": "Get your Telegram ID"},
                        {"command": "approve", "description": "Approve a business (admin only)"},
                        {"command": "reject", "description": "Reject a business (admin only)"}
                    ]
                }
            )
            response.raise_for_status()
            logger.info("Set menu button and commands")
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to set menu button or commands: HTTP {e.response.status_code} - {e.response.text}")
        except Exception as e:
            logger.error(f"Failed to set menu button or commands: {str(e)}", exc_info=True)
