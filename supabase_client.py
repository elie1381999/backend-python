import os
import asyncio
import httpx
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
import aiohttp

load_dotenv()

# ---------- Config ----------
SUPA_BASE = os.getenv("SUPABASE_URL", "").rstrip("/") + "/rest/v1"
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

# network / retry settings
HTTP_TIMEOUT = httpx.Timeout(15.0, connect=5.0)  # total, connect
MAX_RETRIES = 3
BACKOFF_BASE = 0.5
# optional: respect environment proxy vars by default (httpx does this if trust_env=True)
HTTP_CLIENT_KWARGS = {"timeout": HTTP_TIMEOUT, "trust_env": True}


# ---------- Internal helper ----------
async def _request_with_retries(method, url, retries=3, **kwargs):
    for attempt in range(retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(method, url, **kwargs) as resp:
                    # If response is 2xx but no JSON content, return None or empty
                    if resp.status < 500:
                        content_type = resp.headers.get("Content-Type", "")
                        if "application/json" in content_type:
                            data = await resp.json()
                            return data
                        else:
                            # No JSON to decode (e.g., 201 Created with empty body)
                            return None
                    else:
                        # Retry on server errors >=500
                        # Optionally, read body and print/log
                        body = await resp.text()
                        print(f"Server error {resp.status}: {body}")
        except aiohttp.ClientConnectionError as e:
            if attempt == retries - 1:
                raise
            # optionally add delay here with backoff
        except aiohttp.ContentTypeError:
            # response is not JSON, treat as no content
            return None
    raise RuntimeError(f"Failed after {retries} retries: {url}")



async def create_giveaway_entry(data):
    url = f"{SUPA_BASE}/giveaway_entries"
    data_resp = await _request_with_retries("POST", url, headers=HEADERS, json=data)
    return data_resp[0] if data_resp else None


async def update_giveaway_entry(entry_id, data):
    url = f"{SUPA_BASE}/giveaway_entries?id=eq.{entry_id}"
    data_resp = await _request_with_retries("PATCH", url, headers=HEADERS, json=data)
    return data_resp[0] if data_resp else None


async def get_draft_by_chat(chat_id):
    url = f"{SUPA_BASE}/giveaway_entries"
    params = {
        "telegram_chat_id": f"eq.{chat_id}",
        "is_draft": "eq.true",
        "select": "*"
    }
    data_resp = await _request_with_retries("GET", url, headers=HEADERS, params=params)
    return data_resp[0] if data_resp else None


# ---------- Salon lookups ----------
async def get_salon_by_bot_username(bot_username: str) -> Optional[Dict[str, Any]]:
    url = f"{SUPA_BASE}/salons"
    params = {"telegram_bot_username": f"eq.{bot_username}", "select": "id,name,telegram_bot_username"}
    arr = await _request_with_retries("GET", url, headers=HEADERS, params=params)
    return arr[0] if arr else None


async def get_salon_by_webhook_id(webhook_id: str) -> Optional[Dict[str, Any]]:
    url = f"{SUPA_BASE}/salons"
    params = {"webhook_id": f"eq.{webhook_id}", "select": "id,name,telegram_bot_username"}
    arr = await _request_with_retries("GET", url, headers=HEADERS, params=params)
    return arr[0] if arr else None


async def get_telegram_token_by_bot_username(bot_username: str) -> Optional[str]:
    url = f"{SUPA_BASE}/salons"
    params = {"telegram_bot_username": f"eq.{bot_username}", "select": "telegram_bot_token"}
    arr = await _request_with_retries("GET", url, headers=HEADERS, params=params)
    return arr[0]["telegram_bot_token"] if arr and "telegram_bot_token" in arr[0] else None


async def get_services_for_salon(salon_id: str) -> List[Dict[str, Any]]:
    url = f"{SUPA_BASE}/salon_services"
    params = {
        "salon_id": f"eq.{salon_id}",
        "is_active": "eq.true",
        "select": "variant_name,price,duration,description"
    }
    arr = await _request_with_retries("GET", url, headers=HEADERS, params=params)
    return arr if arr else []


# ---------- logging helper ----------
async def log_message_to_db(username: str, text: str) -> None:
    url = f"{SUPA_BASE}/testmsg"
    payload = {"username": username, "text": text}
    try:
        await _request_with_retries("POST", url, headers=HEADERS, json=payload)
    except Exception as exc:
        # non-fatal: log and continue
        print("[supabase_client] log_message_to_db failed:", exc)


# ---------- Telegram send helper ----------
async def send_telegram_message(token: str, chat_id: int, text: str, parse_mode: Optional[str] = "Markdown") -> Dict[str, Any]:
    """Send a message via Telegram Bot API. Returns Telegram response JSON."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
            r = await client.post(url, json=payload)
        if r.status_code != 200:
            print("[supabase_client] Telegram error:", r.status_code, r.text)
            r.raise_for_status()
        return r.json()
    except Exception as exc:
        print("[supabase_client] send_telegram_message failed:", exc)
        raise


# ---------- Optional utilities ----------
def check_env_ok() -> bool:
    """Quick helper for startup checks."""
    if not SUPA_BASE or not SUPABASE_KEY:
        print("[supabase_client] WARNING: SUPABASE_URL or SUPABASE_KEY is not set.")
        return False
    return True
