import os
import asyncio
import logging
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any

import httpx
from dotenv import load_dotenv
from supabase import create_client, Client

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

BOT_TOKEN = os.getenv("CENTRAL_BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Validate environment variables
if not all([BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY]):
    raise RuntimeError("Missing required environment variables: CENTRAL_BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Helpers: normalize supabase execute() responses ---
def _extract_resp_data(resp) -> Optional[List[Dict[str, Any]]]:
    if resp is None:
        return None
    if isinstance(resp, dict):
        if resp.get("error"):
            raise RuntimeError(f"Supabase error: {resp['error']}")
        return resp.get("data")
    if hasattr(resp, "data"):
        return resp.data
    if isinstance(resp, list):
        return resp
    return None

async def _run_in_thread(fn, *args, **kwargs):
    return await asyncio.to_thread(lambda: fn(*args, **kwargs))

# --- Querying users by filters ---
async def fetch_users_by_city(city: str, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Fetch users where location equals `city` and is_draft = false.
    Use limit/offset for pagination.
    """
    def _q():
        return supabase.table("central_bot_leads") \
            .select("*") \
            .eq("location", city) \
            .eq("is_draft", False) \
            .order("joined_at", desc=False) \
            .limit(limit) \
            .offset(offset) \
            .execute()
    try:
        resp = await _run_in_thread(_q)
        data = _extract_resp_data(resp)
        logger.info(f"Fetched {len(data or [])} users for city {city} at offset {offset}")
        return data or []
    except Exception as e:
        logger.error(f"Failed to fetch users by city {city}: {e}")
        return []

async def fetch_users_by_age_range(min_age: int, max_age: int, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Fetch users whose DOB falls between two dates so their age is between min_age and max_age.
    """
    today = date.today()
    newest_dob = (today - timedelta(days=365 * max_age + max_age // 4)).isoformat()
    oldest_dob = (today - timedelta(days=365 * min_age + min_age // 4)).isoformat()
    def _q():
        return supabase.table("central_bot_leads") \
            .select("*") \
            .gte("dob", newest_dob) \
            .lte("dob", oldest_dob) \
            .eq("is_draft", False) \
            .limit(limit) \
            .offset(offset) \
            .execute()
    try:
        resp = await _run_in_thread(_q)
        data = _extract_resp_data(resp)
        logger.info(f"Fetched {len(data or [])} users for age range {min_age}-{max_age} at offset {offset}")
        return data or []
    except Exception as e:
        logger.error(f"Failed to fetch users by age range {min_age}-{max_age}: {e}")
        return []

async def fetch_users_by_interest(interest: str, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Fetch users whose interests array contains the specific interest using a PostgreSQL function.
    """
    def _q():
        return supabase.rpc("query_users_by_interest", {
            "interest": interest,
            "limit_val": limit,
            "offset_val": offset
        }).execute()
    try:
        resp = await _run_in_thread(_q)
        data = _extract_resp_data(resp)
        logger.info(f"Fetched {len(data or [])} users for interest {interest} at offset {offset}")
        return data or []
    except Exception as e:
        logger.error(f"Failed to fetch users by interest {interest}: {e}")
        return []

# --- Telegram sender with concurrency and retries ---
TELEGRAM_URL = "https://api.telegram.org/bot{token}/sendMessage"
DEFAULT_CONCURRENCY = 4
DEFAULT_RETRIES = 2
SEND_TIMEOUT = 10.0

async def _send_telegram(chat_id: int, text: str) -> dict:
    async with httpx.AsyncClient(timeout=httpx.Timeout(SEND_TIMEOUT)) as client:
        try:
            r = await client.post(TELEGRAM_URL.format(token=BOT_TOKEN), json={"chat_id": chat_id, "text": text})
            try:
                response = r.json()
                if not response.get("ok"):
                    logger.error(f"Telegram API error for chat_id {chat_id}: {response}")
                else:
                    logger.info(f"Telegram message sent to chat_id {chat_id}: {response}")
                return response
            except Exception as e:
                logger.error(f"Failed to parse Telegram response for chat_id {chat_id}: {e}, status: {r.status_code}, text: {r.text}")
                return {"ok": False, "status_code": r.status_code, "text": r.text}
        except Exception as e:
            logger.error(f"Failed to send Telegram message to chat_id {chat_id}: {e}")
            return {"ok": False, "error": str(e)}

async def broadcast_messages(users: List[Dict[str, Any]], message: str, concurrency: int = DEFAULT_CONCURRENCY):
    sem = asyncio.Semaphore(concurrency)
    results = []

    async def _safe_send(user):
        async with sem:
            chat_id = user.get("telegram_id")
            if not chat_id:
                logger.warning(f"No chat_id for user {user.get('id')}")
                return {"ok": False, "reason": "no_chat_id", "user": user}
            last_exc = None
            for attempt in range(1, DEFAULT_RETRIES + 1):
                try:
                    resp = await _send_telegram(chat_id, message)
                    if isinstance(resp, dict) and resp.get("ok"):
                        try:
                            await _run_in_thread(lambda: supabase.table("central_bot_leads").update(
                                {"last_notified_at": datetime.utcnow().isoformat()}
                            ).eq("id", user["id"]).execute())
                        except Exception as e:
                            logger.error(f"Failed to update last_notified_at for user {user['id']}: {e}")
                        return {"ok": True, "chat_id": chat_id}
                    else:
                        last_exc = resp
                except Exception as exc:
                    last_exc = exc
                await asyncio.sleep(1.0 * attempt)
            logger.error(f"Failed to send message to chat_id {chat_id} after {DEFAULT_RETRIES} attempts: {last_exc}")
            return {"ok": False, "chat_id": chat_id, "error": str(last_exc)}

    tasks = [asyncio.create_task(_safe_send(u)) for u in users]
    for t in asyncio.as_completed(tasks):
        results.append(await t)
    return results

# --- High-level convenience functions ---
async def notify_city(city: str, message: str, page_size: int = 200) -> dict:
    """
    Notify all finalized users in a specific city.
    Pages through results to avoid fetching too many rows into memory.
    """
    logger.info(f"Notifying city: {city} with message: {message}")
    offset = 0
    all_results = []
    total_sent = 0
    while True:
        try:
            users = await fetch_users_by_city(city, limit=page_size, offset=offset)
            logger.info(f"Fetched {len(users)} users for city {city} at offset {offset}")
            if not users:
                break
            results = await broadcast_messages(users, message)
            all_results.extend(results)
            total_sent += sum(1 for r in results if r.get("ok"))
            offset += page_size
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Error in notify_city for city {city} at offset {offset}: {e}")
            break
    logger.info(f"Completed notification for city {city}: {total_sent}/{len(all_results)} sent successfully")
    return {"city": city, "total_targeted": len(all_results), "sent_success": total_sent, "details": all_results}

async def notify_with_filters(city: Optional[str] = None, interest: Optional[str] = None,
                             min_age: Optional[int] = None, max_age: Optional[int] = None,
                             message: str = "", page_size: int = 200):
    """
    Notify users based on city, interest, and/or age range filters.
    """
    logger.info(f"Notifying with filters: city={city}, interest={interest}, min_age={min_age}, max_age={max_age}")
    users = []
    offset = 0
    while True:
        try:
            if city:
                page = await fetch_users_by_city(city, limit=page_size, offset=offset)
            elif interest:
                page = await fetch_users_by_interest(interest, limit=page_size, offset=offset)
            elif min_age is not None and max_age is not None:
                page = await fetch_users_by_age_range(min_age, max_age, limit=page_size, offset=offset)
            else:
                def _q_all():
                    return supabase.table("central_bot_leads").select("*").eq("is_draft", False).limit(page_size).offset(offset).execute()
                resp = await _run_in_thread(_q_all)
                page = _extract_resp_data(resp) or []
            logger.info(f"Fetched {len(page)} users at offset {offset}")
            if not page:
                break
            users.extend(page)
            offset += page_size
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Error fetching users at offset {offset}: {e}")
            break

    if interest and not city:
        users = [u for u in users if isinstance(u.get("interests"), list) and interest in u.get("interests")]
        logger.info(f"Filtered {len(users)} users by interest {interest}")

    if min_age is not None or max_age is not None:
        def age_from_dob_row(row):
            dob = row.get("dob")
            if not dob:
                return None
            try:
                dob_date = datetime.fromisoformat(dob.replace('Z', '+00:00')).date() if isinstance(dob, str) else dob
            except Exception:
                return None
            today = date.today()
            return today.year - dob_date.year - ((today.month, today.day) < (dob_date.month, dob_date.day))
        filtered = []
        for u in users:
            a = age_from_dob_row(u)
            if a is None:
                continue
            ok = True
            if min_age is not None and a < min_age: ok = False
            if max_age is not None and a > max_age: ok = False
            if ok: filtered.append(u)
        users = filtered
        logger.info(f"Filtered {len(users)} users by age range {min_age}-{max_age}")

    results = await broadcast_messages(users, message)
    sent = sum(1 for r in results if r.get("ok"))
    logger.info(f"Completed notification: {sent}/{len(users)} sent successfully")
    return {"targeted": len(users), "sent": sent, "details": results}