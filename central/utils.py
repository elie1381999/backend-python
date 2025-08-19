# central/utils.py
import os
import asyncio
import random
import logging
import uuid as _uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, List, Tuple

import httpx
from dotenv import load_dotenv
from supabase import create_client, Client

# Load .env
load_dotenv()

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("central.utils")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# Environment variables (expected names used elsewhere)
BOT_TOKEN = os.getenv("CENTRAL_BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# Minimal env check (non-fatal here so imports in other contexts can still work)
if not BOT_TOKEN or not SUPABASE_URL or not SUPABASE_KEY:
    logger.warning("One or more Supabase/Telegram env vars missing (CENTRAL_BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY). Some functions will fail at runtime.")

# Supabase client
supabase: Optional[Client] = None
try:
    if SUPABASE_URL and SUPABASE_KEY:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception:
    logger.exception("Failed to create supabase client")

# In-memory state for user flows (same shape your handlers expect)
USER_STATES: Dict[int, Dict[str, Any]] = {}
STATE_TTL_SECONDS = 30 * 60  # 30 minutes

# Domain model constants (shared)
INTERESTS = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining", "Discounts only", "Giveaways only"]
CATEGORIES = ["Nails", "Hair", "Lashes", "Massage", "Spa", "Fine Dining", "Casual Dining"]
EMOJIS = ["1️⃣", "2️⃣", "3️⃣"]
STARTER_POINTS = 100

# Points config
POINTS_SIGNUP = 20
POINTS_PROFILE_COMPLETE = 40
POINTS_VIEW_DISCOUNT = 5
POINTS_CLAIM_PROMO = 10
POINTS_BOOKING_CREATED = 15
POINTS_BOOKING_VERIFIED = 200
POINTS_REFERRAL_JOIN = 10
POINTS_REFERRAL_VERIFIED = 100

DAILY_POINTS_CAP = 2000
TIER_THRESHOLDS = [
    ("Bronze", 0),
    ("Silver", 200),
    ("Gold", 500),
    ("Platinum", 1000),
]

# Utility helpers
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def compute_tier(points: int) -> str:
    tier = "Bronze"
    for name, threshold in TIER_THRESHOLDS:
        if points >= threshold:
            tier = name
    return tier

def compute_tier_progress(points: int) -> Tuple[str, int, int]:
    """
    Return (current_tier_name, points_into_tier, points_needed_for_next_tier)
    If on max tier, points_needed_for_next_tier will be 0.
    """
    thresholds = TIER_THRESHOLDS
    current_name = thresholds[0][0]
    current_threshold = thresholds[0][1]
    next_threshold = None
    for i, (name, thresh) in enumerate(thresholds):
        if points >= thresh:
            current_name = name
            current_threshold = thresh
            next_threshold = thresholds[i+1][1] if i+1 < len(thresholds) else None
    points_into = points - current_threshold
    if next_threshold is None:
        needed = 0
    else:
        needed = next_threshold - points
        if needed < 0:
            needed = 0
    return current_name, points_into, needed

# State management
def set_state(chat_id: int, state: Dict[str, Any]) -> None:
    state["updated_at"] = now_iso()
    USER_STATES[chat_id] = state

def get_state(chat_id: int) -> Optional[Dict[str, Any]]:
    st = USER_STATES.get(chat_id)
    if not st:
        return None
    try:
        updated = datetime.fromisoformat(st.get("updated_at"))
        if (datetime.now(timezone.utc) - updated).total_seconds() > STATE_TTL_SECONDS:
            USER_STATES.pop(chat_id, None)
            return None
    except Exception:
        USER_STATES.pop(chat_id, None)
        return None
    return st

# Small wrapper to expose uuid module for handlers that expect `uuid` symbol
uuid = _uuid

# Telegram helpers (async)
async def _post_telegram(path: str, payload: dict, timeout: float = 20.0) -> dict:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN not configured")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{path}"
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()
        return r.json()

async def send_message(chat_id: int, text: str, reply_markup: Optional[dict] = None, parse_mode: str = "Markdown") -> dict:
    try:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        return await _post_telegram("sendMessage", payload)
    except httpx.HTTPStatusError as e:
        logger.error("Failed to send message HTTP %s: %s", e.response.status_code, e.response.text)
        return {"ok": False, "error": str(e)}
    except Exception:
        logger.exception("send_message failed")
        return {"ok": False, "error": "exception"}

async def edit_message_keyboard(chat_id: int, message_id: int, reply_markup: dict) -> dict:
    try:
        payload = {"chat_id": chat_id, "message_id": message_id, "reply_markup": reply_markup}
        return await _post_telegram("editMessageReplyMarkup", payload)
    except Exception:
        logger.exception("edit_message_keyboard failed")
        return {"ok": False}

async def clear_inline_keyboard(chat_id: int, message_id: int) -> dict:
    try:
        payload = {"chat_id": chat_id, "message_id": message_id, "reply_markup": {}}
        return await _post_telegram("editMessageReplyMarkup", payload)
    except Exception:
        logger.exception("clear_inline_keyboard failed")
        return {"ok": False}

async def safe_clear_markup(chat_id: int, message_id: int) -> None:
    try:
        await clear_inline_keyboard(chat_id, message_id)
    except Exception:
        logger.debug("safe_clear_markup ignored failure")

# Keyboards (exported for handlers)
def create_menu_options_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [{"text": "Main Menu", "callback_data": "menu:main"}],
            [{"text": "Change Language", "callback_data": "menu:language"}],
        ]
    }

def create_language_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [{"text": "English", "callback_data": "lang:en"}],
            [{"text": "Русский", "callback_data": "lang:ru"}],
        ]
    }

def create_gender_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "Female", "callback_data": "gender:female"},
                {"text": "Male", "callback_data": "gender:male"},
            ]
        ]
    }

def create_interests_keyboard(selected: List[str] = []) -> dict:
    buttons = []
    for i, interest in enumerate(INTERESTS):
        text = interest
        for idx, sel in enumerate(selected):
            if sel == interest:
                text = f"{EMOJIS[idx]} {interest}"
                break
        buttons.append([{"text": text, "callback_data": f"interest:{interest}"}])
    buttons.append([{"text": "Done", "callback_data": "interests_done"}])
    return {"inline_keyboard": buttons}

def create_main_menu_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [{"text": "My Points", "callback_data": "menu:points"}],
            [{"text": "Profile", "callback_data": "menu:profile"}],
            [{"text": "Discounts", "callback_data": "menu:discounts"}],
            [{"text": "Giveaways", "callback_data": "menu:giveaways"}],
        ]
    }

def create_categories_keyboard() -> dict:
    buttons = []
    for cat in CATEGORIES:
        buttons.append([{"text": cat, "callback_data": f"discount_category:{cat}"}])
    return {"inline_keyboard": buttons}

def create_phone_keyboard() -> dict:
    return {
        "keyboard": [[{"text": "Share phone", "request_contact": True}]],
        "resize_keyboard": True,
        "one_time_keyboard": True,
    }

# Supabase wrappers (synchronous queries executed in threads)
def _ensure_supabase():
    if not supabase:
        raise RuntimeError("Supabase client not configured")

async def supabase_insert_return(table: str, payload: dict) -> Optional[Dict[str, Any]]:
    _ensure_supabase()
    def _ins():
        return supabase.table(table).insert(payload).execute()
    try:
        resp = await asyncio.to_thread(_ins)
        data = getattr(resp, "data", None) or resp.get("data") if isinstance(resp, dict) else None
        if not data:
            logger.error("Insert returned no data for table %s", table)
            return None
        return data[0] if isinstance(data, list) else data
    except Exception:
        logger.exception("supabase_insert_return failed")
        return None

async def supabase_update_by_id_return(table: str, entry_id: str, payload: dict) -> Optional[Dict[str, Any]]:
    _ensure_supabase()
    def _upd():
        return supabase.table(table).update(payload).eq("id", entry_id).execute()
    try:
        resp = await asyncio.to_thread(_upd)
        data = getattr(resp, "data", None) or resp.get("data") if isinstance(resp, dict) else None
        if not data:
            logger.error("Update returned no data for table %s id %s", table, entry_id)
            return None
        return data[0] if isinstance(data, list) else data
    except Exception:
        logger.exception("supabase_update_by_id_return failed")
        return None

async def supabase_find_draft(chat_id: int) -> Optional[Dict[str, Any]]:
    _ensure_supabase()
    def _q():
        return supabase.table("central_bot_leads").select("*").eq("telegram_id", chat_id).eq("is_draft", True).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = getattr(resp, "data", None) or resp.get("data", []) if isinstance(resp, dict) else []
        return rows[0] if rows else None
    except Exception:
        logger.exception("supabase_find_draft failed")
        return None

async def supabase_find_registered(chat_id: int) -> Optional[Dict[str, Any]]:
    _ensure_supabase()
    def _q():
        return supabase.table("central_bot_leads").select("*").eq("telegram_id", chat_id).eq("is_draft", False).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = getattr(resp, "data", None) or resp.get("data", []) if isinstance(resp, dict) else []
        return rows[0] if rows else None
    except Exception:
        logger.exception("supabase_find_registered failed")
        return None

async def supabase_find_business(business_id: str) -> Optional[Dict[str, Any]]:
    _ensure_supabase()
    def _q():
        return supabase.table("businesses").select("*").eq("id", business_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = getattr(resp, "data", None) or resp.get("data", []) if isinstance(resp, dict) else []
        return rows[0] if rows else None
    except Exception:
        logger.exception("supabase_find_business failed")
        return None

async def supabase_find_discount(discount_id: str) -> Optional[Dict[str, Any]]:
    _ensure_supabase()
    def _q():
        return supabase.table("discounts").select("*").eq("id", discount_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = getattr(resp, "data", None) or resp.get("data", []) if isinstance(resp, dict) else []
        return rows[0] if rows else None
    except Exception:
        logger.exception("supabase_find_discount failed")
        return None

async def supabase_find_discounts_by_category(category: str) -> List[Dict[str, Any]]:
    _ensure_supabase()
    def _q():
        return supabase.table("discounts").select("id, name, discount_percentage, category, business_id").eq("category", category).eq("active", True).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = getattr(resp, "data", None) or resp.get("data", []) if isinstance(resp, dict) else []
        return rows or []
    except Exception:
        logger.exception("supabase_find_discounts_by_category failed")
        return []

async def supabase_find_business_categories(business_id: str) -> List[str]:
    _ensure_supabase()
    def _q():
        return supabase.table("business_categories").select("category").eq("business_id", business_id).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = getattr(resp, "data", None) or resp.get("data", []) if isinstance(resp, dict) else []
        return [r["category"] for r in rows] if rows else []
    except Exception:
        logger.exception("supabase_find_business_categories failed")
        return []

async def supabase_find_discount_by_id(discount_id: str) -> Optional[Dict[str, Any]]:
    return await supabase_find_discount(discount_id)

async def supabase_find_giveaway(giveaway_id: str) -> Optional[Dict[str, Any]]:
    _ensure_supabase()
    def _q():
        return supabase.table("giveaways").select("*").eq("id", giveaway_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = getattr(resp, "data", None) or resp.get("data", []) if isinstance(resp, dict) else []
        return rows[0] if rows else None
    except Exception:
        logger.exception("supabase_find_giveaway failed")
        return None

async def has_redeemed_discount(chat_id: int) -> bool:
    _ensure_supabase()
    def _q():
        current_month = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return supabase.table("user_discounts").select("id").eq("telegram_id", chat_id).eq("entry_status", "standard").gte("joined_at", current_month.isoformat()).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = getattr(resp, "data", None) or resp.get("data", []) if isinstance(resp, dict) else []
        return bool(rows)
    except Exception:
        logger.exception("has_redeemed_discount failed")
        return False

# Promo code generation / saving
async def generate_discount_code(chat_id: int, business_id: str, discount_id: str) -> Tuple[str, str]:
    _ensure_supabase()
    if not business_id or not discount_id:
        raise ValueError("Business or discount ID missing")

    def _check_claimed():
        return supabase.table("user_discounts").select("id").eq("telegram_id", chat_id).eq("discount_id", discount_id).execute()

    claimed = await asyncio.to_thread(_check_claimed)
    claimed_rows = getattr(claimed, "data", None) or claimed.get("data", []) if isinstance(claimed, dict) else []
    if claimed_rows:
        raise ValueError("Already claimed")

    # generate unique code
    for _ in range(10):
        code = f"{random.randint(0, 9999):04d}"
        def _check_existing_code():
            return supabase.table("user_discounts").select("id").eq("promo_code", code).eq("business_id", business_id).execute()
        existing = await asyncio.to_thread(_check_existing_code)
        existing_rows = getattr(existing, "data", None) or existing.get("data", []) if isinstance(existing, dict) else []
        if not existing_rows:
            break
    else:
        raise RuntimeError("Unable to generate unique promo code")

    expiry = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
    payload = {
        "telegram_id": chat_id,
        "business_id": business_id,
        "discount_id": discount_id,
        "promo_code": code,
        "promo_expiry": expiry,
        "entry_status": "standard",
        "joined_at": now_iso(),
    }
    inserted = await supabase_insert_return("user_discounts", payload)
    if not inserted:
        raise RuntimeError("Failed to persist promo")

    # award points for claim (first time)
    try:
        user_row = await supabase_find_registered(chat_id)
        if user_row:
            if not await has_history(user_row["id"], "claim_promo"):
                await award_points(user_row["id"], POINTS_CLAIM_PROMO, "claim_promo")
    except Exception:
        logger.exception("generate_discount_code failed to award points")

    return code, expiry

async def generate_promo_code(chat_id: int, business_id: str, giveaway_id: str, discount_type: str = "standard") -> Tuple[str, str]:
    _ensure_supabase()
    if not business_id or not giveaway_id:
        raise ValueError("Business or giveaway ID missing")

    for _ in range(10):
        code = f"{random.randint(0, 9999):04d}"
        def _check_existing_code():
            return supabase.table("user_giveaways").select("id").eq("promo_code", code).eq("business_id", business_id).execute()
        existing = await asyncio.to_thread(_check_existing_code)
        existing_rows = getattr(existing, "data", None) or existing.get("data", []) if isinstance(existing, dict) else []
        if not existing_rows:
            break
    else:
        raise RuntimeError("Unable to generate unique promo code")

    expiry = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
    payload = {
        "telegram_id": chat_id,
        "business_id": business_id,
        "giveaway_id": giveaway_id,
        "promo_code": code,
        "promo_expiry": expiry,
        "entry_status": discount_type,
        "joined_at": now_iso(),
    }
    inserted = await supabase_insert_return("user_giveaways", payload)
    if not inserted:
        raise RuntimeError("Failed to persist giveaway promo")

    return code, expiry

# Points system
async def get_points_awarded_today(user_id: str) -> int:
    _ensure_supabase()
    def _q():
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        return supabase.table("points_history").select("points").eq("user_id", user_id).gte("awarded_at", today_start).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = getattr(resp, "data", None) or resp.get("data", []) if isinstance(resp, dict) else []
        return sum(int(r["points"]) for r in rows) if rows else 0
    except Exception:
        logger.exception("get_points_awarded_today failed")
        return 0

async def award_points(user_id: str, delta: int, reason: str, booking_id: Optional[str] = None) -> dict:
    if delta == 0:
        return {"ok": True, "message": "no-op"}
    _ensure_supabase()

    try:
        awarded_today = await get_points_awarded_today(user_id)
    except Exception:
        awarded_today = 0

    if awarded_today + abs(delta) > DAILY_POINTS_CAP:
        logger.warning("Daily cap reached for %s", user_id)
        return {"ok": False, "error": "daily_cap_reached"}

    def _get_user():
        return supabase.table("central_bot_leads").select("*").eq("id", user_id).limit(1).execute()

    try:
        resp = await asyncio.to_thread(_get_user)
        rows = getattr(resp, "data", None) or resp.get("data", []) if isinstance(resp, dict) else []
        if not rows:
            return {"ok": False, "error": "user_not_found"}
        user = rows[0]
    except Exception:
        logger.exception("award_points fetch user failed")
        return {"ok": False, "error": "fetch_failed"}

    old_points = int(user.get("points") or 0)
    new_points = max(0, old_points + delta)
    new_tier = compute_tier(new_points)

    def _upd_user():
        return supabase.table("central_bot_leads").update({"points": new_points, "tier": new_tier, "last_login": now_iso()}).eq("id", user_id).execute()

    try:
        await asyncio.to_thread(_upd_user)
    except Exception:
        logger.exception("award_points update failed")
        return {"ok": False, "error": "update_failed"}

    hist = {"user_id": user_id, "points": delta, "reason": reason, "awarded_at": now_iso()}
    await supabase_insert_return("points_history", hist)
    logger.info("Awarded %s pts to %s for %s", delta, user_id, reason)

    # referral bonus for booking verification
    if reason.startswith("booking_verified"):
        referred_by = user.get("referred_by")
        if referred_by:
            try:
                await award_points(referred_by, POINTS_REFERRAL_VERIFIED, "referral_booking_verified", booking_id)
            except Exception:
                logger.exception("award_points referral bonus failed")

    return {"ok": True, "old_points": old_points, "new_points": new_points, "tier": new_tier}

async def has_history(user_id: str, reason: str) -> bool:
    _ensure_supabase()
    def _q():
        return supabase.table("points_history").select("id").eq("user_id", user_id).eq("reason", reason).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = getattr(resp, "data", None) or resp.get("data", []) if isinstance(resp, dict) else []
        return bool(rows)
    except Exception:
        logger.exception("has_history failed")
        return False

# Bot meta helpers
_bot_username_cache: Optional[str] = None
_bot_username_lock = asyncio.Lock()

async def fetch_bot_username() -> Optional[str]:
    global _bot_username_cache
    if _bot_username_cache:
        return _bot_username_cache
    async with _bot_username_lock:
        if _bot_username_cache:
            return _bot_username_cache
        if not BOT_TOKEN:
            return None
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
                r = await client.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getMe")
                r.raise_for_status()
                data = r.json()
                username = data.get("result", {}).get("username")
                _bot_username_cache = username
                return username
        except Exception:
            logger.exception("fetch_bot_username failed")
            return None

async def get_referral_link_for_user_by_id(user_id: str) -> Optional[str]:
    """
    Build a referral link using the user's stored referral_code in central_bot_leads.
    Returns a t.me link like "https://t.me/<bot_username>?start=<ref_code>"
    """
    if not supabase:
        return None
    def _q():
        return supabase.table("central_bot_leads").select("referral_code").eq("id", user_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = getattr(resp, "data", None) or resp.get("data", []) if isinstance(resp, dict) else []
        if not rows:
            return None
        code = rows[0].get("referral_code")
        bot_user = await fetch_bot_username()
        if not bot_user or not code:
            return None
        return f"https://t.me/{bot_user}?start={code}"
    except Exception:
        logger.exception("get_referral_link_for_user_by_id failed")
        return None

# Convenience alias used by handlers that may call get_referral_link(...)
get_referral_link = get_referral_link_for_user_by_id

# Set menu button / commands
async def set_menu_button() -> None:
    if not BOT_TOKEN:
        logger.warning("set_menu_button: BOT_TOKEN not configured")
        return
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
            await client.post(f"https://api.telegram.org/bot{BOT_TOKEN}/setChatMenuButton", json={"menu_button": {"type": "commands"}})
            await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands",
                json={
                    "commands": [
                        {"command": "start", "description": "Start the bot"},
                        {"command": "menu", "description": "Open the menu"},
                        {"command": "myid", "description": "Get your Telegram ID"},
                        {"command": "approve", "description": "Approve a business (admin only)"},
                        {"command": "reject", "description": "Reject a business (admin only)"},
                    ]
                },
            )
            logger.info("set_menu_button: commands set")
    except Exception:
        logger.exception("set_menu_button failed")

# Expose public symbols expected by central_bot and handlers
__all__ = [
    "BOT_TOKEN",
    "SUPABASE_URL",
    "SUPABASE_KEY",
    "ADMIN_CHAT_ID",
    "WEBHOOK_URL",
    "logger",
    "supabase",
    "USER_STATES",
    "set_state",
    "get_state",
    "now_iso",
    "send_message",
    "edit_message_keyboard",
    "clear_inline_keyboard",
    "safe_clear_markup",
    "create_menu_options_keyboard",
    "create_language_keyboard",
    "create_gender_keyboard",
    "create_interests_keyboard",
    "create_main_menu_keyboard",
    "create_categories_keyboard",
    "create_phone_keyboard",
    "supabase_insert_return",
    "supabase_update_by_id_return",
    "supabase_find_draft",
    "supabase_find_registered",
    "supabase_find_business",
    "supabase_find_discount",
    "supabase_find_discounts_by_category",
    "supabase_find_business_categories",
    "supabase_find_discount_by_id",
    "supabase_find_giveaway",
    "has_redeemed_discount",
    "generate_discount_code",
    "generate_promo_code",
    "award_points",
    "get_points_awarded_today",
    "has_history",
    "compute_tier",
    "compute_tier_progress",
    "get_referral_link",
    "fetch_bot_username",
    "set_menu_button",
    "STARTER_POINTS",
    "POINTS_SIGNUP",
    "POINTS_PROFILE_COMPLETE",
    "POINTS_VIEW_DISCOUNT",
    "POINTS_CLAIM_PROMO",
    "POINTS_BOOKING_CREATED",
    "POINTS_BOOKING_VERIFIED",
    "POINTS_REFERRAL_JOIN",
    "POINTS_REFERRAL_VERIFIED",
    "DAILY_POINTS_CAP",
    "TIER_THRESHOLDS",
    "uuid",
]
