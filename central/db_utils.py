import os
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from supabase import create_client, Client

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY]):
    raise RuntimeError("Required env vars missing: SUPABASE_URL, SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# In-memory state
USER_STATES: Dict[int, Dict[str, Any]] = {}
STARTER_POINTS = 100
POINTS_SIGNUP = 20
POINTS_PROFILE_COMPLETE = 40
POINTS_VIEW_DISCOUNT = 5
POINTS_CLAIM_PROMO = 10
POINTS_BOOKING_CREATED = 15
POINTS_BOOKING_VERIFIED = 200
POINTS_REFERRAL_JOIN = 10
POINTS_REFERRAL_VERIFIED = 100
DAILY_POINTS_CAP = 2000
STATE_TTL_SECONDS = 30 * 60  # 30 minutes
TIER_THRESHOLDS = [
    ("Bronze", 0),
    ("Silver", 200),
    ("Gold", 500),
    ("Platinum", 1000),
]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def set_state(chat_id: int, state: Dict[str, Any]):
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

async def supabase_get_points_history(user_id: str, limit: int = 5) -> List[Dict[str, Any]]:
    def _q():
        return supabase.table("points_history").select("*").eq("user_id", user_id).order("awarded_at", desc=True).limit(limit).execute()
    try:
        resp = await asyncio.to_thread(_q)
        return resp.data if hasattr(resp, "data") else resp.get("data", []) or []
    except Exception as e:
        logger.error(f"supabase_get_points_history failed: {str(e)}", exc_info=True)
        return []

async def supabase_find_draft(chat_id: int) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("central_bot_leads").select("*").eq("telegram_id", chat_id).eq("is_draft", True).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_find_draft failed: {str(e)}", exc_info=True)
        return None

async def supabase_find_registered(chat_id: int) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("central_bot_leads").select("*").eq("telegram_id", chat_id).eq("is_draft", False).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_find_registered failed: {str(e)}", exc_info=True)
        return None

async def supabase_insert_return(table: str, payload: dict) -> Optional[Dict[str, Any]]:
    def _ins():
        return supabase.table(table).insert(payload).execute()
    try:
        resp = await asyncio.to_thread(_ins)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.error(f"Insert failed for {table}")
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_insert_return failed: {str(e)}", exc_info=True)
        return None

async def supabase_update_by_id_return(table: str, entry_id: str, payload: dict) -> Optional[Dict[str, Any]]:
    def _upd():
        return supabase.table(table).update(payload).eq("id", entry_id).execute()
    try:
        resp = await asyncio.to_thread(_upd)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            logger.error(f"Update failed for {table} id {entry_id}")
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_update_by_id_return failed: {str(e)}", exc_info=True)
        return None

async def supabase_find_business(business_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("businesses").select("*").eq("id", business_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_find_business failed: {str(e)}", exc_info=True)
        return None

async def supabase_find_discount(discount_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("discounts").select("*").eq("id", discount_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_find_discount failed: {str(e)}", exc_info=True)
        return None

async def supabase_find_discounts_by_category(category: str):
    def _q():
        return supabase.table("discounts") \
            .select("id, name, discount_percentage, category, business_id") \
            .eq("category", category) \
            .eq("active", True) \
            .execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = resp.data if hasattr(resp, "data") else resp.get("data", []) or []
        return rows
    except Exception:
        logger.exception("supabase_find_discounts_by_category failed")
        return []

async def supabase_find_business_categories(business_id: str):
    def _q():
        return supabase.table("business_categories") \
            .select("category") \
            .eq("business_id", business_id) \
            .execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = resp.data if hasattr(resp, "data") else resp.get("data", []) or []
        return [r["category"] for r in rows]
    except Exception:
        logger.exception("supabase_find_business_categories failed")
        return []

async def supabase_find_discount_by_id(discount_id: str):
    def _q():
        return supabase.table("discounts").select("*").eq("id", discount_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = resp.data if hasattr(resp, "data") else resp.get("data", []) or []
        return rows[0] if rows else None
    except Exception:
        logger.exception("supabase_find_discount_by_id failed")
        return None

async def supabase_find_giveaway(giveaway_id: str) -> Optional[Dict[str, Any]]:
    def _q():
        return supabase.table("giveaways").select("*").eq("id", giveaway_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        data = resp.data if hasattr(resp, "data") else resp.get("data")
        if not data:
            return None
        return data[0]
    except Exception as e:
        logger.error(f"supabase_find_giveaway failed: {str(e)}", exc_info=True)
        return None

def compute_tier(points: int) -> str:
    tier = "Bronze"
    for name, threshold in TIER_THRESHOLDS:
        if points >= threshold:
            tier = name
    return tier

async def compute_tier_progress(points: int) -> Dict[str, Any]:
    current_tier = compute_tier(points)
    next_tier = None
    next_threshold = None
    points_to_next = 0
    percent_to_next = 0
    for i, (name, threshold) in enumerate(TIER_THRESHOLDS):
        if name == current_tier and i < len(TIER_THRESHOLDS) - 1:
            next_tier, next_threshold = TIER_THRESHOLDS[i + 1]
            points_to_next = next_threshold - points
            if next_threshold > 0:
                percent_to_next = min(100, int((points / next_threshold) * 100))
            break
    return {
        "current_tier": current_tier,
        "next_tier": next_tier,
        "next_threshold": next_threshold,
        "points_to_next": points_to_next,
        "percent_to_next": percent_to_next
    }

async def get_points_awarded_today(user_id: str) -> int:
    def _q():
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        return supabase.table("points_history").select("points").eq("user_id", user_id).gte("awarded_at", today_start).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = resp.data if hasattr(resp, "data") else resp.get("data", []) or []
        return sum(int(r["points"]) for r in rows)
    except Exception:
        logger.exception("get_points_awarded_today failed")
        return 0

async def award_points(user_id: str, delta: int, reason: str, booking_id: Optional[str] = None) -> dict:
    if delta == 0:
        return {"ok": True, "message": "no-op"}

    awarded_today = await get_points_awarded_today(user_id)
    if awarded_today + abs(delta) > DAILY_POINTS_CAP:
        logger.warning(f"Daily cap reached for {user_id}")
        return {"ok": False, "error": "daily_cap_reached"}

    def _get_user():
        return supabase.table("central_bot_leads").select("*").eq("id", user_id).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_get_user)
        rows = resp.data if hasattr(resp, "data") else resp.get("data", []) or []
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
    logger.info(f"Awarded {delta} pts to {user_id} for {reason}")

    if reason == "booking_verified":
        referred_by = user.get("referred_by")
        if referred_by:
            try:
                await award_points(referred_by, POINTS_REFERRAL_VERIFIED, "referral_booking_verified", booking_id)
            except Exception:
                logger.exception("Failed referral bonus")

    return {"ok": True, "old_points": old_points, "new_points": new_points, "tier": new_tier}

async def has_history(user_id: str, reason: str) -> bool:
    def _q():
        return supabase.table("points_history").select("id").eq("user_id", user_id).eq("reason", reason).limit(1).execute()
    try:
        resp = await asyncio.to_thread(_q)
        rows = resp.data if hasattr(resp, "data") else resp.get("data", []) or []
        return bool(rows)
    except Exception:
        logger.exception("has_history failed")
        return False
