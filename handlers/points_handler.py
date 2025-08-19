# handlers/points_handler.py
from typing import Any, Dict
import datetime
from central.utils import (
    send_message,
    supabase_find_registered,
    get_referral_link,
    compute_tier_progress,
    supabase_get_points_history,
    compute_tier,
    logger,
)

async def handle_points(callback_query: dict[str, Any], registered: dict[str, Any]):
    """
    Shows a Points summary:
     - total points
     - current tier
     - progress to next tier
     - referral link (deep link) with inline button to share
     - recent points activity (last 5)
    """
    chat_id = callback_query.get("from", {}).get("id")
    if not chat_id:
        logger.error("handle_points: no chat_id in callback_query")
        return {"ok": True}

    # Ensure we have a registered row (if caller passed none)
    if not registered:
        registered = await supabase_find_registered(chat_id)
        if not registered:
            await send_message(chat_id, "You are not registered yet. Use /start to register.")
            return {"ok": True}

    points = int(registered.get("points", 0) or 0)
    tier = registered.get("tier") or compute_tier(points)
    referral_code = registered.get("referral_code") or ""
    # referral link
    referral_link = await get_referral_link(referral_code) if referral_code else ""

    # Tier progress
    progress = compute_tier_progress(points)
    if progress["next_tier"] is None:
        progress_line = f"You are at the top tier: *{progress['current_tier']}* (no further tiers)."
    else:
        progress_line = (
            f"Tier: *{progress['current_tier']}* → Next: *{progress['next_tier']}*\n"
            f"Progress: *{points}* / *{progress['next_threshold']}*  ({progress['percent_to_next']}%); "
            f"{progress['points_to_next']} pts to *{progress['next_tier']}*"
        )

    # recent history
    try:
        history_rows = await supabase_get_points_history(registered["id"], limit=5)
    except Exception:
        logger.exception("Failed fetching points history")
        history_rows = []

    if history_rows:
        lines = []
        for r in history_rows:
            # r may include awarded_at as ISO string
            ts = r.get("awarded_at")
            when = ts.split("T")[0] if isinstance(ts, str) else str(ts)
            lines.append(f"{when} — {r.get('points')} pts — {r.get('reason')}")
        history_text = "\n".join(lines)
    else:
        history_text = "No recent activity."

    msg = (
        f"*Your Points Summary*\n\n"
        f"*Total points:* {points}\n"
        f"*Current tier:* {tier}\n\n"
        f"{progress_line}\n\n"
        f"*Referral code:* `{referral_code}`\n"
        f"*Referral link:* {referral_link or 'Not available'}\n\n"
        f"*Recent activity:*\n{history_text}\n\n"
        f"Tip: share your referral link to invite friends and earn referral bonuses."
    )

    # Build keyboard: share referral (url) + back to menu
    buttons = []
    if referral_link:
        buttons.append([{"text": "Share referral", "url": referral_link}])
    buttons.append([{"text": "Main menu", "callback_data": "menu:main"}])
    keyboard = {"inline_keyboard": buttons}

    await send_message(chat_id, msg, reply_markup=keyboard)
    return {"ok": True}
