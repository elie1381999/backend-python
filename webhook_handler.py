from fastapi import Request, HTTPException
from collections import defaultdict
import json
from datetime import datetime
from typing import Dict, Any

from supabase_client import (
    get_salon_by_bot_username,
    get_salon_by_webhook_id,
    get_telegram_token_by_bot_username,
    get_services_for_salon,
    log_message_to_db,
    send_telegram_message,
    create_giveaway_entry,
    update_giveaway_entry,
)

# Simple in-memory user state storage: chat_id -> state dict
USER_STATES: Dict[int, Dict[str, Any]] = defaultdict(lambda: {"stage": None, "data": {}})


async def handle_webhook_by_username(request: Request, bot_username: str):
    update = await request.json()
    return await _process_update(update, bot_username)


async def handle_webhook_by_webhook_id(request: Request, webhook_id: str):
    salon = await get_salon_by_webhook_id(webhook_id)
    if not salon:
        raise HTTPException(status_code=404, detail="Unknown webhook_id")

    bot_username = salon["telegram_bot_username"]
    update = await request.json()
    return await _process_update(update, bot_username)


async def _process_update(update: dict, bot_username: str):
    print(f"\n🔔 Update received for bot: {bot_username}")
    print(json.dumps(update, indent=2))  # Debug log

    # 1) Find the salon by bot username
    salon = await get_salon_by_bot_username(bot_username)
    if not salon:
        raise HTTPException(status_code=404, detail="Salon not found")

    # 2) Get Telegram bot token for API calls
    token = await get_telegram_token_by_bot_username(bot_username)
    if not token:
        raise HTTPException(status_code=500, detail="Telegram bot token not found")

    # 3) Process incoming Telegram message update
    msg = update.get("message")
    if not msg:
        # Ignore non-message updates (like inline queries, callbacks)
        return {"ok": True}

    chat_id = msg["chat"]["id"]
    text = msg.get("text", "").strip()
    user = msg.get("from", {}).get("username", "unknown")

    # Log the incoming message asynchronously, errors do not block
    try:
        await log_message_to_db(user, text)
    except Exception as exc:
        print(f"[WARN] log_message_to_db failed: {exc}")

    user_state = USER_STATES[chat_id]

    # === Giveaway Flow ===

    # Start giveaway entry flow on /giveaway command, only if no current stage
    if text.lower() == "/giveaway" and user_state.get("stage") is None:
        user_state["stage"] = "awaiting_name"
        return await send_telegram_message(
            token,
            chat_id,
            "🎁 Welcome to our giveaway!\nFirst, please reply with your *full name*:",
        )

    # Awaiting full name stage
    if user_state.get("stage") == "awaiting_name":
        name = text.strip()
        if not name:
            return await send_telegram_message(token, chat_id, "Please send your full name to continue.")

        user_state["data"]["name"] = name

        try:
            created = await create_giveaway_entry({
                "name": name,
                "telegram_username": user,
                "telegram_chat_id": chat_id,
                "status": "pending",
                "is_draft": True,
            })
        except Exception as exc:
            print(f"[ERROR] DB create error: {exc}")
            return await send_telegram_message(token, chat_id, "❌ Sorry, we couldn't save your entry right now. Try again later.")

        if created and created.get("id"):
            user_state["entry_id"] = created["id"]

        user_state["stage"] = "awaiting_dob"
        return await send_telegram_message(token, chat_id, "✅ Got it.\nNow please send your *date of birth* (YYYY-MM-DD):")

    # Awaiting date of birth stage
    if user_state.get("stage") == "awaiting_dob":
        try:
            dob_obj = datetime.strptime(text.strip(), "%Y-%m-%d").date()
        except ValueError:
            return await send_telegram_message(token, chat_id, "❌ Date format invalid. Please send DOB as YYYY-MM-DD (e.g. 1990-05-30).")

        dob_iso = dob_obj.isoformat()
        user_state["data"]["dob"] = dob_iso

        if user_state.get("entry_id"):
            try:
                await update_giveaway_entry(user_state["entry_id"], {"dob": dob_iso})
            except Exception as exc:
                print(f"[ERROR] DB patch error (dob): {exc}")
                return await send_telegram_message(token, chat_id, "❌ Could not save DOB right now. Try again later.")

        user_state["stage"] = "awaiting_profile"
        return await send_telegram_message(token, chat_id, "📎 Great.\nNext, send me the *link to your profile* so we can verify your story:")

    # Awaiting profile link stage
    if user_state.get("stage") == "awaiting_profile":
        profile_link = text
        user_state["data"]["profile_link"] = profile_link

        if user_state.get("entry_id"):
            try:
                await update_giveaway_entry(user_state["entry_id"], {"profile_link": profile_link})
            except Exception as exc:
                print(f"[WARN] DB patch error (profile_link): {exc}")

        user_state["stage"] = "awaiting_confirmation"
        return await send_telegram_message(
            token,
            chat_id,
            "✅ Thanks. Your profile link is recorded.\nReply YES to confirm your entry or NO to cancel.",
        )

    # Confirmation stage
    if user_state.get("stage") == "awaiting_confirmation":
        if text.lower() in ("yes", "y"):
            if user_state.get("entry_id"):
                try:
                    await update_giveaway_entry(user_state["entry_id"], {"status": "pending", "is_draft": False})
                except Exception as exc:
                    print(f"[WARN] DB patch error (status update): {exc}")
            USER_STATES.pop(chat_id, None)
            return await send_telegram_message(token, chat_id, "🎉 Your entry has been recorded. Winners will be announced soon.")

        elif text.lower() in ("no", "n"):
            if user_state.get("entry_id"):
                try:
                    await update_giveaway_entry(user_state["entry_id"], {"status": "loser", "is_draft": False})
                except Exception as exc:
                    print(f"[WARN] DB patch error (cancel): {exc}")
            USER_STATES.pop(chat_id, None)
            return await send_telegram_message(token, chat_id, "❌ Your entry was cancelled. To try again send /giveaway.")

        else:
            return await send_telegram_message(token, chat_id, "Please reply YES to confirm or NO to cancel your entry.")

    # === /services command ===
    if text.lower() == "/services":
        services = await get_services_for_salon(salon["id"])
        if not services:
            reply = "❌ No active services right now."
        else:
            lines = [f"💇 {s['variant_name']} — ${s['price']} ({s['duration']} min)" for s in services]
            reply = "💈 Our services:\n" + "\n".join(lines)
        return await send_telegram_message(token, chat_id, reply)

    # === Default fallback message ===
    fallback = (
        "👋 Hi! You can send:\n"
        "- /services to see our menu\n"
        "- /giveaway to join the giveaway"
    )
    return await send_telegram_message(token, chat_id, fallback)
