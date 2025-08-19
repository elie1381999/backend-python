from typing import Dict, Any, Optional
from uuid import uuid4
from central.db_utils import (
    supabase_insert_return,
    supabase_update_by_id_return,
    supabase_find_draft,
    supabase_find_registered,
    award_points,
    has_history,
    STARTER_POINTS,
    POINTS_REFERRAL_JOIN,
    POINTS_SIGNUP,
    get_state,
    set_state,
    logger,
    now_iso
)
from central.utils import (
    send_message,
    create_language_keyboard,
    create_gender_keyboard,
    create_interests_keyboard,
    create_main_menu_keyboard,
    create_menu_options_keyboard,
    safe_clear_markup,
    edit_message_keyboard
)

async def handle_start(message: Dict[str, Any], state: Dict[str, Any], chat_id: int) -> Dict[str, Any]:
    text = message.get("text", "")
    referral = text.split(" ", 1)[1] if " " in text else None

    registered = await supabase_find_registered(chat_id)
    if registered:
        await send_message(chat_id, "Welcome back! Choose an option:", reply_markup=create_menu_options_keyboard())
        return {"ok": True}

    draft = await supabase_find_draft(chat_id)
    if not draft:
        payload = {
            "telegram_id": chat_id,
            "is_draft": True,
            "points": STARTER_POINTS,
            "tier": "Bronze",
            "referral_code": uuid4().hex[:8],
            "created_at": now_iso(),
            "last_login": now_iso()
        }
        if referral:
            referrer_rows = supabase.table("central_bot_leads").select("id").eq("referral_code", referral).limit(1).execute().data
            if referrer_rows:
                referrer_id = referrer_rows[0]["id"]
                payload["referred_by"] = referrer_id
                if not await has_history(referrer_id, f"referral_join_{chat_id}"):
                    await award_points(referrer_id, POINTS_REFERRAL_JOIN, f"referral_join_{chat_id}")

        draft = await supabase_insert_return("central_bot_leads", payload)
        if not draft:
            await send_message(chat_id, "Error creating profile. Try again.")
            return {"ok": True}

    await send_message(chat_id, "Welcome! Select language:", reply_markup=create_language_keyboard())
    set_state(chat_id, {"stage": "awaiting_language", "draft_id": draft["id"]})
    return {"ok": True}

async def handle_menu(callback_data: str, chat_id: int, message_id: int, state: Dict[str, Any]) -> Dict[str, Any]:
    if callback_data == "menu:main":
        await send_message(chat_id, "Main menu:", reply_markup=create_main_menu_keyboard())
    elif callback_data == "menu:language":
        await send_message(chat_id, "Select language:", reply_markup=create_language_keyboard())
        set_state(chat_id, {"stage": "awaiting_language_change"})
    return {"ok": True}

async def handle_language_selection(callback_data: str, state: Dict[str, Any], chat_id: int, message_id: int) -> Dict[str, Any]:
    lang = callback_data.split(":")[1]
    stage = state.get("stage")

    if stage == "awaiting_language_change":
        registered = await supabase_find_registered(chat_id)
        if registered:
            await supabase_update_by_id_return("central_bot_leads", registered["id"], {"language": lang})
            await send_message(chat_id, f"Language updated to {lang}.", reply_markup=create_main_menu_keyboard())
            set_state(chat_id, {})
            return {"ok": True}
    elif stage == "awaiting_language":
        draft_id = state.get("draft_id")
        if draft_id:
            await supabase_update_by_id_return("central_bot_leads", draft_id, {"language": lang})
        await edit_message_keyboard(chat_id, message_id, create_gender_keyboard())
        set_state(chat_id, {"stage": "awaiting_gender", "draft_id": draft_id})
        return {"ok": True}

    return {"ok": True}

async def handle_gender_selection(callback_data: str, state: Dict[str, Any], chat_id: int, message_id: int) -> Dict[str, Any]:
    gender = callback_data.split(":")[1]
    draft_id = state.get("draft_id")
    if draft_id:
        await supabase_update_by_id_return("central_bot_leads", draft_id, {"gender": gender})
    await edit_message_keyboard(chat_id, message_id, create_interests_keyboard())
    set_state(chat_id, {"stage": "awaiting_interests", "draft_id": draft_id, "selected_interests": []})
    return {"ok": True}

async def handle_interests_selection(callback_data: str, state: Dict[str, Any], chat_id: int, message_id: int, registered: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if callback_data == "interests_done":
        selected = state.get("selected_interests", [])
        draft_id = state.get("draft_id")
        if draft_id:
            await supabase_update_by_id_return("central_bot_leads", draft_id, {"interests": selected, "is_draft": False})
            user_id = draft_id
            if not await has_history(user_id, "signup"):
                await award_points(user_id, POINTS_SIGNUP, "signup")
            await send_message(chat_id, "Registration complete!", reply_markup=create_main_menu_keyboard())
            set_state(chat_id, {})
        return {"ok": True}

    interest = callback_data.split(":")[1]
    selected = state.get("selected_interests", [])
    if interest in selected:
        selected.remove(interest)
    else:
        if len(selected) < 3:
            selected.append(interest)
    set_state(chat_id, {**state, "selected_interests": selected})
    await edit_message_keyboard(chat_id, message_id, create_interests_keyboard(selected))
    return {"ok": True}
