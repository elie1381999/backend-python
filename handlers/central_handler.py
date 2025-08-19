from typing import Dict, Any
from utils import supabase_insert_return, supabase_update_by_id_return, supabase_find_draft, supabase_find_registered, send_message, create_language_keyboard, create_gender_keyboard, create_interests_keyboard, create_main_menu_keyboard, get_state, set_state, award_points, has_history, STARTER_POINTS, POINTS_REFERRAL_JOIN, logger, safe_clear_markup, edit_message_keyboard, uuid

async def handle_start(message: Dict[str, Any], state: Dict[str, Any], chat_id: int):
    text = message.get("text", "").lower()
    if text != "/start":
        referred_by = text[len("/start "):]
        try:
            uuid.UUID(referred_by)
            state["referred_by"] = referred_by
        except ValueError:
            logger.error(f"Invalid referral: {referred_by}")
    registered = await supabase_find_registered(chat_id)
    if registered:
        await send_message(chat_id, "You're already registered! Explore options:", reply_markup=create_main_menu_keyboard())
        return {"ok": True}
    existing = await supabase_find_draft(chat_id)
    if existing:
        state = {
            "stage": "awaiting_gender",
            "data": {"language": existing.get("language")},
            "entry_id": existing.get("id"),
            "selected_interests": []
        }
        await send_message(chat_id, "What's your gender? (optional, helps target offers)", reply_markup=create_gender_keyboard())
    else:
        state = {"stage": "awaiting_language", "data": {}, "entry_id": None, "selected_interests": []}
        await send_message(chat_id, "Welcome! Choose your language:", reply_markup=create_language_keyboard())
    set_state(chat_id, state)
    return {"ok": True}

async def handle_menu(callback_data: str, chat_id: int, message_id: int, state: Dict[str, Any]):
    await safe_clear_markup(chat_id, message_id)
    if callback_data == "menu:main":
        await send_message(chat_id, "Explore options:", reply_markup=create_main_menu_keyboard())
    elif callback_data == "menu:language":
        await send_message(chat_id, "Choose your language:", reply_markup=create_language_keyboard())
        state["stage"] = "awaiting_language_change"
        set_state(chat_id, state)
    return {"ok": True}

async def handle_language_selection(callback_data: str, state: Dict[str, Any], chat_id: int, message_id: int):
    language = callback_data[len("lang:"):]
    if language not in ["en", "ru"]:
        await send_message(chat_id, "Invalid language:", reply_markup=create_language_keyboard())
        return {"ok": True}
    state["data"]["language"] = language
    entry_id = state.get("entry_id")
    if not entry_id:
        created = await supabase_insert_return("central_bot_leads", {"telegram_id": chat_id, "language": language, "is_draft": True})
        state["entry_id"] = created.get("id") if created else None
    else:
        await supabase_update_by_id_return("central_bot_leads", entry_id, {"language": language})
    await safe_clear_markup(chat_id, message_id)
    if state.get("stage") == "awaiting_language":
        await send_message(chat_id, "What's your gender? (optional, helps target offers)", reply_markup=create_gender_keyboard())
        state["stage"] = "awaiting_gender"
    else:
        await send_message(chat_id, "Language updated! Explore options:", reply_markup=create_main_menu_keyboard())
        del USER_STATES[chat_id]  # Assuming USER_STATES imported or global
    set_state(chat_id, state)
    return {"ok": True}

async def handle_gender_selection(callback_data: str, state: Dict[str, Any], chat_id: int, message_id: int):
    gender = callback_data[len("gender:"):]
    if gender not in ["female", "male"]:
        await send_message(chat_id, "Invalid gender:", reply_markup=create_gender_keyboard())
        return {"ok": True}
    state["data"]["gender"] = gender
    entry_id = state.get("entry_id")
    if entry_id:
        await supabase_update_by_id_return("central_bot_leads", entry_id, {"gender": gender})
    await safe_clear_markup(chat_id, message_id)
    await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:")
    state["stage"] = "awaiting_dob"
    set_state(chat_id, state)
    return {"ok": True}

async def handle_interests_selection(callback_data: str, state: Dict[str, Any], chat_id: int, message_id: int, registered):
    if callback_data.startswith("interest:"):
        interest = callback_data[len("interest:"):]
        if interest not in INTERESTS:
            return {"ok": True}
        selected = state.get("selected_interests", [])
        if interest in selected:
            selected.remove(interest)
        elif len(selected) < 3:
            selected.append(interest)
        state["selected_interests"] = selected
        await edit_message_keyboard(chat_id, message_id, create_interests_keyboard(selected))
        set_state(chat_id, state)
        return {"ok": True}
    elif callback_data == "interests_done":
        selected = state.get("selected_interests", [])
        if len(selected) != 3:
            await send_message(chat_id, f"Please select exactly 3 interests (currently {len(selected)}):", reply_markup=create_interests_keyboard(selected))
            return {"ok": True}
        await send_message(chat_id, "Interests saved! Finalizing registration...")
        entry_id = state.get("entry_id")
        if entry_id:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {
                "interests": selected,
                "is_draft": False
            })
            try:
                if not await has_history(entry_id, "signup"):
                    await award_points(entry_id, STARTER_POINTS, "signup")
                referred = state.get("referred_by")
                if referred:
                    try:
                        ref_uuid = str(uuid.UUID(referred))
                        await supabase_update_by_id_return("central_bot_leads", entry_id, {"referred_by": ref_uuid})
                        if not await has_history(ref_uuid, "referral_join"):
                            await award_points(ref_uuid, POINTS_REFERRAL_JOIN, "referral_join")
                    except Exception:
                        logger.debug("Skipping referral join")
            except Exception:
                logger.exception("Failed signup/referral points")
        await send_message(chat_id, f"Congrats! You've earned {STARTER_POINTS} points. Explore options:", reply_markup=create_main_menu_keyboard())
        del USER_STATES[chat_id]
        return {"ok": True}
