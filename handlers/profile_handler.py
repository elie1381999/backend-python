
from typing import Dict, Any
from datetime import datetime
from utils import send_message, create_phone_keyboard, supabase_update_by_id_return, supabase_find_registered, award_points, has_history, POINTS_PROFILE_COMPLETE, create_main_menu_keyboard, get_state, set_state, EMOJIS, logger, USER_STATES
async def handle_profile(callback_query: Dict[str, Any], registered: Dict[str, Any], state: Dict[str, Any], chat_id: int):
    if not registered.get("phone_number"):
        await send_message(chat_id, "Please share your phone number to complete your profile:", reply_markup=create_phone_keyboard())
        state["stage"] = "awaiting_phone_profile"
        state["data"] = registered
        state["entry_id"] = registered["id"]
        set_state(chat_id, state)
        return {"ok": True}
    if not registered.get("dob"):
        await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:")
        state["stage"] = "awaiting_dob_profile"
        state["data"] = registered
        state["entry_id"] = registered["id"]
        set_state(chat_id, state)
        return {"ok": True}
    interests = registered.get("interests", []) or []
    interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
    await send_message(chat_id, f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}")
    return {"ok": True}

async def handle_phone_contact(contact: Dict[str, Any], state: Dict[str, Any], chat_id: int):
    phone_number = contact.get("phone_number")
    if not phone_number:
        await send_message(chat_id, "Invalid phone number. Please try again:", reply_markup=create_phone_keyboard())
        return {"ok": True}
    state["data"]["phone_number"] = phone_number
    entry_id = state.get("entry_id")
    if entry_id:
        await supabase_update_by_id_return("central_bot_leads", entry_id, {"phone_number": phone_number})
    registered = await supabase_find_registered(chat_id)
    try:
        if registered:
            user_row = await supabase_find_registered(chat_id)
            if user_row and user_row.get("dob") and user_row.get("phone_number"):
                user_id = user_row["id"]
                if not await has_history(user_id, "profile_complete"):
                    await award_points(user_id, POINTS_PROFILE_COMPLETE, "profile_complete")
    except Exception:
        logger.exception("Failed profile complete points")

    if registered and not registered.get("dob"):
        await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:")
        state["stage"] = "awaiting_dob_profile"
    else:
        interests = registered.get("interests", []) or []
        interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
        await send_message(chat_id, f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}")
        del USER_STATES[chat_id]
    set_state(chat_id, state)
    return {"ok": True}

async def handle_dob_input(text: str, state: Dict[str, Any], chat_id: int):
    stage = state.get("stage")
    if text.lower() == "/skip":
        state["data"]["dob"] = None
        entry_id = state.get("entry_id")
        if entry_id:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": None})
        if stage == "awaiting_dob":
            state["stage"] = "awaiting_interests"
            await send_message(chat_id, "Choose exactly 3 interests for this month:", reply_markup=create_interests_keyboard())
            set_state(chat_id, state)
            return {"ok": True}
        else:
            registered = await supabase_find_registered(chat_id)
            interests = registered.get("interests", []) or []
            interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
            await send_message(chat_id, f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}")
            del USER_STATES[chat_id]
            return {"ok": True}
    try:
        dob_obj = datetime.strptime(text, "%Y-%m-%d").date()
        if dob_obj.year < 1900 or dob_obj > datetime.now().date():
            await send_message(chat_id, "Invalid date. Use YYYY-MM-DD or /skip.")
            return {"ok": True}
        state["data"]["dob"] = dob_obj.isoformat()
        entry_id = state.get("entry_id")
        if entry_id:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": state["data"]["dob"]})
        if stage == "awaiting_dob":
            state["stage"] = "awaiting_interests"
            await send_message(chat_id, "Choose exactly 3 interests for this month:", reply_markup=create_interests_keyboard())
            set_state(chat_id, state)
            return {"ok": True}
        else:
            registered = await supabase_find_registered(chat_id)
            try:
                if registered and registered.get("phone_number") and registered.get("dob"):
                    if not await has_history(registered["id"], "profile_complete"):
                        await award_points(registered["id"], POINTS_PROFILE_COMPLETE, "profile_complete")
            except Exception:
                logger.exception("Failed profile_complete after dob")
            interests = registered.get("interests", []) or []
            interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
            await send_message(chat_id, f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}")
            del USER_STATES[chat_id]
            return {"ok": True}
    except ValueError:
        await send_message(chat_id, "Invalid date. Use YYYY-MM-DD or /skip.")
    return {"ok": True}
