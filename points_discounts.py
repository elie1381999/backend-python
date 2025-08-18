import os
import json
import logging
import asyncio
from typing import Dict, Any, Optional
from fastapi import FastAPI, Request
from starlette.responses import PlainTextResponse, Response
from dotenv import load_dotenv
from points_discounts import handle_points_and_discounts
from utils import (
    create_client, now_iso, set_state, get_state, send_message, safe_clear_markup,
    create_menu_options_keyboard, create_language_keyboard, create_gender_keyboard,
    create_main_menu_keyboard, create_interests_keyboard, create_phone_keyboard,
    supabase_find_draft, supabase_find_registered, supabase_insert_return,
    supabase_update_by_id_return, supabase_find_business, initialize_bot,
    supabase_insert_feedback, INTERESTS, EMOJIS, STATE_TTL_SECONDS, ADMIN_CHAT_ID, BOT_TOKEN
)

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="Multi-Business Telegram Bot")

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

if not all([BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY, ADMIN_CHAT_ID, WEBHOOK_URL]):
    raise RuntimeError("Required environment variables not set")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# In-memory state
USER_STATES: Dict konseDict[int, Dict[str, Any]] = {}

@app.post("/hook/central_bot")
async def webhook_handler(request: Request) -> Response:
    try:
        update = await request.json()
        if not update:
            logger.error("Received empty update from Telegram")
            return Response(status_code=200)
        await initialize_bot()
        message = update.get("message")
        if message:
            await handle_message_update(message)
        callback_query = update.get("callback_query")
        if callback_query:
            await handle_callback_query(callback_query)
        return Response(status_code=200)
    except json.JSONDecodeError:
        logger.error("Invalid JSON in webhook", exc_info=True)
        return Response(status_code=200)
    except Exception as e:
        logger.error(f"Error processing webhook update: {str(e)}", exc_info=True)
        return Response(status_code=200)

@app.get("/health")
async def health() -> PlainTextResponse:
    return PlainTextResponse("OK", status_code=200)

async def handle_message_update(message: Dict[str, Any]):
    chat_id = message.get("chat", {}).get("id")
    if not chat_id:
        logger.error("No chat_id in message")
        return {"ok": True}
    text = (message.get("text") or "").strip()
    contact = message.get("contact")
    state = get_state(chat_id) or {}

    # Handle /myid
    if text.lower() == "/myid":
        await send_message(chat_id, f"Your Telegram ID: {chat_id}")
        return {"ok": True}

    # Handle admin commands for business approval/rejection
    if chat_id == int(ADMIN_CHAT_ID) and text.startswith("/approve_"):
        business_id = text[len("/approve_"):]
        try:
            business = await supabase_find_business(business_id, supabase)
            if not business:
                await send_message(chat_id, f"Business with ID {business_id} not found.")
                return {"ok": True}
            await supabase_update_by_id_return("businesses", business_id, {"status": "approved", "updated_at": now_iso()}, supabase)
            await send_message(chat_id, f"Business {business['name']} approved.")
            await send_message(business["telegram_id"], "Your business has been approved! You can now add discounts and giveaways.")
        except ValueError:
            await send_message(chat_id, f"Invalid business ID format: {business_id}")
        except Exception as e:
            logger.error(f"Failed to approve business {business_id}: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to approve business. Please try again.")
        return {"ok": True}

    if chat_id == int(ADMIN_CHAT_ID) and text.startswith("/reject_"):
        business_id = text[len("/reject_"):]
        try:
            business = await supabase_find_business(business_id, supabase)
            if not business:
                await send_message(chat_id, f"Business with ID {business_id} not found.")
                return {"ok": True}
            await supabase_update_by_id_return("businesses", business_id, {"status": "rejected", "updated_at": now_iso()}, supabase)
            await send_message(chat_id, f"Business {business['name']} rejected.")
            await send_message(business["telegram_id"], "Your business registration was rejected. Please contact support.")
        except ValueError:
            await send_message(chat_id, f"Invalid business ID format: {business_id}")
        except Exception as e:
            logger.error(f"Failed to reject business {business_id}: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to reject business. Please try again.")
        return {"ok": True}

    # Handle /menu
    if text.lower() == "/menu":
        await send_message(chat_id, "Choose an option:", reply_markup=create_menu_options_keyboard())
        return {"ok": True}

    # Handle phone number
    if contact and state.get("stage") == "awaiting_phone_profile":
        phone_number = contact.get("phone_number")
        if not phone_number:
            await send_message(chat_id, "Invalid phone number. Please try again:", reply_markup=create_phone_keyboard())
            return {"ok": True}
        state["data"]["phone_number"] = phone_number
        entry_id = state.get("entry_id")
        if entry_id:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"phone_number": phone_number}, supabase)
        registered = await supabase_find_registered(chat_id, supabase)
        if registered and not registered.get("dob"):
            await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:")
            state["stage"] = "awaiting_dob_profile"
        else:
            interests = registered.get("interests", []) or []
            interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
            await send_message(chat_id, f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}")
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
        set_state(chat_id, state)
        return {"ok": True}

    # Handle DOB (initial registration or profile update)
    if state.get("stage") in ["awaiting_dob", "awaiting_dob_profile"]:
        if text.lower() == "/skip":
            state["data"]["dob"] = None
            entry_id = state.get("entry_id")
            if entry_id:
                await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": None}, supabase)
            if state.get("stage") == "awaiting_dob":
                state["stage"] = "awaiting_interests"
                await send_message(chat_id, "Choose exactly 3 interests for this month:", reply_markup=create_interests_keyboard())
            else:
                registered = await supabase_find_registered(chat_id, supabase)
                interests = registered.get("interests", []) or []
                interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
                await send_message(chat_id, f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}")
                if chat_id in USER_STATES:
                    del USER_STATES[chat_id]
            set_state(chat_id, state)
            return {"ok": True}
        try:
            dob_obj = datetime.strptime(text, "%Y-%m-%d").date()
            if dob_obj.year < 1900 or dob_obj > datetime.now().date():
                await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.")
                return {"ok": True}
            state["data"]["dob"] = dob_obj.isoformat()
            entry_id = state.get("entry_id")
            if entry_id:
                await supabase_update_by_id_return("central_bot_leads", entry_id, {"dob": state["data"]["dob"]}, supabase)
            if state.get("stage") == "awaiting_dob":
                state["stage"] = "awaiting_interests"
                await send_message(chat_id, "Choose exactly 3 interests for this month:", reply_markup=create_interests_keyboard())
            else:
                registered = await supabase_find_registered(chat_id, supabase)
                interests = registered.get("interests", []) or []
                interests_text = ", ".join(f"{EMOJIS[i]} {interest}" for i, interest in enumerate(interests)) if interests else "Not set"
                await send_message(chat_id, f"Profile:\nPhone: {registered.get('phone_number', 'Not set')}\nDOB: {registered.get('dob', 'Not set')}\nGender: {registered.get('gender', 'Not set')}\nYour interests for this month are: {interests_text}")
                if chat_id in USER_STATES:
                    del USER_STATES[chat_id]
            set_state(chat_id, state)
        except ValueError:
            await send_message(chat_id, "Invalid date. Use YYYY-MM-DD (e.g., 1995-06-22) or /skip.")
        return {"ok": True}

    # Handle /start
    if text.lower().startswith("/start"):
        if text.lower() != "/start":
            business_id = text[len("/start "):]
            try:
                state["referred_by"] = business_id
            except ValueError:
                logger.error(f"Invalid referral business_id: {business_id}")
        registered = await supabase_find_registered(chat_id, supabase)
        if registered:
            await send_message(chat_id, "You're already registered! Explore options:", reply_markup=create_main_menu_keyboard())
            return {"ok": True}
        existing = await supabase_find_draft(chat_id, supabase)
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

    # Handle /feedback
    if text.lower().startswith("/feedback"):
        registered = await supabase_find_registered(chat_id, supabase)
        if not registered:
            await send_message(chat_id, "Please register first using /start.")
            return {"ok": True}
        if text.lower() == "/feedback":
            await send_message(chat_id, "Please provide the business ID for your feedback (e.g., /feedback <business_id>).")
            return {"ok": True}
        business_id = text[len("/feedback "):].strip()
        try:
            uuid.UUID(business_id)
            business = await supabase_find_business(business_id, supabase)
            if not business:
                await send_message(chat_id, f"Business with ID {business_id} not found.")
                return {"ok": True}
            state = {
                "stage": "awaiting_feedback_rating",
                "data": {"business_id": business_id},
                "entry_id": None,
                "selected_interests": []
            }
            await send_message(chat_id, "Please enter a rating (1–5):")
            set_state(chat_id, state)
        except ValueError:
            await send_message(chat_id, f"Invalid business ID format: {business_id}")
        return {"ok": True}

    # Handle feedback rating
    if state.get("stage") == "awaiting_feedback_rating":
        try:
            rating = int(text)
            if rating < 1 or rating > 5:
                await send_message(chat_id, "Rating must be between 1 and 5. Please try again:")
                return {"ok": True}
            state["data"]["rating"] = rating
            state["stage"] = "awaiting_feedback_comment"
            await send_message(chat_id, "Please enter a comment (or /skip to submit without a comment):")
            set_state(chat_id, state)
        except ValueError:
            await send_message(chat_id, "Invalid rating. Please enter a number between 1 and 5:")
        return {"ok": True}

    # Handle feedback comment
    if state.get("stage") == "awaiting_feedback_comment":
        comment = None if text.lower() == "/skip" else text
        business_id = state["data"]["business_id"]
        rating = state["data"]["rating"]
        feedback = await supabase_insert_feedback(chat_id, business_id, rating, comment, supabase)
        if feedback:
            await send_message(chat_id, "Thank you for your feedback!", reply_markup=create_main_menu_keyboard())
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
        else:
            await send_message(chat_id, "Failed to save feedback. Please try again later.")
        return {"ok": True}

    return {"ok": True}

async def handle_callback_query(callback_query: Dict[str, Any]):
    chat_id = callback_query.get("from", {}).get("id")
    callback_data = callback_query.get("data")
    message_id = callback_query.get("message", {}).get("message_id")
    if not chat_id or not callback_data or not message_id:
        logger.error(f"Invalid callback query: chat_id={chat_id}, callback_data={callback_data}, message_id={message_id}")
        return {"ok": True}

    registered = await supabase_find_registered(chat_id, supabase)
    state = get_state(chat_id) or {}

    # Handle admin approval/rejection for businesses
    if chat_id == int(ADMIN_CHAT_ID) and callback_data.startswith("approve:"):
        business_id = callback_data[len("approve:"):]
        try:
            business = await supabase_find_business(business_id, supabase)
            if not business:
                await send_message(chat_id, f"Business with ID {business_id} not found.")
                await safe_clear_markup(chat_id, message_id)
                return {"ok": True}
            await supabase_update_by_id_return("businesses", business_id, {"status": "approved", "updated_at": now_iso()}, supabase)
            await send_message(chat_id, f"Business {business['name']} approved.")
            await send_message(business["telegram_id"], "Your business has been approved! You can now add discounts and giveaways.")
            await safe_clear_markup(chat_id, message_id)
        except ValueError:
            await send_message(chat_id, f"Invalid business ID format: {business_id}")
        except Exception as e:
            logger.error(f"Failed to approve business {business_id}: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to approve business. Please try again.")
        return {"ok": True}

    if chat_id == int(ADMIN_CHAT_ID) and callback_data.startswith("reject:"):
        business_id = callback_data[len("reject:"):]
        try:
            business = await supabase_find_business(business_id, supabase)
            if not business:
                await send_message(chat_id, f"Business with ID {business_id} not found.")
                await safe_clear_markup(chat_id, message_id)
                return {"ok": True}
            await supabase_update_by_id_return("businesses", business_id, {"status": "rejected", "updated_at": now_iso()}, supabase)
            await send_message(chat_id, f"Business {business['name']} rejected.")
            await send_message(business["telegram_id"], "Your business registration was rejected. Please contact support.")
            await safe_clear_markup(chat_id, message_id)
        except ValueError:
            await send_message(chat_id, f"Invalid business ID format: {business_id}")
        except Exception as e:
            logger.error(f"Failed to reject business {business_id}: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to reject business. Please try again.")
        return {"ok": True}

    # Menu options
    if callback_data == "menu:main":
        await safe_clear_markup(chat_id, message_id)
        await send_message(chat_id, "Explore options:", reply_markup=create_main_menu_keyboard())
        return {"ok": True}
    elif callback_data == "menu:language":
        await safe_clear_markup(chat_id, message_id)
        await send_message(chat_id, "Choose your language:", reply_markup=create_language_keyboard())
        state["stage"] = "awaiting_language_change"
        set_state(chat_id, state)
        return {"ok": True}

    # Language selection
    if state.get("stage") in ["awaiting_language", "awaiting_language_change"] and callback_data.startswith("lang:"):
        language = callback_data[len("lang:"):]
        if language not in ["en", "ru"]:
            await send_message(chat_id, "Invalid language:", reply_markup=create_language_keyboard())
            return {"ok": True}
        state["data"]["language"] = language
        entry_id = state.get("entry_id")
        if not entry_id:
            created = await supabase_insert_return("central_bot_leads", {"telegram_id": chat_id, "language": language, "is_draft": True}, supabase)
            state["entry_id"] = created.get("id") if created else None
        else:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"language": language}, supabase)
        await safe_clear_markup(chat_id, message_id)
        if state.get("stage") == "awaiting_language":
            await send_message(chat_id, "What's your gender? (optional, helps target offers)", reply_markup=create_gender_keyboard())
            state["stage"] = "awaiting_gender"
        else:
            await send_message(chat_id, "Language updated! Explore options:", reply_markup=create_main_menu_keyboard())
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
        set_state(chat_id, state)
        return {"ok": True}

    # Gender selection
    if state.get("stage") == "awaiting_gender" and callback_data.startswith("gender:"):
        gender = callback_data[len("gender:"):]
        if gender not in ["female", "male"]:
            await send_message(chat_id, "Invalid gender:", reply_markup=create_gender_keyboard())
            return {"ok": True}
        state["data"]["gender"] = gender
        entry_id = state.get("entry_id")
        if entry_id:
            await supabase_update_by_id_return("central_bot_leads", entry_id, {"gender": gender}, supabase)
        await safe_clear_markup(chat_id, message_id)
        await send_message(chat_id, "Enter your birthdate (YYYY-MM-DD, e.g., 1995-06-22) or /skip:")
        state["stage"] = "awaiting_dob"
        set_state(chat_id, state)
        return {"ok": True}

    # Interests selection
    if state.get("stage") == "awaiting_interests":
        if callback_data.startswith("interest:"):
            interest = callback_data[len("interest:"):]
            if interest not in INTERESTS:
                logger.warning(f"Invalid interest selected: {interest}")
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
                    "is_draft": False,
                    "points": 100  # STARTER_POINTS
                }, supabase)
            await send_message(chat_id, f"Congrats! You've earned 100 points. Explore options:", reply_markup=create_main_menu_keyboard())
            if chat_id in USER_STATES:
                del USER_STATES[chat_id]
            return {"ok": True}

    # Delegate points and discounts/giveaways handling
    if registered:
        await handle_points_and_discounts(chat_id, callback_data, message_id, registered)
        return {"ok": True}

    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    asyncio.run(initialize_bot())
    uvicorn.run(app, host="0.0.0.0", port=8000)
