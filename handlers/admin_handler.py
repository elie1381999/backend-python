from typing import Dict, Any
from utils import supabase_find_business, supabase_find_giveaway, supabase_update_by_id_return, send_message, notify_users, now_iso, safe_clear_markup, logger, uuid

async def handle_admin_command(text: str, chat_id: int):
    if text.startswith("/approve_"):
        business_id = text[len("/approve_"):]
        try:
            uuid.UUID(business_id)
            business = await supabase_find_business(business_id)
            if not business:
                await send_message(chat_id, f"Business {business_id} not found.")
                return {"ok": True}
            await supabase_update_by_id_return("businesses", business_id, {"status": "approved", "updated_at": now_iso()})
            await send_message(chat_id, f"Business {business['name']} approved.")
            await send_message(business["telegram_id"], "Your business approved! Add discounts/giveaways.")
        except ValueError:
            await send_message(chat_id, f"Invalid ID: {business_id}")
        except Exception as e:
            logger.error(f"Approve failed: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to approve.")
        return {"ok": True}
    elif text.startswith("/reject_"):
        business_id = text[len("/reject_"):]
        try:
            uuid.UUID(business_id)
            business = await supabase_find_business(business_id)
            if not business:
                await send_message(chat_id, f"Business {business_id} not found.")
                return {"ok": True}
            await supabase_update_by_id_return("businesses", business_id, {"status": "rejected", "updated_at": now_iso()})
            await send_message(chat_id, f"Business {business['name']} rejected.")
            await send_message(business["telegram_id"], "Business rejected. Contact support.")
        except ValueError:
            await send_message(chat_id, f"Invalid ID: {business_id}")
        except Exception as e:
            logger.error(f"Reject failed: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to reject.")
        return {"ok": True}

async def handle_admin_callback(callback_query: Dict[str, Any], message_id: int):
    callback_data = callback_query.get("data")
    chat_id = callback_query.get("from", {}).get("id")
    if callback_data.startswith("approve:"):
        business_id = callback_data[len("approve:"):]
        try:
            uuid.UUID(business_id)
            business = await supabase_find_business(business_id)
            if not business:
                await send_message(chat_id, f"Business {business_id} not found.")
                await safe_clear_markup(chat_id, message_id)
                return {"ok": True}
            await supabase_update_by_id_return("businesses", business_id, {"status": "approved", "updated_at": now_iso()})
            await send_message(chat_id, f"Business {business['name']} approved.")
            await send_message(business["telegram_id"], "Your business approved! Add discounts/giveaways.")
            await safe_clear_markup(chat_id, message_id)
        except ValueError:
            await send_message(chat_id, f"Invalid ID: {business_id}")
        except Exception as e:
            logger.error(f"Approve failed: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to approve.")
        return {"ok": True}
    elif callback_data.startswith("reject:"):
        business_id = callback_data[len("reject:"):]
        try:
            uuid.UUID(business_id)
            business = await supabase_find_business(business_id)
            if not business:
                await send_message(chat_id, f"Business {business_id} not found.")
                await safe_clear_markup(chat_id, message_id)
                return {"ok": True}
            await supabase_update_by_id_return("businesses", business_id, {"status": "rejected", "updated_at": now_iso()})
            await send_message(chat_id, f"Business {business['name']} rejected.")
            await send_message(business["telegram_id"], "Business rejected. Contact support.")
            await safe_clear_markup(chat_id, message_id)
        except ValueError:
            await send_message(chat_id, f"Invalid ID: {business_id}")
        except Exception as e:
            logger.error(f"Reject failed: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to reject.")
        return {"ok": True}
    elif callback_data.startswith("giveaway_approve:"):
        giveaway_id = callback_data[len("giveaway_approve:"):]
        try:
            uuid.UUID(giveaway_id)
            giveaway = await supabase_find_giveaway(giveaway_id)
            if not giveaway:
                await send_message(chat_id, f"Giveaway {giveaway_id} not found.")
                await safe_clear_markup(chat_id, message_id)
                return {"ok": True}
            await supabase_update_by_id_return("giveaways", giveaway_id, {"active": True, "updated_at": now_iso()})
            await send_message(chat_id, f"Approved {giveaway['business_type']}: {giveaway['name']}.")
            business = await supabase_find_business(giveaway["business_id"])
            await send_message(business["telegram_id"], f"Your {giveaway['business_type']} '{giveaway['name']}' approved and live!")
            await notify_users(giveaway_id)
            await safe_clear_markup(chat_id, message_id)
        except ValueError:
            await send_message(chat_id, f"Invalid giveaway ID: {giveaway_id}")
        except Exception as e:
            logger.error(f"Approve giveaway failed: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to approve giveaway.")
        return {"ok": True}
    elif callback_data.startswith("giveaway_reject:"):
        giveaway_id = callback_data[len("giveaway_reject:"):]
        try:
            uuid.UUID(giveaway_id)
            giveaway = await supabase_find_giveaway(giveaway_id)
            if not giveaway:
                await send_message(chat_id, f"Giveaway {giveaway_id} not found.")
                await safe_clear_markup(chat_id, message_id)
                return {"ok": True}
            await supabase_update_by_id_return("giveaways", giveaway_id, {"active": False, "updated_at": now_iso()})
            await send_message(chat_id, f"Rejected {giveaway['business_type']}: {giveaway['name']}.")
            business = await supabase_find_business(giveaway["business_id"])
            await send_message(business["telegram_id"], f"Your {giveaway['business_type']} '{giveaway['name']}' rejected. Contact support.")
            await safe_clear_markup(chat_id, message_id)
        except ValueError:
            await send_message(chat_id, f"Invalid giveaway ID: {giveaway_id}")
        except Exception as e:
            logger.error(f"Reject giveaway failed: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to reject giveaway.")
        return {"ok": True}
