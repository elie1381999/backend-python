from typing import Dict, Any
import asyncio
from datetime import datetime, timezone
from utils import send_message, create_main_menu_keyboard, has_redeemed_discount, supabase_update_by_id_return, generate_promo_code, supabase_find_giveaway, notify_users, logger, uuid

async def handle_giveaways(callback_query: Dict[str, Any], registered: Dict[str, Any], chat_id: int):
    try:
        if not await has_redeemed_discount(chat_id):
            await send_message(chat_id, "Claim a discount first to unlock giveaways. Check Discounts:", reply_markup=create_main_menu_keyboard())
            return {"ok": True}
        interests = registered.get("interests", []) or []
        if not interests:
            await send_message(chat_id, "No interests set. Please update your profile.")
            return {"ok": True}
        def _query_giveaways():
            return supabase.table("giveaways").select("*").in_("category", interests).eq("active", True).eq("business_type", "giveaway").execute()
        resp = await asyncio.to_thread(_query_giveaways)
        giveaways = resp.data if hasattr(resp, "data") else resp.get("data", [])
        if not giveaways:
            await send_message(chat_id, "No giveaways available for your interests. Check Discover Offers:", reply_markup=create_main_menu_keyboard())
            return {"ok": True}
        for g in giveaways:
            business_type = g.get("business_type", "salon").capitalize()
            cost = g.get("cost", 200)
            message = f"{business_type}: *{g['name']}* at {g.get('salon_name')} ({g.get('category')})"
            keyboard = {"inline_keyboard": [
                [{"text": f"Join ({cost} pts)", "callback_data": f"giveaway_points:{g['id']}"}],
                [{"text": "Join via Booking", "callback_data": f"giveaway_book:{g['id']}"}]
            ]}
            await send_message(chat_id, message, keyboard)
    except Exception as e:
        logger.error(f"Fetch giveaways failed: {str(e)}", exc_info=True)
        await send_message(chat_id, "Failed to load giveaways.")
    return {"ok": True}

async def handle_giveaway_callback(callback_data: str, chat_id: int, registered: Dict[str, Any]):
    if callback_data.startswith("giveaway_points:"):
        giveaway_id = callback_data[len("giveaway_points:"):]
        try:
            uuid.UUID(giveaway_id)
            def _query_giveaway():
                return supabase.table("giveaways").select("*").eq("id", giveaway_id).eq("active", True).limit(1).execute()
            resp = await asyncio.to_thread(_query_giveaway)
            giveaway = resp.data[0] if resp.data else None
            if not giveaway:
                await send_message(chat_id, "Giveaway not found or inactive.")
                return {"ok": True}
            if not giveaway.get("business_id"):
                await send_message(chat_id, "Giveaway unavailable due to config issue.")
                return {"ok": True}
            cost = giveaway.get("cost", 200)
            if registered.get("points", 0) < cost:
                await send_message(chat_id, f"Not enough points (need {cost}).")
                return {"ok": True}
            def _check_existing():
                current_month = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                return supabase.table("user_giveaways").select("id").eq("telegram_id", chat_id).eq("giveaway_id", giveaway_id).gte("joined_at", current_month.isoformat()).execute()
            resp = await asyncio.to_thread(_check_existing)
            if resp.data:
                await send_message(chat_id, "Already joined this month.")
                return {"ok": True}
            await supabase_update_by_id_return("central_bot_leads", registered["id"], {"points": registered["points"] - cost})
            code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "loser")
            await supabase_insert_return("user_giveaways", {
                "telegram_id": chat_id,
                "giveaway_id": giveaway_id,
                "business_id": giveaway["business_id"],
                "entry_status": "pending",
                "joined_at": now_iso()
            })
            business_type = giveaway.get("business_type", "salon").capitalize()
            await send_message(chat_id, f"Joined {business_type} {giveaway['name']} with {cost} points. Your 20% loser discount code: *{code}*, valid until {expiry.split('T')[0]}.")
        except ValueError:
            await send_message(chat_id, "Invalid giveaway ID.")
        except Exception as e:
            logger.error(f"Giveaway points failed: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to join giveaway.")
        return {"ok": True}
    elif callback_data.startswith("giveaway_book:"):
        giveaway_id = callback_data[len("giveaway_book:"):]
        try:
            uuid.UUID(giveaway_id)
            def _query_giveaway():
                return supabase.table("giveaways").select("*").eq("id", giveaway_id).eq("active", True).limit(1).execute()
            resp = await asyncio.to_thread(_query_giveaway)
            giveaway = resp.data[0] if resp.data else None
            if not giveaway:
                await send_message(chat_id, "Giveaway not found or inactive.")
                return {"ok": True}
            if not giveaway.get("business_id"):
                await send_message(chat_id, "Giveaway unavailable due to config issue.")
                return {"ok": True}
            def _check_existing():
                current_month = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                return supabase.table("user_giveaways").select("id").eq("telegram_id", chat_id).eq("giveaway_id", giveaway_id).gte("joined_at", current_month.isoformat()).execute()
            resp = await asyncio.to_thread(_check_existing)
            if resp.data:
                await send_message(chat_id, "Already joined this month.")
                return {"ok": True}
            code, expiry = await generate_promo_code(chat_id, giveaway["business_id"], giveaway_id, "awaiting_booking")
            await supabase_insert_return("user_giveaways", {
                "telegram_id": chat_id,
                "giveaway_id": giveaway_id,
                "business_id": giveaway["business_id"],
                "entry_status": "awaiting_booking",
                "joined_at": now_iso()
            })
            business_type = giveaway.get("business_type", "salon").capitalize()
            await send_message(chat_id, f"Book a service at {business_type} {giveaway.get('salon_name')} with code *{code}* to join {giveaway['name']}. Valid until {expiry.split('T')[0]}.")
        except ValueError:
            await send_message(chat_id, "Invalid giveaway ID.")
        except Exception as e:
            logger.error(f"Giveaway book failed: {str(e)}", exc_info=True)
            await send_message(chat_id, "Failed to join giveaway.")
        return {"ok": True}
