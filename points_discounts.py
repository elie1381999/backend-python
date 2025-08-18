import logging
import asyncio
from typing import Dict, Any, Optional
from supabase import Client
from utils import send_message, safe_clear_markup, create_categories_keyboard, create_main_menu_keyboard

# Logging setup
logger = logging.getLogger(__name__)

async def handle_points_and_discounts(
    chat_id: int,
    callback_data: str,
    message_id: int,
    registered: Dict[str, Any],
    supabase: Client
):
    """
    Handle points and discounts/giveaways menu options.
    """
    try:
        # Handle "My Points" menu
        if callback_data == "menu:points":
            points = registered.get("points", 0)
            await safe_clear_markup(chat_id, message_id)
            await send_message(
                chat_id,
                f"Your current points: {points}",
                reply_markup=create_main_menu_keyboard()
            )
            return {"ok": True}

        # Handle "Discounts" menu
        elif callback_data == "menu:discounts":
            user_interests = registered.get("interests", []) or []
            # Query active discounts from businesses with 'approved' status
            def _q():
                query = (
                    supabase.table("discounts")
                    .select("discounts.*, businesses.name AS business_name")
                    .eq("discounts.active", True)
                    .eq("businesses.status", "approved")
                    .join("businesses", "discounts.business_id = businesses.id")
                )
                if user_interests:
                    query = query.in_("discounts.category", user_interests)
                return query.execute()
            
            try:
                resp = await asyncio.to_thread(_q)
                discounts = resp.data if hasattr(resp, "data") else resp.get("data", [])
                
                if not discounts:
                    await safe_clear_markup(chat_id, message_id)
                    await send_message(
                        chat_id,
                        "No discounts available at the moment.",
                        reply_markup=create_main_menu_keyboard()
                    )
                    return {"ok": True}
                
                # Group discounts by category
                discount_text = "Available Discounts:\n"
                for discount in discounts:
                    discount_text += (
                        f"• {discount['name']} ({discount['category']}): "
                        f"{discount['discount_percentage']}% off at {discount['business_name']}\n"
                    )
                
                await safe_clear_markup(chat_id, message_id)
                await send_message(
                    chat_id,
                    discount_text,
                    reply_markup=create_categories_keyboard()
                )
                return {"ok": True}
            
            except Exception as e:
                logger.error(f"Failed to fetch discounts for chat_id {chat_id}: {str(e)}", exc_info=True)
                await safe_clear_markup(chat_id, message_id)
                await send_message(
                    chat_id,
                    "Error fetching discounts. Please try again later.",
                    reply_markup=create_main_menu_keyboard()
                )
                return {"ok": True}

        # Handle "Giveaways" menu (placeholder, as per schema)
        elif callback_data == "menu:giveaways":
            # Similar logic for giveaways can be added here
            await safe_clear_markup(chat_id, message_id)
            await send_message(
                chat_id,
                "No giveaways available at the moment.",
                reply_markup=create_main_menu_keyboard()
            )
            return {"ok": True}

        # Handle discount category selection (optional)
        elif callback_data.startswith("discount_category:"):
            category = callback_data[len("discount_category:"):]
            def _q():
                return (
                    supabase.table("discounts")
                    .select("discounts.*, businesses.name AS business_name")
                    .eq("discounts.active", True)
                    .eq("businesses.status", "approved")
                    .eq("discounts.category", category)
                    .join("businesses", "discounts.business_id = businesses.id")
                    .execute()
                )
            
            try:
                resp = await asyncio.to_thread(_q)
                discounts = resp.data if hasattr(resp, "data") else resp.get("data", [])
                
                if not discounts:
                    await safe_clear_markup(chat_id, message_id)
                    await send_message(
                        chat_id,
                        f"No discounts available for {category}.",
                        reply_markup=create_main_menu_keyboard()
                    )
                    return {"ok": True}
                
                discount_text = f"Discounts for {category}:\n"
                for discount in discounts:
                    discount_text += (
                        f"• {discount['name']}: "
                        f"{discount['discount_percentage']}% off at {discount['business_name']}\n"
                    )
                
                await safe_clear_markup(chat_id, message_id)
                await send_message(
                    chat_id,
                    discount_text,
                    reply_markup=create_main_menu_keyboard()
                )
                return {"ok": True}
            
            except Exception as e:
                logger.error(f"Failed to fetch discounts for category {category}, chat_id {chat_id}: {str(e)}", exc_info=True)
                await safe_clear_markup(chat_id, message_id)
                await send_message(
                    chat_id,
                    "Error fetching discounts. Please try again later.",
                    reply_markup=create_main_menu_keyboard()
                )
                return {"ok": True}

        return {"ok": True}
    
    except Exception as e:
        logger.error(f"Error in handle_points_and_discounts for chat_id {chat_id}: {str(e)}", exc_info=True)
        await safe_clear_markup(chat_id, message_id)
        await send_message(
            chat_id,
            "An error occurred. Please try again.",
            reply_markup=create_main_menu_keyboard()
        )
        return {"ok": True}
