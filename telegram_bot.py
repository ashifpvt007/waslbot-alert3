import asyncio
import logging
from typing import List, Dict, Any
from datetime import datetime
import requests
from config import (
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    NEW_LISTING_TEMPLATE, PRICE_CHANGE_TEMPLATE, 
    DELETED_LISTING_TEMPLATE, STATUS_REPORT_TEMPLATE
)
from utils import format_current_listings_summary
from database import PropertyDatabase
from config import DATABASE_PATH

logger = logging.getLogger(__name__)

class TelegramNotifier:
    def __init__(self):
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.db = PropertyDatabase(DATABASE_PATH)
    
    def send_message(self, message: str, parse_mode: str = "Markdown") -> bool:
        """Send a message to Telegram chat"""
        try:
            url = f"{self.api_url}/sendMessage"
            
            # Split long messages if necessary
            max_length = 4096
            if len(message) > max_length:
                messages = self._split_message(message, max_length)
                for msg in messages:
                    self._send_single_message(msg, parse_mode)
            else:
                return self._send_single_message(message, parse_mode)
            
            return True
            
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")
            return False
    
    def _send_single_message(self, message: str, parse_mode: str = "Markdown") -> bool:
        """Send a single message to Telegram"""
        try:
            data = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True
            }
            
            response = requests.post(f"{self.api_url}/sendMessage", data=data, timeout=30)
            response.raise_for_status()
            
            return True
            
        except Exception as e:
            logger.error(f"Error sending single Telegram message: {e}")
            return False
    
    def _split_message(self, message: str, max_length: int) -> List[str]:
        """Split long message into chunks"""
        messages = []
        current_message = ""
        
        lines = message.split('\n')
        for line in lines:
            if len(current_message + line + '\n') > max_length:
                if current_message:
                    messages.append(current_message.strip())
                    current_message = line + '\n'
                else:
                    # Line is too long, split it
                    while len(line) > max_length:
                        messages.append(line[:max_length])
                        line = line[max_length:]
                    current_message = line + '\n'
            else:
                current_message += line + '\n'
        
        if current_message:
            messages.append(current_message.strip())
        
        return messages

    def _get_current_listings_summary(self) -> str:
        """Get formatted current listings summary"""
        try:
            listing_counts = self.db.get_current_listing_counts()
            return format_current_listings_summary(listing_counts)
        except Exception as e:
            logger.error(f"Error getting current listings summary: {e}")
            return "Current listings data unavailable"
    
    def send_new_listing_notification(self, property_data: Dict[str, Any]) -> bool:
        """Send notification for new property listing"""
        try:
            # Format price
            price = property_data.get('price', 0)
            price_formatted = f"{price:,.0f}" if price else "Price on request"
            
            # Format size
            size = property_data.get('size_sqft', 0)
            size_formatted = f"{size:,.0f}" if size else "N/A"
            
            # Format bedrooms/bathrooms
            bedrooms = property_data.get('bedrooms', 0) or "N/A"
            bathrooms = property_data.get('bathrooms', 0) or "N/A"
            
            # Truncate description
            description = property_data.get('description', 'No description available')
            if len(description) > 200:
                description = description[:200] + "..."
            
            # Get current listings summary
            current_listings = self._get_current_listings_summary()
            
            message = NEW_LISTING_TEMPLATE.format(
                location=property_data.get('location', 'Unknown'),
                property_type=property_data.get('property_type', 'Unknown').title(),
                price=price_formatted,
                bedrooms=bedrooms,
                bathrooms=bathrooms,
                size=size_formatted,
                url=property_data.get('url', 'N/A'),
                description=description,
                current_listings=current_listings,
                date_added=datetime.now().strftime('%Y-%m-%d %H:%M')
            )
            
            return self.send_message(message)
            
        except Exception as e:
            logger.error(f"Error sending new listing notification: {e}")
            return False
    
    def send_price_change_notification(self, property_data: Dict[str, Any], 
                                     old_price: float, new_price: float) -> bool:
        """Send notification for price change"""
        try:
            price_change = new_price - old_price
            percentage_change = (price_change / old_price * 100) if old_price > 0 else 0
            
            message = PRICE_CHANGE_TEMPLATE.format(
                title=property_data.get('title', 'Unknown Property'),
                location=property_data.get('location', 'Unknown'),
                old_price=old_price,
                new_price=new_price,
                price_change=price_change,
                percentage_change=percentage_change,
                url=property_data.get('url', 'N/A')
            )
            
            return self.send_message(message)
            
        except Exception as e:
            logger.error(f"Error sending price change notification: {e}")
            return False
    
    def send_deleted_listing_notification(self, property_data: Dict[str, Any]) -> bool:
        """Send notification for deleted property listing"""
        try:
            message = DELETED_LISTING_TEMPLATE.format(
                title=property_data.get('title', 'Unknown Property'),
                location=property_data.get('location', 'Unknown'),
                price=property_data.get('price', 0),
                date_added=property_data.get('date_added', 'Unknown'),
                date_removed=datetime.now().strftime('%Y-%m-%d %H:%M'),
                url=property_data.get('url', 'N/A')
            )
            
            return self.send_message(message)
            
        except Exception as e:
            logger.error(f"Error sending deleted listing notification: {e}")
            return False
    
    def send_status_report(self, stats: Dict[str, Any]) -> bool:
        """Send monitoring status report"""
        try:
            next_check = datetime.now().strftime('%Y-%m-%d %H:%M')
            
            # Get current listings summary
            current_listings = self._get_current_listings_summary()
            
            message = STATUS_REPORT_TEMPLATE.format(
                last_check=stats.get('last_check', 'Never'),
                total_properties=stats.get('total_properties', 0),
                new_today=stats.get('new_today', 0),
                price_changes_today=stats.get('price_changes_today', 0),
                deletions_today=stats.get('deletions_today', 0),
                current_listings=current_listings,
                next_check=next_check
            )
            
            return self.send_message(message)
            
        except Exception as e:
            logger.error(f"Error sending status report: {e}")
            return False
    
    def send_error_notification(self, error_message: str) -> bool:
        """Send error notification"""
        try:
            message = f"🚨 *WASL MONITOR ERROR*\n\n❌ Error: {error_message}\n\n⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            return self.send_message(message)
            
        except Exception as e:
            logger.error(f"Error sending error notification: {e}")
            return False
    
    def send_startup_notification(self) -> bool:
        """Send notification when monitoring starts"""
        try:
            message = f"✅ *WASL PROPERTY MONITOR STARTED*\n\n📍 Monitoring: Ras Al Khor Third Area\n⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n🔄 Check interval: 10 minutes\n\n🤖 Bot: @Wasl_alert1_bot"
            return self.send_message(message)
            
        except Exception as e:
            logger.error(f"Error sending startup notification: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Test Telegram bot connection"""
        try:
            url = f"{self.api_url}/getMe"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            bot_info = response.json()
            if bot_info.get('ok'):
                logger.info(f"Telegram bot connection successful: {bot_info['result']['username']}")
                return True
            else:
                logger.error(f"Telegram bot connection failed: {bot_info}")
                return False
                
        except Exception as e:
            logger.error(f"Error testing Telegram connection: {e}")
            return False
